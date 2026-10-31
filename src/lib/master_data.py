"""Helpers for full-load master data: snapshot selection and SCD Type 1 merge."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Placeholder used when hashing NULLs, so NULL and "" produce different hashes
_NULL_TOKEN = "<NULL>"


def latest_snapshot(bronze_df: DataFrame) -> DataFrame:
    """Return only the rows of the most recent snapshot found in a bronze table.

    The snapshot id is the landing folder name (e.g. 'snapshot_v2'). The latest
    snapshot is the one whose files are the most recent in the source.
    """
    with_id = bronze_df.withColumn(
        "_snapshot_id", F.regexp_extract("_source_file", r"/(snapshot_[^/]+)/", 1))

    latest_id = (with_id
                 .groupBy("_snapshot_id")
                 .agg(F.max("_source_file_ts").alias("_snapshot_ts"))
                 .orderBy(F.col("_snapshot_ts").desc())
                 .first()["_snapshot_id"])

    return with_id.filter(F.col("_snapshot_id") == latest_id)


def merge_snapshot_scd1(snapshot: DataFrame, target_table: str, key: str, run_id: str,
                        min_active_ratio: float = 0.5) -> None:
    """Apply a full snapshot to a SCD Type 1 table.

    - New keys are inserted.
    - Existing keys are updated only if their business attributes changed (row hash).
    - Keys missing from the snapshot are soft-deleted (is_deleted = TRUE).

    Safety guard: if the snapshot has fewer rows than `min_active_ratio` times the
    currently active rows, the merge is aborted. An incomplete extract would
    otherwise soft-delete most of the table.

    Args:
        snapshot: Clean snapshot. Business columns + lineage columns (prefixed with "_").
        target_table: Fully qualified silver table name.
        key: Business key column.
        run_id: Current run id (lineage).
        min_active_ratio: Minimum snapshot size relative to active rows.
    """
    spark = snapshot.sparkSession

    # --- Safety guard against incomplete extracts --------------------------------
    snapshot_rows = snapshot.count()
    active_rows = spark.table(target_table).filter("NOT is_deleted").count()
    if active_rows and snapshot_rows < active_rows * min_active_ratio:
        raise ValueError(
            f"Snapshot for {target_table} has {snapshot_rows} rows vs {active_rows} active "
            f"rows (< {min_active_ratio:.0%}). Aborting to avoid mass soft-deletes.")

    # --- Change detection: hash of business attributes ----------------------------
    business_cols = [c for c in snapshot.columns if not c.startswith("_") and c != key]
    row_hash = F.sha2(F.concat_ws("||", *[
        F.coalesce(F.col(c).cast("string"), F.lit(_NULL_TOKEN)) for c in business_cols]), 256)

    staged = (snapshot
              .withColumn("_row_hash", row_hash)
              .withColumn("is_deleted", F.lit(False))
              .withColumn("_run_id", F.lit(run_id))
              .withColumn("_silver_updated_at", F.current_timestamp()))
    staged.createOrReplaceTempView("_snapshot_stage")

    # --- SCD1 merge with soft deletes ------------------------------------------------
    spark.sql(f"""
        MERGE INTO {target_table} AS t
        USING _snapshot_stage AS s
          ON t.{key} = s.{key}
        WHEN MATCHED AND (t._row_hash <> s._row_hash OR t.is_deleted) THEN
          UPDATE SET *
        WHEN NOT MATCHED THEN
          INSERT *
        WHEN NOT MATCHED BY SOURCE AND NOT t.is_deleted THEN
          UPDATE SET t.is_deleted = TRUE,
                     t._run_id = '{run_id}',
                     t._silver_updated_at = current_timestamp()
    """)