# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# =============================================================================
# Parameters
# =============================================================================
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("job_run_id", "", "Orchestrator run id")

# COMMAND ----------

# =============================================================================
# Setup: imports, configuration and entity-specific transformations
# =============================================================================
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from lib.audit import audit_run
from lib.config import Config
from lib.master_data import latest_snapshot, merge_snapshot_scd1
from lib.quality import log_quality_issues
from lib.watermarks import get_watermark, set_watermark

cfg = Config(catalog=dbutils.widgets.get("catalog"))
JOB_RUN_ID = dbutils.widgets.get("job_run_id")

# Lineage columns carried from bronze to silver
LINEAGE_COLS = [
    F.col("_snapshot_id"),
    F.col("_source_file"),
    F.col("_ingested_at").alias("_bronze_ingested_at"),
]


def clean_text(col_name: str) -> Column:
    """Trim a text column; empty strings become NULL."""
    trimmed = F.trim(F.col(col_name))
    return F.when(trimmed != "", trimmed)


def transform_customers(df: DataFrame) -> DataFrame:
    """Map the customers snapshot to the silver contract (names, types, formats)."""
    return df.select(
        F.col("customerID").cast("bigint").alias("customer_id"),
        F.initcap(clean_text("first_name")).alias("first_name"),
        F.initcap(clean_text("last_name")).alias("last_name"),
        F.lower(clean_text("email_address")).alias("email"),
        clean_text("phone_number").alias("phone"),
        clean_text("address").alias("address"),
        clean_text("city").alias("city"),
        clean_text("state").alias("state"),
        clean_text("country").alias("country"),
        clean_text("continent").alias("continent"),
        F.col("postal_zip_code").cast("string").alias("postal_code"),
        clean_text("gender").alias("gender"),
        *LINEAGE_COLS,
    )


def transform_franchises(df: DataFrame) -> DataFrame:
    """Map the franchises snapshot to the silver contract (names, types, formats)."""
    return df.select(
        F.col("franchiseID").cast("bigint").alias("franchise_id"),
        clean_text("name").alias("franchise_name"),
        clean_text("city").alias("city"),
        clean_text("district").alias("district"),
        F.col("zipcode").cast("string").alias("zip_code"),
        clean_text("country").alias("country"),
        clean_text("size").alias("size"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("supplierID").cast("bigint").alias("supplier_id"),
        *LINEAGE_COLS,
    )


# Entities processed by this notebook: (bronze/silver name, business key, transformation)
ENTITIES = [
    ("customers",  "customer_id",  transform_customers),
    ("franchises", "franchise_id", transform_franchises),
]

# COMMAND ----------

# =============================================================================
# Load: latest snapshot -> quality checks -> SCD1 merge (one audited run per entity)
# -----------------------------------------------------------------------------
# A snapshot is applied only once: its timestamp is stored as the watermark, so
# a daily run without a new snapshot skips the entity instead of re-hashing it.
# =============================================================================
for name, key, transform in ENTITIES:
    source_table = cfg.table("bronze", name)
    target_table = cfg.table("silver", name)

    with audit_run(cfg, "03_silver_master_data", "silver", target_table, JOB_RUN_ID) as run:
        run.rows_read = run.rows_written = run.rows_rejected = 0

        # 1. Latest snapshot in bronze and its timestamp
        raw_snapshot = latest_snapshot(spark.table(source_table))
        snapshot_ts = raw_snapshot.agg(F.max("_source_file_ts")).first()[0]
        watermark = get_watermark(spark, cfg, source_table, target_table)

        if watermark is not None and snapshot_ts <= watermark:
            print(f"{target_table}: snapshot of {snapshot_ts} already applied, skipped")
        else:
            # 2. Snapshot mapped to the silver contract
            snapshot = transform(raw_snapshot)
            run.rows_read = snapshot.count()

            # 3. Quality: rows without business key are logged and excluded
            run.rows_rejected = log_quality_issues(
                cfg, snapshot.filter(F.col(key).isNull()), run.run_id,
                layer="silver", target_table=target_table,
                rule_name=f"{key}_not_null", key=key)
            valid = snapshot.filter(F.col(key).isNotNull())

            # 4. Contract check: the business key must be unique within a snapshot.
            #    Duplicates mean a broken extract -> fail loudly instead of guessing.
            duplicated = valid.groupBy(key).count().filter("count > 1").count()
            if duplicated:
                raise ValueError(f"{duplicated} duplicated {key} values in the "
                                 f"latest snapshot of {source_table}")

            # 5. SCD1 merge: insert new, update changed (hash), soft-delete missing
            merge_snapshot_scd1(valid, target_table, key, run.run_id)
            run.rows_written = (spark.table(target_table)
                                .filter(F.col("_run_id") == run.run_id).count())

            # 6. Record the snapshot as applied
            set_watermark(spark, cfg, source_table, target_table, snapshot_ts, run.run_id)

            print(f"{target_table}: read {run.rows_read} | written {run.rows_written} "
                  f"| rejected {run.rows_rejected}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver master data: rows per snapshot and deletion status
# MAGIC USE CATALOG IDENTIFIER(:catalog);
# MAGIC
# MAGIC SELECT 'customers' AS entity, _snapshot_id, is_deleted, COUNT(*) AS row_count
# MAGIC FROM silver.customers
# MAGIC GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'franchises', _snapshot_id, is_deleted, COUNT(*)
# MAGIC FROM silver.franchises
# MAGIC GROUP BY ALL
# MAGIC ORDER BY entity;