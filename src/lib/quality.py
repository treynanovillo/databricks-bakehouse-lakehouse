"""Data quality helpers: record quality issues in ops.data_quality_issues."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def log_quality_issues(cfg, records: DataFrame, run_id: str, layer: str,
                       target_table: str, rule_name: str, key: str) -> int:
    """Append records that broke a quality rule to the data quality table.

    Used both for rejected records (excluded from the target) and for flagged
    records (loaded anyway, e.g. facts linked to an unknown dimension member).
    Each record is stored with its full content as JSON for analysis or reprocessing.

    Returns:
        Number of records logged.
    """
    n_records = records.count()
    if n_records == 0:
        return 0

    (records
        .select(
            F.lit(run_id).alias("run_id"),
            F.lit(layer).alias("layer"),
            F.lit(target_table).alias("target_table"),
            F.lit(rule_name).alias("rule_name"),
            F.col(key).cast("string").alias("record_key"),
            F.to_json(F.struct("*")).alias("record_payload"),
            F.current_timestamp().alias("detected_at"))
        .write.mode("append").saveAsTable(cfg.table("ops", "data_quality_issues")))

    return n_records