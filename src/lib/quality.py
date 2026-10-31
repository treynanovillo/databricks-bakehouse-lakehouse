"""Data quality helpers: log rejected records to ops.data_quality_issues."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def log_rejected(cfg, rejected: DataFrame, run_id: str, layer: str, target_table: str,
                 rule_name: str, key: str) -> int:
    """Append rejected records to the data quality table.

    Each record is stored with the rule it broke and its full content as JSON,
    so it can be analysed or reprocessed later.

    Returns:
        Number of rejected records.
    """
    n_rejected = rejected.count()
    if n_rejected == 0:
        return 0

    (rejected
        .select(
            F.lit(run_id).alias("run_id"),
            F.lit(layer).alias("layer"),
            F.lit(target_table).alias("target_table"),
            F.lit(rule_name).alias("rule_name"),
            F.col(key).cast("string").alias("record_key"),
            F.to_json(F.struct("*")).alias("record_payload"),
            F.current_timestamp().alias("detected_at"))
        .write.mode("append").saveAsTable(cfg.table("ops", "data_quality_issues")))

    return n_rejected