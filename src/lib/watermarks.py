"""Watermark control for incremental loads between layers (ops.watermarks).

The table is append-only: every successful load adds a row, and the current
watermark is the maximum value for a (source, target) pair.
"""
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_watermark(spark: SparkSession, cfg, source_table: str,
                  target_table: str) -> datetime | None:
    """Return the last watermark processed for (source, target), or None on the first run."""
    return spark.sql(
        f"""
        SELECT MAX(watermark_ts) AS watermark
        FROM {cfg.table("ops", "watermarks")}
        WHERE source_table = :source AND target_table = :target
        """,
        args={"source": source_table, "target": target_table},
    ).first()["watermark"]


def set_watermark(spark: SparkSession, cfg, source_table: str, target_table: str,
                  value: datetime, run_id: str) -> None:
    """Record a new watermark. Call it only after the load has been committed."""
    (spark.createDataFrame(
            [(source_table, target_table, value, run_id)],
            "source_table string, target_table string, watermark_ts timestamp, run_id string")
        .withColumn("updated_at", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(cfg.table("ops", "watermarks")))