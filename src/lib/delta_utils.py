"""Generic Delta Lake helpers shared by all layers."""
from pyspark.sql import SparkSession


def run_merge(spark: SparkSession, merge_sql: str) -> int:
    """Execute a MERGE statement and return the number of rows inserted + updated.

    Delta returns the operation metrics as the result of the MERGE itself,
    so no extra query on the target table is needed.
    """
    metrics = spark.sql(merge_sql).first()
    return int(metrics["num_inserted_rows"]) + int(metrics["num_updated_rows"])