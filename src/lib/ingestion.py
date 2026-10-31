"""Reusable incremental file ingestion into the bronze layer (Auto Loader)."""
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class FileSource:
    """Declarative description of a file source landing in the raw volume.

    Attributes:
        name: Source name. Defines the landing folder and the bronze table name.
        file_format: Auto Loader file format (json, csv, parquet...).
        schema_hints: Types for critical columns; the rest is inferred.
    """
    name: str
    file_format: str = "json"
    schema_hints: str | None = None


def ingest_to_bronze(cfg, source: FileSource, run_id: str) -> int:
    """Incrementally load new files of `source` into bronze.<source.name>.

    Only files not yet recorded in the checkpoint are processed (exactly-once).
    Every row gets lineage columns: ingestion time, source file and run id.

    Returns:
        Number of rows written by this run.
    """
    spark = SparkSession.getActiveSession()
    source_path = f"{cfg.landing_path}/{source.name}"
    target = cfg.table("bronze", source.name)
    checkpoint = f"{cfg.checkpoint_path}/bronze_{source.name}"

    # Reader: Auto Loader with schema tracking and evolution
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", source.file_format)
              .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
              .option("cloudFiles.inferColumnTypes", "true")
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
    if source.schema_hints:
        reader = reader.option("cloudFiles.schemaHints", source.schema_hints)

    # Lineage columns added to every row
    df = (reader.load(source_path)
          .select("*",
                  F.current_timestamp().alias("_ingested_at"),
                  F.col("_metadata.file_path").alias("_source_file"),
                  F.col("_metadata.file_modification_time").alias("_source_file_ts"),
                  F.lit(run_id).alias("_run_id")))

    # Writer: process all pending files, then stop (batch-style incremental load)
    (df.writeStream
       .option("checkpointLocation", checkpoint)
       .option("mergeSchema", "true")
       .trigger(availableNow=True)
       .toTable(target)
       .awaitTermination())

    return spark.table(target).filter(F.col("_run_id") == run_id).count()