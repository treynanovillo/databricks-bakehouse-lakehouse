"""Auditoría de ejecuciones: registra inicio y fin de cada tarea en ops.pipeline_runs."""
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType,
                               LongType, TimestampType)

_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("job_run_id", StringType()),
    StructField("pipeline", StringType()),
    StructField("task", StringType()),
    StructField("layer", StringType()),
    StructField("target_table", StringType()),
    StructField("status", StringType()),
    StructField("rows_read", LongType()),
    StructField("rows_written", LongType()),
    StructField("rows_rejected", LongType()),
    StructField("error_message", StringType()),
    StructField("started_at", TimestampType()),
    StructField("finished_at", TimestampType()),
    StructField("executed_by", StringType()),
])


class AuditRun:
    """Datos de una ejecución. El notebook rellena los contadores de filas."""

    def __init__(self, cfg, task, layer, target_table, job_run_id=None):
        self.spark = SparkSession.getActiveSession()
        self.table = cfg.table("ops", "pipeline_runs")
        self.run_id = str(uuid.uuid4())
        self.job_run_id = job_run_id or None
        self.pipeline = cfg.pipeline
        self.task = task
        self.layer = layer
        self.target_table = target_table
        self.rows_read = None
        self.rows_written = None
        self.rows_rejected = None
        self.started_at = datetime.now(timezone.utc)
        self.user = self.spark.sql("SELECT current_user()").first()[0]

    def _write(self, status, finished_at=None, error=None):
        row = (self.run_id, self.job_run_id, self.pipeline, self.task, self.layer,
               self.target_table, status, self.rows_read, self.rows_written,
               self.rows_rejected, error, self.started_at, finished_at, self.user)
        (self.spark.createDataFrame([row], _SCHEMA)
             .write.mode("append").saveAsTable(self.table))


@contextmanager
def audit_run(cfg, task, layer, target_table, job_run_id=None):
    """Uso:
        with audit_run(cfg, "02_bronze", "bronze", tabla, job_run_id) as run:
            ...
            run.rows_written = n
    Si el bloque falla: registra FAILED y RELANZA el error para que el Job también falle.
    """
    run = AuditRun(cfg, task, layer, target_table, job_run_id)
    run._write("RUNNING")
    try:
        yield run
    except Exception as e:
        run._write("FAILED", datetime.now(timezone.utc), f"{type(e).__name__}: {e}"[:4000])
        raise
    else:
        run._write("SUCCESS", datetime.now(timezone.utc))