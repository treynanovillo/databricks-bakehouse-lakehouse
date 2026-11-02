"""Run auditing: records the start and end of every task in ops.pipeline_runs.

The log is append-only: one RUNNING row when a task starts and one SUCCESS or
FAILED row when it ends. The view ops.v_pipeline_runs exposes the final state.
"""
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (LongType, StringType, StructField, StructType,
                               TimestampType)

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


def _summarize_error(error: Exception) -> str:
    """Error type and message without the JVM stack trace, capped at 4,000 characters."""
    text = f"{type(error).__name__}: {error}"
    return text.split("JVM stacktrace:")[0].strip()[:4000]


class AuditRun:
    """State of one task run. The calling notebook fills in the row counters."""

    def __init__(self, cfg, task: str, layer: str, target_table: str,
                 job_run_id: str | None = None):
        self.spark = SparkSession.getActiveSession()
        self.table = cfg.table("ops", "pipeline_runs")
        self.run_id = str(uuid.uuid4())
        self.job_run_id = job_run_id or None
        self.pipeline = cfg.pipeline
        self.task = task
        self.layer = layer
        self.target_table = target_table
        self.rows_read: int | None = None
        self.rows_written: int | None = None
        self.rows_rejected: int | None = None
        self.started_at = datetime.now(timezone.utc)
        self.user = self.spark.sql("SELECT current_user()").first()[0]

    def _write(self, status: str, finished_at: datetime | None = None,
               error: str | None = None) -> None:
        """Append one audit row with the current state of the run."""
        row = (self.run_id, self.job_run_id, self.pipeline, self.task, self.layer,
               self.target_table, status, self.rows_read, self.rows_written,
               self.rows_rejected, error, self.started_at, finished_at, self.user)
        (self.spark.createDataFrame([row], _SCHEMA)
             .write.mode("append").saveAsTable(self.table))


@contextmanager
def audit_run(cfg, task: str, layer: str, target_table: str,
              job_run_id: str | None = None):
    """Audit a block of code as one task run.

    Usage:
        with audit_run(cfg, "02_bronze", "bronze", target, job_run_id) as run:
            ...
            run.rows_written = n

    On failure the run is recorded as FAILED and the exception is RE-RAISED,
    so the orchestrator (Job / ADF) also marks the task as failed.
    """
    run = AuditRun(cfg, task, layer, target_table, job_run_id)
    run._write("RUNNING")
    try:
        yield run
    except Exception as error:
        run._write("FAILED", datetime.now(timezone.utc), _summarize_error(error))
        raise
    else:
        run._write("SUCCESS", datetime.now(timezone.utc))