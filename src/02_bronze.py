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

if not dbutils.widgets.get("catalog").strip():
    raise ValueError("Parameter 'catalog' is required (e.g. 'workspace', 'dev', 'prod').")

# COMMAND ----------

# =============================================================================
# Bronze ingestion (metadata-driven)
# -----------------------------------------------------------------------------
# Every file source is loaded incrementally with the same reusable function
# (Auto Loader + lineage columns). Adding a source means adding one entry to
# SOURCES, not writing a new notebook. In a larger project this list would live
# in a config table or a YAML file.
# =============================================================================
from lib.audit import audit_run
from lib.config import Config
from lib.ingestion import FileSource, ingest_to_bronze

cfg = Config(catalog=dbutils.widgets.get("catalog"))
JOB_RUN_ID = dbutils.widgets.get("job_run_id")

SOURCES = [
    # Incremental daily extracts. Critical types are pinned with schema hints:
    # inference alone depends on which files exist on the first run.
    FileSource(
        "transactions",
        schema_hints=("transactionID BIGINT, customerID BIGINT, franchiseID BIGINT, "
                      "quantity BIGINT, dateTime TIMESTAMP, "
                      "createdAt TIMESTAMP, updatedAt TIMESTAMP"),
    ),
    # Full-load master data snapshots
    FileSource("customers", schema_hints="customerID BIGINT"),
    FileSource("franchises", schema_hints="franchiseID BIGINT, latitude DOUBLE, longitude DOUBLE"),
]

# COMMAND ----------

# =============================================================================
# Load every source (one audited run per source)
# -----------------------------------------------------------------------------
# Failures are isolated: if one source fails, the others are still loaded and
# the notebook fails at the end listing the failed sources, so the Job retries
# them. Re-running is safe: Auto Loader only picks up files not yet processed.
# =============================================================================
failed_sources = []

for source in SOURCES:
    target = cfg.table("bronze", source.name)
    try:
        with audit_run(cfg, "02_bronze", "bronze", target, JOB_RUN_ID) as run:
            run.rows_read = run.rows_written = ingest_to_bronze(cfg, source, run.run_id)
        print(f"{target}: {run.rows_written} new rows")
    except Exception as error:
        # Already recorded as FAILED by audit_run; keep loading the other sources
        failed_sources.append(source.name)
        print(f"{target}: FAILED ({type(error).__name__})")

if failed_sources:
    raise RuntimeError(f"Bronze ingestion failed for: {', '.join(failed_sources)}")