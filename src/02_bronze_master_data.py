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
# Bronze ingestion for master data sources (metadata-driven)
# -----------------------------------------------------------------------------
# Each source is processed with the same reusable function. Adding a new source
# means adding one line to SOURCES, not writing a new notebook.
# In a larger project this list would live in a config table or YAML file.
# =============================================================================
from lib.audit import audit_run
from lib.config import Config
from lib.ingestion import FileSource, ingest_to_bronze

cfg = Config(catalog=dbutils.widgets.get("catalog"))
JOB_RUN_ID = dbutils.widgets.get("job_run_id")

SOURCES = [
    FileSource("customers",  schema_hints="customerID BIGINT"),
    FileSource("franchises", schema_hints="franchiseID BIGINT, latitude DOUBLE, longitude DOUBLE"),
]

for source in SOURCES:
    target = cfg.table("bronze", source.name)
    with audit_run(cfg, "02_bronze_master_data", "bronze", target, JOB_RUN_ID) as run:
        run.rows_read = run.rows_written = ingest_to_bronze(cfg, source, run.run_id)
    print(f"{target}: {run.rows_written} new rows")