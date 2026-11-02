# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# =============================================================================
# Parameters (validated: fail fast with a clear message instead of an obscure SQL error)
# =============================================================================
dbutils.widgets.text("catalog", "workspace", "Catalog")

if not dbutils.widgets.get("catalog").strip():
    raise ValueError("Parameter 'catalog' is required (e.g. 'workspace', 'dev', 'prod').")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- Platform setup: schemas, volumes and operational tables
# MAGIC -- Idempotent: safe to re-run. Run once per environment before any pipeline.
# MAGIC -- The catalog comes from the widget, so the same notebook serves dev and prod.
# MAGIC -- =============================================================================
# MAGIC USE CATALOG IDENTIFIER(:catalog);
# MAGIC
# MAGIC -- Medallion and operational schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS raw           COMMENT 'Landing zone for files delivered by source systems';
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze        COMMENT 'Raw data as received, plus lineage columns';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver        COMMENT 'Clean, typed and deduplicated data';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold          COMMENT 'Star schema for BI and data marts';
# MAGIC CREATE SCHEMA IF NOT EXISTS ops           COMMENT 'Run audit, data quality and load control';
# MAGIC CREATE SCHEMA IF NOT EXISTS bakehouse_dlt COMMENT 'Declarative (Lakeflow) version of the sales pipeline';
# MAGIC
# MAGIC -- Volumes
# MAGIC CREATE VOLUME IF NOT EXISTS raw.landing     COMMENT 'Incoming files from source systems';
# MAGIC CREATE VOLUME IF NOT EXISTS ops.checkpoints COMMENT 'Auto Loader and streaming state';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- Operational tables (append-only)
# MAGIC -- =============================================================================
# MAGIC USE CATALOG IDENTIFIER(:catalog);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
# MAGIC   run_id        STRING    COMMENT 'Unique id of this task run',
# MAGIC   job_run_id    STRING    COMMENT 'Run id of the orchestrator (Job / ADF) that launched it',
# MAGIC   pipeline      STRING    COMMENT 'Pipeline name',
# MAGIC   task          STRING    COMMENT 'Notebook or task name',
# MAGIC   layer         STRING    COMMENT 'raw | bronze | silver | gold',
# MAGIC   target_table  STRING    COMMENT 'Table written by the task',
# MAGIC   status        STRING    COMMENT 'RUNNING | SUCCESS | FAILED',
# MAGIC   rows_read     BIGINT    COMMENT 'Rows read from the source',
# MAGIC   rows_written  BIGINT    COMMENT 'Rows inserted or updated in the target',
# MAGIC   rows_rejected BIGINT    COMMENT 'Rows excluded by quality rules',
# MAGIC   error_message STRING    COMMENT 'Error type and message when status = FAILED',
# MAGIC   started_at    TIMESTAMP COMMENT 'Start of the run (UTC)',
# MAGIC   finished_at   TIMESTAMP COMMENT 'End of the run (UTC); NULL while RUNNING',
# MAGIC   executed_by   STRING    COMMENT 'Identity that executed the run'
# MAGIC ) COMMENT 'Append-only run log: one RUNNING row and one final row per task run';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ops.data_quality_issues (
# MAGIC   run_id         STRING    COMMENT 'Run that detected the issue',
# MAGIC   layer          STRING    COMMENT 'Layer where the rule was evaluated',
# MAGIC   target_table   STRING    COMMENT 'Table the record was meant for',
# MAGIC   rule_name      STRING    COMMENT 'Quality rule that was broken',
# MAGIC   record_key     STRING    COMMENT 'Business key of the record',
# MAGIC   record_payload STRING    COMMENT 'Full record as JSON, for analysis or reprocessing',
# MAGIC   detected_at    TIMESTAMP COMMENT 'When the issue was detected (UTC)'
# MAGIC ) COMMENT 'Records rejected or flagged by data quality rules';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ops.watermarks (
# MAGIC   source_table  STRING    COMMENT 'Table read by the incremental load',
# MAGIC   target_table  STRING    COMMENT 'Table written by the incremental load',
# MAGIC   watermark_ts  TIMESTAMP COMMENT 'Last source timestamp processed successfully',
# MAGIC   run_id        STRING    COMMENT 'Run that moved the watermark',
# MAGIC   updated_at    TIMESTAMP COMMENT 'When the watermark was recorded'
# MAGIC ) COMMENT 'Append-only watermarks for incremental loads between layers';
# MAGIC
# MAGIC CREATE OR REPLACE VIEW ops.v_pipeline_runs
# MAGIC COMMENT 'Final state of every task run (latest row per run_id)'
# MAGIC AS
# MAGIC SELECT * EXCEPT (rn)
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY run_id
# MAGIC                             ORDER BY COALESCE(finished_at, started_at) DESC) AS rn
# MAGIC   FROM ops.pipeline_runs
# MAGIC )
# MAGIC WHERE rn = 1;