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
# Setup: imports, configuration, quality rules and transformations
# =============================================================================
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from lib.audit import audit_run
from lib.config import Config
from lib.delta_utils import run_merge
from lib.quality import log_quality_issues
from lib.watermarks import get_watermark, set_watermark

cfg = Config(catalog=dbutils.widgets.get("catalog"))
JOB_RUN_ID = dbutils.widgets.get("job_run_id")

SOURCE = cfg.table("bronze", "transactions")
TARGET = cfg.table("silver", "transactions")

# Quality rules: name -> condition that a VALID row satisfies. Written null-safe,
# so "NOT (rule)" reliably finds the rows that break it. Same rules and names as
# the declarative pipeline (pipelines/bakehouse_declarative/transformations).
QUALITY_RULES = {
    "transaction_id_not_null": "transaction_id IS NOT NULL",
    "customer_id_not_null":    "customer_id IS NOT NULL",
    "quantity_positive":       "quantity IS NOT NULL AND quantity > 0",
    "no_rescued_data":         "_rescued_data IS NULL",
}


def to_silver_contract(bronze: DataFrame) -> DataFrame:
    """Map bronze sales to the silver contract: snake_case names, explicit types,
    DECIMAL for money and _change_ts = COALESCE(updated_at, created_at)."""
    return bronze.select(
        F.col("transactionID").cast("bigint").alias("transaction_id"),
        F.col("customerID").cast("bigint").alias("customer_id"),
        F.col("franchiseID").cast("bigint").alias("franchise_id"),
        F.col("dateTime").cast("timestamp").alias("transaction_ts"),
        F.to_date("dateTime").alias("transaction_date"),
        F.trim("product").alias("product"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unitPrice").cast("decimal(10,2)").alias("unit_price"),
        F.col("totalPrice").cast("decimal(12,2)").alias("total_price"),
        F.lower(F.trim("paymentMethod")).alias("payment_method"),
        F.col("discountPct").cast("decimal(5,2)").alias("discount_pct"),
        F.col("createdAt").cast("timestamp").alias("created_at"),
        F.col("updatedAt").cast("timestamp").alias("updated_at"),
        F.coalesce(F.col("updatedAt").cast("timestamp"),
                   F.col("createdAt").cast("timestamp")).alias("_change_ts"),
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_rescued_data"),
    )


def latest_version_per_sale(sales: DataFrame) -> DataFrame:
    """Keep one row per transaction_id: the newest source version; ties are broken
    by the latest arrival in bronze."""
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("_change_ts").desc(), F.col("_bronze_ingested_at").desc())
    return (sales.withColumn("_rn", F.row_number().over(window))
                 .filter("_rn = 1")
                 .drop("_rn"))

# COMMAND ----------

# =============================================================================
# Incremental load: new bronze rows -> quality gate -> latest version -> MERGE
# =============================================================================
with audit_run(cfg, "03_silver_transactions", "silver", TARGET, JOB_RUN_ID) as run:

    # 1. Only bronze rows ingested after the last successful load
    watermark = get_watermark(spark, cfg, SOURCE, TARGET)
    new_rows = spark.table(SOURCE)
    if watermark is not None:
        new_rows = new_rows.filter(F.col("_ingested_at") > F.lit(watermark))

    run.rows_read = new_rows.count()
    run.rows_written = run.rows_rejected = 0
    print(f"Previous watermark: {watermark} | new bronze rows: {run.rows_read}")

    if run.rows_read > 0:
        new_watermark = new_rows.agg(F.max("_ingested_at")).first()[0]
        typed = to_silver_contract(new_rows)

        # 2. Quality gate: log every broken rule; keep rows that pass all of them
        for rule_name, condition in QUALITY_RULES.items():
            log_quality_issues(cfg, typed.filter(f"NOT ({condition})"), run.run_id,
                               layer="silver", target_table=TARGET,
                               rule_name=rule_name, key="transaction_id")
        passes_all_rules = " AND ".join(f"({c})" for c in QUALITY_RULES.values())
        valid = typed.filter(passes_all_rules)
        run.rows_rejected = typed.filter(f"NOT ({passes_all_rules})").count()

        # 3. Latest version per sale, stamped with this run's lineage
        (latest_version_per_sale(valid)
            .drop("_rescued_data")
            .withColumn("_run_id", F.lit(run.run_id))
            .withColumn("_silver_updated_at", F.current_timestamp())
            .createOrReplaceTempView("_silver_upserts"))

        # 4. Upsert: a newer source version wins; equal or older ones are ignored
        #    (protects against out-of-order data and makes re-runs idempotent)
        run.rows_written = run_merge(spark, f"""
            MERGE INTO {TARGET} AS t
            USING _silver_upserts AS s
              ON t.transaction_id = s.transaction_id
            WHEN MATCHED AND s._change_ts > t._change_ts THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        # 5. Move the watermark only after a successful MERGE
        set_watermark(spark, cfg, SOURCE, TARGET, new_watermark, run.run_id)

print(f"Read: {run.rows_read} | written: {run.rows_written} | rejected: {run.rows_rejected}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Contract checks: one row per sale (duplicates = 0) and corrections applied
# MAGIC SELECT
# MAGIC   COUNT(*)                                   AS sales,
# MAGIC   COUNT(DISTINCT transaction_id)             AS distinct_sales,
# MAGIC   COUNT(*) - COUNT(DISTINCT transaction_id)  AS duplicates,
# MAGIC   COUNT(updated_at)                          AS modified_sales
# MAGIC FROM IDENTIFIER(:catalog || '.silver.transactions');