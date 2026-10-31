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
# Setup: imports, configuration and table names
# =============================================================================
from pyspark.sql import functions as F

from lib.audit import audit_run
from lib.config import Config
from lib.dimensions import run_merge
from lib.quality import log_rejected
from lib.watermarks import get_watermark, set_watermark

cfg = Config(catalog=dbutils.widgets.get("catalog"))
JOB_RUN_ID = dbutils.widgets.get("job_run_id")

SOURCE        = cfg.table("silver", "transactions")
TARGET        = cfg.table("gold", "fact_sales")
DIM_DATE      = cfg.table("gold", "dim_date")
DIM_CUSTOMER  = cfg.table("gold", "dim_customer")
DIM_FRANCHISE = cfg.table("gold", "dim_franchise")
DIM_PRODUCT   = cfg.table("gold", "dim_product")

# Surrogate key columns that fall back to the unknown member (-1) when not found
DIMENSION_KEYS = ["customer_key", "franchise_key", "product_key"]

# COMMAND ----------

# =============================================================================
# Incremental fact load: new silver rows -> key lookups -> MERGE
# =============================================================================
with audit_run(cfg, "04_gold_fact_sales", "gold", TARGET, JOB_RUN_ID) as run:

    # 1. Only silver rows written after the last successful load
    watermark = get_watermark(spark, cfg, SOURCE, TARGET)
    new_sales = spark.table(SOURCE)
    if watermark is not None:
        new_sales = new_sales.filter(F.col("_silver_updated_at") > F.lit(watermark))

    run.rows_read = new_sales.count()
    run.rows_rejected = 0
    print(f"Previous watermark: {watermark} | new silver rows: {run.rows_read}")

    if run.rows_read == 0:
        run.rows_written = 0
    else:
        new_watermark = new_sales.agg(F.max("_silver_updated_at")).first()[0]
        new_sales.createOrReplaceTempView("_new_sales")

        # 2. Surrogate key lookups
        #    - customer: point-in-time (version valid at the moment of the sale)
        #    - not found -> -1 (unknown member): the sale is kept, never dropped
        facts = spark.sql(f"""
            SELECT
              s.transaction_id,
              CAST(date_format(s.transaction_date, 'yyyyMMdd') AS INT) AS date_key,
              COALESCE(c.customer_key, -1)   AS customer_key,
              COALESCE(f.franchise_key, -1)  AS franchise_key,
              COALESCE(p.product_key, -1)    AS product_key,
              s.payment_method,
              s.quantity,
              s.unit_price,
              s.total_price,
              s.discount_pct,
              s.transaction_ts,
              s._silver_updated_at,
              '{run.run_id}'                 AS _run_id,
              current_timestamp()            AS _gold_updated_at
            FROM _new_sales s
            LEFT JOIN {DIM_CUSTOMER} c
              ON  c.customer_id = s.customer_id
              AND s.transaction_ts >= c.valid_from
              AND s.transaction_ts <  c.valid_to
            LEFT JOIN {DIM_FRANCHISE} f
              ON f.franchise_id = s.franchise_id
            LEFT JOIN {DIM_PRODUCT} p
              ON p.product_name = s.product
        """)

        # 3. Calendar check: a missing date is a configuration error -> fail loudly
        missing_dates = facts.join(spark.table(DIM_DATE), "date_key", "left_anti").count()
        if missing_dates:
            raise ValueError(f"{missing_dates} sales have dates outside dim_date. "
                             "Extend the calendar range in 04_gold_dimensions.")

        # 4. Unknown members: keep the fact, log it for follow-up
        for key in DIMENSION_KEYS:
            orphans = log_rejected(
                cfg, facts.filter(F.col(key) == -1), run.run_id, layer="gold",
                target_table=TARGET, rule_name=f"unknown_{key.removesuffix('_key')}",
                key="transaction_id")
            if orphans:
                print(f"Warning: {orphans} sales linked to unknown {key}")

        # 5. Upsert: update only when silver holds a newer version of the sale
        facts.createOrReplaceTempView("_facts")
        run.rows_written = run_merge(spark, f"""
            MERGE INTO {TARGET} AS t
            USING _facts AS s
              ON t.transaction_id = s.transaction_id
            WHEN MATCHED AND s._silver_updated_at > t._silver_updated_at THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        # 6. Move the watermark only after a successful MERGE
        set_watermark(spark, cfg, SOURCE, TARGET, new_watermark, run.run_id)

print(f"Read: {run.rows_read} | written: {run.rows_written}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reconciliation: every silver sale must be in the fact table, and unknown members
# MAGIC -- should be zero (non-zero = late-arriving or missing master data)
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM workspace.silver.transactions)                       AS silver_sales,
# MAGIC   (SELECT COUNT(*) FROM workspace.gold.fact_sales)                           AS fact_sales,
# MAGIC   (SELECT COUNT(*) FROM workspace.gold.fact_sales WHERE customer_key  = -1)  AS unknown_customer,
# MAGIC   (SELECT COUNT(*) FROM workspace.gold.fact_sales WHERE franchise_key = -1)  AS unknown_franchise,
# MAGIC   (SELECT COUNT(*) FROM workspace.gold.fact_sales WHERE product_key   = -1)  AS unknown_product;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 in action: sales of customers who moved stay in the city where they lived
# MAGIC -- when they bought (old version, is_current = false), not in Barcelona
# MAGIC SELECT c.customer_id, c.city, c.is_current,
# MAGIC        COUNT(*)           AS sales,
# MAGIC        SUM(f.total_price) AS revenue
# MAGIC FROM workspace.gold.fact_sales f
# MAGIC JOIN workspace.gold.dim_customer c
# MAGIC   ON f.customer_key = c.customer_key
# MAGIC WHERE c.customer_id IN (
# MAGIC   SELECT customer_id FROM workspace.gold.dim_customer
# MAGIC   GROUP BY customer_id HAVING COUNT(*) > 1
# MAGIC )
# MAGIC GROUP BY ALL
# MAGIC ORDER BY c.customer_id;