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
# Setup: imports, configuration and one loader function per dimension
# Each loader returns the number of rows inserted + updated.
# =============================================================================
from pyspark.sql import functions as F

from lib.audit import audit_run
from lib.config import Config
from lib.dimensions import apply_scd2, run_merge, upsert_scd1

cfg = Config(catalog=dbutils.widgets.get("catalog"))
JOB_RUN_ID = dbutils.widgets.get("job_run_id")

CALENDAR_START = "2020-01-01"
CALENDAR_END = "2030-12-31"


def load_dim_date(target: str) -> int:
    """Generate the calendar and insert only the missing days."""
    return run_merge(spark, f"""
        MERGE INTO {target} AS t
        USING (
          SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
                 d                                      AS date,
                 year(d)                                AS year,
                 quarter(d)                             AS quarter,
                 month(d)                               AS month,
                 date_format(d, 'MMMM')                 AS month_name,
                 day(d)                                 AS day,
                 weekday(d) + 1                         AS day_of_week,  -- ISO: 1 = Monday
                 date_format(d, 'EEEE')                 AS day_name,
                 weekday(d) >= 5                        AS is_weekend
          FROM (SELECT explode(sequence(DATE'{CALENDAR_START}', DATE'{CALENDAR_END}',
                                        INTERVAL 1 DAY)) AS d)
        ) AS s
        ON t.date_key = s.date_key
        WHEN NOT MATCHED THEN INSERT *
    """)


def load_dim_franchise(target: str) -> int:
    """SCD Type 1 from silver.franchises."""
    franchises = spark.table(cfg.table("silver", "franchises")).select(
        "franchise_id", "franchise_name", "city", "district",
        "country", "size", "latitude", "longitude")
    return upsert_scd1(
        franchises, target, key="franchise_id",
        attributes=["franchise_name", "city", "district", "country",
                    "size", "latitude", "longitude"])


def load_dim_product(target: str) -> int:
    """Insert products seen in silver sales that are not in the dimension yet."""
    return run_merge(spark, f"""
        MERGE INTO {target} AS t
        USING (
          SELECT DISTINCT product AS product_name
          FROM {cfg.table("silver", "transactions")}
          WHERE product IS NOT NULL
        ) AS s
        ON t.product_name = s.product_name
        WHEN NOT MATCHED THEN
          INSERT (product_name, _created_at) VALUES (s.product_name, current_timestamp())
    """)


def load_dim_customer(target: str) -> int:
    """SCD Type 2 from silver.customers (active customers only).

    Tracked (history): city, state, country.
    Type 1 (overwrite): full_name, email, continent, gender.
    """
    customers = (spark.table(cfg.table("silver", "customers"))
                 .filter("NOT is_deleted")
                 .select("customer_id",
                         F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
                         "email", "city", "state", "country", "continent", "gender"))
    return apply_scd2(
        customers, target, key="customer_id",
        tracked=["city", "state", "country"],
        type1=["full_name", "email", "continent", "gender"])


# Dimensions processed by this notebook: (table name, loader)
DIMENSIONS = [
    ("dim_date", load_dim_date),
    ("dim_franchise", load_dim_franchise),
    ("dim_product", load_dim_product),
    ("dim_customer", load_dim_customer),
]

# COMMAND ----------

# =============================================================================
# Load all dimensions (one audited run per dimension)
# =============================================================================
for name, loader in DIMENSIONS:
    target = cfg.table("gold", name)
    with audit_run(cfg, "04_gold_dimensions", "gold", target, JOB_RUN_ID) as run:
        run.rows_written = loader(target)
    print(f"{target}: {run.rows_written} rows inserted/updated")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dimension sizes (the -1 unknown member is included in each count)
# MAGIC SELECT 'dim_date' AS dimension, COUNT(*) AS row_count FROM workspace.gold.dim_date
# MAGIC UNION ALL SELECT 'dim_franchise', COUNT(*) FROM workspace.gold.dim_franchise
# MAGIC UNION ALL SELECT 'dim_product',   COUNT(*) FROM workspace.gold.dim_product
# MAGIC UNION ALL SELECT 'dim_customer',  COUNT(*) FROM workspace.gold.dim_customer;