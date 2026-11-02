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
# Data smoke tests: run after every load, last task of the Job
# -----------------------------------------------------------------------------
# Each check is a query that counts VIOLATIONS (expected: 0).
#   error    must never happen -> the task fails and the Job alerts
#   warning  can happen legitimately but needs a look -> reported only
# =============================================================================
from lib.audit import audit_run
from lib.config import Config

cfg = Config(catalog=dbutils.widgets.get("catalog"))
JOB_RUN_ID = dbutils.widgets.get("job_run_id")

SILVER_SALES  = cfg.table("silver", "transactions")
FACT_SALES    = cfg.table("gold", "fact_sales")
DIM_CUSTOMER  = cfg.table("gold", "dim_customer")
DIM_FRANCHISE = cfg.table("gold", "dim_franchise")
DIM_PRODUCT   = cfg.table("gold", "dim_product")

CHECKS = [
    ("error", "silver sales are unique per transaction_id", f"""
        SELECT COUNT(*) - COUNT(DISTINCT transaction_id) FROM {SILVER_SALES}"""),

    ("error", "every silver sale reached fact_sales", f"""
        SELECT COUNT(*) FROM {SILVER_SALES} s
        LEFT ANTI JOIN {FACT_SALES} f ON s.transaction_id = f.transaction_id"""),

    ("error", "revenue reconciles between silver and gold", f"""
        SELECT CAST(ABS((SELECT COALESCE(SUM(total_price), 0) FROM {SILVER_SALES})
                      - (SELECT COALESCE(SUM(total_price), 0) FROM {FACT_SALES})) AS BIGINT)"""),

    ("error", "every dimension has its unknown member (-1)", f"""
        SELECT 3 - (  (SELECT COUNT(*) FROM {DIM_CUSTOMER}  WHERE customer_key  = -1)
                    + (SELECT COUNT(*) FROM {DIM_FRANCHISE} WHERE franchise_key = -1)
                    + (SELECT COUNT(*) FROM {DIM_PRODUCT}   WHERE product_key   = -1))"""),

    ("error", "exactly one current version per customer", f"""
        SELECT COUNT(*) FROM (
          SELECT customer_id FROM {DIM_CUSTOMER}
          WHERE is_current AND customer_key <> -1
          GROUP BY customer_id HAVING COUNT(*) <> 1)"""),

    ("error", "no gaps or overlaps in customer history (SCD2)", f"""
        SELECT COUNT(*) FROM (
          SELECT valid_to,
                 LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) AS next_from
          FROM {DIM_CUSTOMER})
        WHERE next_from IS NOT NULL AND next_from <> valid_to"""),

    ("warning", "sales linked to unknown members (late-arriving master data)", f"""
        SELECT COUNT(*) FROM {FACT_SALES}
        WHERE customer_key = -1 OR franchise_key = -1 OR product_key = -1"""),
]

# COMMAND ----------

# =============================================================================
# Run every check, report all results, fail only on errors
# =============================================================================
with audit_run(cfg, "99_smoke_tests", "gold", FACT_SALES, JOB_RUN_ID) as run:
    failed_errors = []
    run.rows_read = len(CHECKS)

    for severity, name, query in CHECKS:
        violations = spark.sql(query).first()[0]
        status = "PASS" if violations == 0 else severity.upper()
        print(f"[{status:7}] {name}: {violations}")
        if violations and severity == "error":
            failed_errors.append(name)

    run.rows_rejected = len(failed_errors)
    if failed_errors:
        raise AssertionError(f"{len(failed_errors)} data check(s) failed: {failed_errors}")