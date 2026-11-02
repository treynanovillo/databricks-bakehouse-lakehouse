# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# =============================================================================
# Parameters
#   catalog          target environment
#   sales_batch      daily sales extract to deliver: "0".."4", "all" or "none"
#   master_snapshot  master data snapshot to deliver: "1", "2" or "none"
# =============================================================================
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.dropdown("sales_batch", "none",
                         ["none", "0", "1", "2", "3", "4", "all"], "Sales batch")
dbutils.widgets.dropdown("master_snapshot", "none", ["none", "1", "2"], "Master data snapshot")

if not dbutils.widgets.get("catalog").strip():
    raise ValueError("Parameter 'catalog' is required (e.g. 'workspace', 'dev', 'prod').")

# COMMAND ----------

# =============================================================================
# Source system simulator
# -----------------------------------------------------------------------------
# Plays the role of the source systems (or an ADF Copy activity): it delivers
# files to the landing volume. It is NOT part of the pipeline and never runs
# in the Job.
#
# Sales (incremental daily extracts, built from samples.bakehouse):
#   batch 2  schema drift: the source adds a new column (discountPct)
#   batch 3  20 sales of batch 0 modified in the source (new quantity, updatedAt)
#   batch 4  invalid rows (negative quantity, missing customer) + a wrong data type
# Master data (full-load snapshots):
#   snapshot 2  1 in 50 customers moves to Barcelona (SCD2-tracked attributes),
#               1 in 40 customers changes email (SCD1 attribute)
# =============================================================================
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lib.config import Config

cfg = Config(catalog=dbutils.widgets.get("catalog"))

SALES_LANDING = f"{cfg.landing_path}/transactions"
NUM_SALES_BATCHES = 5


def already_delivered(path: str) -> bool:
    """True if a delivery folder exists. Volumes are mounted as a local file system."""
    return os.path.exists(path)


def write_json(df: DataFrame, path: str) -> None:
    """Write JSON files keeping explicit nulls (absent keys would look like schema drift)."""
    df.write.option("ignoreNullFields", "false").json(path)


# -----------------------------------------------------------------------------
# Sales: incremental daily extracts
# -----------------------------------------------------------------------------
def source_sales() -> DataFrame:
    """Sample sales with the audit columns a real ERP would expose.

    createdAt = sale time; updatedAt = NULL until the record is modified.
    Each sale is assigned to a stable batch (same sale -> same batch on every run).
    """
    return (spark.table("samples.bakehouse.sales_transactions")
            .withColumn("createdAt", F.col("dateTime"))
            .withColumn("updatedAt", F.lit(None).cast("timestamp"))
            .withColumn("_batch", F.abs(F.hash("transactionID")) % NUM_SALES_BATCHES))


def deliver_sales_batch(batch: int) -> None:
    """Deliver one daily sales extract to the landing volume (idempotent)."""
    target = f"{SALES_LANDING}/batch_{batch}"
    if already_delivered(target):
        print(f"Sales batch {batch} already delivered: skipped")
        return

    sales = source_sales()
    extract = sales.filter(F.col("_batch") == batch).drop("_batch")

    if batch == 2:  # schema drift
        extract = extract.withColumn("discountPct", F.round(F.rand(seed=42) * 0.15, 2))

    if batch == 3:  # modifications of previously delivered sales
        modified = (sales.filter(F.col("_batch") == 0).limit(20).drop("_batch")
                    .withColumn("quantity", F.col("quantity") + 1)
                    .withColumn("totalPrice", F.col("quantity") * F.col("unitPrice"))
                    .withColumn("updatedAt", F.current_timestamp()))
        extract = extract.unionByName(modified)

    if batch == 4:  # invalid rows
        negative_qty = (extract.limit(3)
                        .withColumn("transactionID", F.col("transactionID") + 10_000_000)
                        .withColumn("quantity", F.lit(-1)))
        missing_customer = (extract.limit(2)
                            .withColumn("transactionID", F.col("transactionID") + 20_000_000)
                            .withColumn("customerID", F.lit(None).cast("long")))
        extract = extract.unionByName(negative_qty).unionByName(missing_customer)

    write_json(extract, target)
    print(f"Sales batch {batch}: {extract.count()} rows -> {target}")

    if batch == 4:  # a record whose quantity arrives as text
        dbutils.fs.put(
            f"{SALES_LANDING}/batch_4_wrong_type/part-00000.json",
            '{"transactionID": 30000001, "customerID": 1000000, "franchiseID": 3000000, '
            '"dateTime": "2024-05-20T10:00:00", "product": "Croissant", '
            '"quantity": "two", "unitPrice": 3, "totalPrice": 6, "paymentMethod": "visa", '
            '"createdAt": "2024-05-20T10:00:00", "updatedAt": null}\n',
            overwrite=True)
        print("Sales batch 4: + 1 record with quantity as text")


# -----------------------------------------------------------------------------
# Master data: full-load snapshots
# -----------------------------------------------------------------------------
def deliver_master_snapshot(version: int) -> None:
    """Deliver a full snapshot of customers and franchises (idempotent)."""
    customers = spark.table("samples.bakehouse.sales_customers")
    franchises = spark.table("samples.bakehouse.sales_franchises")

    if version >= 2:
        moves = F.col("customerID") % 50 == 0
        new_email = F.col("customerID") % 40 == 0
        customers = (customers
            .withColumn("city",    F.when(moves, F.lit("Barcelona")).otherwise(F.col("city")))
            .withColumn("state",   F.when(moves, F.lit("Catalonia")).otherwise(F.col("state")))
            .withColumn("country", F.when(moves, F.lit("Spain")).otherwise(F.col("country")))
            .withColumn("email_address",
                        F.when(new_email, F.concat(F.lit("new."), F.col("email_address")))
                         .otherwise(F.col("email_address"))))

    for name, snapshot in {"customers": customers, "franchises": franchises}.items():
        target = f"{cfg.landing_path}/{name}/snapshot_v{version}"
        if already_delivered(target):
            print(f"{name} snapshot v{version} already delivered: skipped")
            continue
        write_json(snapshot, target)
        print(f"{name} snapshot v{version}: {snapshot.count()} rows -> {target}")

# COMMAND ----------

# =============================================================================
# Deliver what the parameters ask for
# =============================================================================
sales_batch = dbutils.widgets.get("sales_batch")
if sales_batch != "none":
    batches = range(NUM_SALES_BATCHES) if sales_batch == "all" else [int(sales_batch)]
    for batch in batches:
        deliver_sales_batch(batch)

master_snapshot = dbutils.widgets.get("master_snapshot")
if master_snapshot != "none":
    deliver_master_snapshot(int(master_snapshot))

# COMMAND ----------

# =============================================================================
# Landing volume contents
# =============================================================================
for source in ("transactions", "customers", "franchises"):
    path = f"{cfg.landing_path}/{source}"
    folders = sorted(os.listdir(path)) if already_delivered(path) else []
    print(f"{source}: {folders}")