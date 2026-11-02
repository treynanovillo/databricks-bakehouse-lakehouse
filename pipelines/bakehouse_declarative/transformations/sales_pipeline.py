"""Declarative version of the Bakehouse sales pipeline (Lakeflow Declarative Pipelines).

Same logic as the imperative notebooks (02_bronze, 03_silver_transactions,
04_gold_*), written as table declarations. The pipeline infers the dependency
graph, manages checkpoints and tracks data quality through expectations.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = "/Volumes/workspace/raw/landing/transactions"
CUSTOMERS_BRONZE = "workspace.bronze.customers"

# Quality rules for a sale. Written null-safe on purpose: every rule must be
# TRUE for a valid row, so the quarantine can simply negate them.
VALID_SALE = {
    "transaction_id_not_null": "transaction_id IS NOT NULL",
    "customer_id_not_null":    "customer_id IS NOT NULL",
    "quantity_positive":       "quantity IS NOT NULL AND quantity > 0",
    "no_rescued_data":         "_rescued_data IS NULL",
}


# =============================================================================
# Bronze: raw files as received (Auto Loader; checkpoint and schema are managed)
# =============================================================================
@dp.table(
    name="bronze_transactions",
    comment="Raw sales files as received from the source system (Auto Loader).",
    table_properties={"quality": "bronze"},
)
def bronze_transactions():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints",
                    "transactionID BIGINT, customerID BIGINT, franchiseID BIGINT, "
                    "quantity BIGINT, dateTime TIMESTAMP, "
                    "createdAt TIMESTAMP, updatedAt TIMESTAMP")
            .load(LANDING_PATH)
            .select("*",
                    F.current_timestamp().alias("_ingested_at"),
                    F.col("_metadata.file_path").alias("_source_file")))


# =============================================================================
# Silver: typed sales -> quality gate -> SCD1 upsert
# =============================================================================
@dp.view(comment="Sales mapped to the silver contract (names and types), before quality checks.")
def sales_typed():
    return spark.readStream.table("bronze_transactions").select(
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
        F.coalesce(F.col("updatedAt"), F.col("createdAt")).alias("_change_ts"),
        F.col("_rescued_data"),
        F.col("_source_file"),
    )


@dp.view(comment="Valid sales only. Dropped rows are counted per rule in the pipeline metrics.")
@dp.expect_all_or_drop(VALID_SALE)
def sales_valid():
    return spark.readStream.table("sales_typed")


@dp.table(
    name="quarantine_transactions",
    comment="Sales that failed at least one quality rule, kept for analysis.",
    table_properties={"quality": "silver"},
)
def quarantine_transactions():
    any_rule_failed = " OR ".join(f"NOT ({rule})" for rule in VALID_SALE.values())
    return spark.readStream.table("sales_typed").filter(F.expr(any_rule_failed))


# Upsert by business key: the latest version (by source change time) wins.
# Replaces the watermark + ROW_NUMBER + MERGE of the imperative silver notebook.
dp.create_streaming_table(
    name="silver_transactions",
    comment="Clean, deduplicated sales: one row per transaction_id (latest version).",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    target="silver_transactions",
    source="sales_valid",
    keys=["transaction_id"],
    sequence_by=F.col("_change_ts"),
    except_column_list=["_rescued_data"],
    stored_as_scd_type=1,
)


# =============================================================================
# Customers: SCD Type 2 from the master data snapshots already in bronze
# =============================================================================
@dp.view(comment="Customer snapshots mapped to dimension attributes, ordered by snapshot time.")
def customer_changes():
    return spark.readStream.table(CUSTOMERS_BRONZE).select(
        F.col("customerID").cast("bigint").alias("customer_id"),
        F.concat_ws(" ", F.initcap("first_name"), F.initcap("last_name")).alias("full_name"),
        F.lower(F.trim("email_address")).alias("email"),
        F.trim("city").alias("city"),
        F.trim("state").alias("state"),
        F.trim("country").alias("country"),
        F.col("_source_file_ts").alias("_snapshot_ts"),
    )


# History is kept only for location changes; an email change updates in place.
# Replaces lib/dimensions.apply_scd2 (merge-key technique, ~100 lines).
dp.create_streaming_table(
    name="dim_customer_scd2",
    comment="Customer dimension with history of location changes (SCD Type 2).",
    table_properties={"quality": "gold"},
)
dp.create_auto_cdc_flow(
    target="dim_customer_scd2",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by=F.col("_snapshot_ts"),
    stored_as_scd_type=2,
    track_history_column_list=["city", "state", "country"],
)


# =============================================================================
# Gold: daily revenue per product (materialized view, refreshed by the pipeline)
# =============================================================================
@dp.materialized_view(
    name="gold_daily_product_sales",
    comment="Revenue, units and transactions per day and product.",
    table_properties={"quality": "gold"},
)
def gold_daily_product_sales():
    return (spark.read.table("silver_transactions")
            .groupBy("transaction_date", "product")
            .agg(F.sum("total_price").alias("revenue"),
                 F.sum("quantity").alias("units"),
                 F.count("transaction_id").alias("transactions")))