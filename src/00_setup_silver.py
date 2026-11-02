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
# MAGIC -- Silver layer DDL: sales (incremental, SCD Type 1)
# MAGIC -- Defined explicitly (data contract), never inferred. Idempotent: safe to re-run.
# MAGIC -- =============================================================================
# MAGIC USE CATALOG IDENTIFIER(:catalog);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.transactions (
# MAGIC   transaction_id      BIGINT        NOT NULL COMMENT 'Business key: source transaction id',
# MAGIC   customer_id         BIGINT        NOT NULL COMMENT 'Customer business key',
# MAGIC   franchise_id        BIGINT                 COMMENT 'Franchise (store) business key',
# MAGIC   transaction_ts      TIMESTAMP              COMMENT 'Sale timestamp (UTC)',
# MAGIC   transaction_date    DATE                   COMMENT 'Sale date',
# MAGIC   product             STRING                 COMMENT 'Product name',
# MAGIC   quantity            INT                    COMMENT 'Units sold',
# MAGIC   unit_price          DECIMAL(10,2)          COMMENT 'Unit price',
# MAGIC   total_price         DECIMAL(12,2)          COMMENT 'Sale amount',
# MAGIC   payment_method      STRING                 COMMENT 'Payment method (lower case)',
# MAGIC   discount_pct        DECIMAL(5,2)           COMMENT 'Discount rate (only since the source started sending it)',
# MAGIC   created_at          TIMESTAMP              COMMENT 'Creation time in the source system',
# MAGIC   updated_at          TIMESTAMP              COMMENT 'Last change in the source system (NULL if never changed)',
# MAGIC   _change_ts          TIMESTAMP              COMMENT 'COALESCE(updated_at, created_at): version of the record',
# MAGIC   _source_file        STRING                 COMMENT 'Source file of this version',
# MAGIC   _bronze_ingested_at TIMESTAMP              COMMENT 'When the record reached bronze',
# MAGIC   _run_id             STRING                 COMMENT 'Run that wrote this version',
# MAGIC   _silver_updated_at  TIMESTAMP              COMMENT 'Last write in silver',
# MAGIC   CONSTRAINT pk_silver_transactions PRIMARY KEY (transaction_id)
# MAGIC )
# MAGIC CLUSTER BY (transaction_date)
# MAGIC COMMENT 'Clean, typed and deduplicated sales: one row per transaction_id (latest version)';
# MAGIC
# MAGIC -- Enforced quality rule: the last line of defence if a bug bypasses the quality gate.
# MAGIC -- DROP + ADD keeps the cell re-runnable.
# MAGIC ALTER TABLE silver.transactions DROP CONSTRAINT IF EXISTS quantity_positive;
# MAGIC ALTER TABLE silver.transactions ADD CONSTRAINT quantity_positive CHECK (quantity > 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- Silver layer DDL: master data (full-load snapshots, SCD Type 1)
# MAGIC -- One row per business key with its latest values. Rows missing from the latest
# MAGIC -- snapshot are soft-deleted (is_deleted = TRUE), never physically removed.
# MAGIC -- =============================================================================
# MAGIC USE CATALOG IDENTIFIER(:catalog);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.customers (
# MAGIC   customer_id         BIGINT    NOT NULL COMMENT 'Business key from the source system',
# MAGIC   first_name          STRING             COMMENT 'First name (title case)',
# MAGIC   last_name           STRING             COMMENT 'Last name (title case)',
# MAGIC   email               STRING             COMMENT 'Email address (lower case)',
# MAGIC   phone               STRING             COMMENT 'Phone number',
# MAGIC   address             STRING             COMMENT 'Street address',
# MAGIC   city                STRING             COMMENT 'City',
# MAGIC   state               STRING             COMMENT 'State / province',
# MAGIC   country             STRING             COMMENT 'Country',
# MAGIC   continent           STRING             COMMENT 'Continent',
# MAGIC   postal_code         STRING             COMMENT 'Postal code (string: may contain leading zeros or letters)',
# MAGIC   gender              STRING             COMMENT 'Gender',
# MAGIC   is_deleted          BOOLEAN   NOT NULL COMMENT 'TRUE if missing from the latest source snapshot',
# MAGIC   _snapshot_id        STRING             COMMENT 'Source snapshot this version comes from',
# MAGIC   _source_file        STRING             COMMENT 'Source file of this version',
# MAGIC   _bronze_ingested_at TIMESTAMP          COMMENT 'When the snapshot reached bronze',
# MAGIC   _row_hash           STRING             COMMENT 'SHA-256 of business attributes (change detection)',
# MAGIC   _run_id             STRING             COMMENT 'Run that last wrote this row',
# MAGIC   _silver_updated_at  TIMESTAMP          COMMENT 'Last write in silver',
# MAGIC   CONSTRAINT pk_silver_customers PRIMARY KEY (customer_id)
# MAGIC )
# MAGIC COMMENT 'Customers master data: latest version per customer (SCD Type 1)';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.franchises (
# MAGIC   franchise_id        BIGINT    NOT NULL COMMENT 'Business key from the source system',
# MAGIC   franchise_name      STRING             COMMENT 'Store name',
# MAGIC   city                STRING             COMMENT 'City',
# MAGIC   district            STRING             COMMENT 'District',
# MAGIC   zip_code            STRING             COMMENT 'Zip code',
# MAGIC   country             STRING             COMMENT 'Country',
# MAGIC   size                STRING             COMMENT 'Store size',
# MAGIC   latitude            DOUBLE             COMMENT 'Latitude',
# MAGIC   longitude           DOUBLE             COMMENT 'Longitude',
# MAGIC   supplier_id         BIGINT             COMMENT 'Main supplier',
# MAGIC   is_deleted          BOOLEAN   NOT NULL COMMENT 'TRUE if missing from the latest source snapshot',
# MAGIC   _snapshot_id        STRING             COMMENT 'Source snapshot this version comes from',
# MAGIC   _source_file        STRING             COMMENT 'Source file of this version',
# MAGIC   _bronze_ingested_at TIMESTAMP          COMMENT 'When the snapshot reached bronze',
# MAGIC   _row_hash           STRING             COMMENT 'SHA-256 of business attributes (change detection)',
# MAGIC   _run_id             STRING             COMMENT 'Run that last wrote this row',
# MAGIC   _silver_updated_at  TIMESTAMP          COMMENT 'Last write in silver',
# MAGIC   CONSTRAINT pk_silver_franchises PRIMARY KEY (franchise_id)
# MAGIC )
# MAGIC COMMENT 'Franchises master data: latest version per store (SCD Type 1)';