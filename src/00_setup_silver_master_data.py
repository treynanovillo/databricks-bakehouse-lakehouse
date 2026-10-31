# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- Silver layer DDL: master data (full-load snapshots, SCD Type 1)
# MAGIC -- One row per business key with its latest values. Rows missing from the latest
# MAGIC -- snapshot are soft-deleted (is_deleted = TRUE), never physically removed.
# MAGIC -- Idempotent: safe to re-run.
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS workspace.silver.customers (
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
# MAGIC CREATE TABLE IF NOT EXISTS workspace.silver.franchises (
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