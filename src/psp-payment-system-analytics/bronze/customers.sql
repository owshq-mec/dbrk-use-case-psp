-- Bronze Layer: Customers
-- Raw customer profile data with hashed PII and customer segmentation
-- Source: Azure Blob Storage via ShadowTraffic generator

CREATE OR REFRESH STREAMING LIVE TABLE bronze_customers
COMMENT "Raw customer profile data from PSP system"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.zOrderCols" = "customer_id",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT
  customer_id,
  email_hash,
  phone_hash,
  customer_type,
  created_at,
  current_timestamp() AS ingestion_timestamp,
  input_file_name() AS source_file
FROM cloud_files(
  "/Volumes/psp/default/vol_landing_zone/customers*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaHints", "customer_id STRING, created_at TIMESTAMP",
    "cloudFiles.schemaLocation", "/Volumes/psp/default/vol_schema/customers"
  )
);
