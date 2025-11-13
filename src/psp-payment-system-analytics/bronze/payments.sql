-- Bronze Layer: Payments
-- Raw payment instrument data (cards) with brand, BIN, and wallet information
-- Source: Azure Blob Storage via ShadowTraffic generator

CREATE OR REFRESH STREAMING LIVE TABLE bronze_payments
COMMENT "Raw payment instrument data from PSP system"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.zOrderCols" = "payment_id,customer_id",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT
  payment_id,
  customer_id,
  brand,
  bin,
  last4,
  expiry_month,
  expiry_year,
  wallet_type,
  status,
  first_seen_at,
  current_timestamp() AS ingestion_timestamp,
  input_file_name() AS source_file
FROM cloud_files(
  "/Volumes/psp/default/vol_landing_zone/payments*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaHints", "payment_id STRING, customer_id STRING, first_seen_at TIMESTAMP",
    "cloudFiles.schemaLocation", "/Volumes/psp/default/vol_schema/payments"
  )
);
