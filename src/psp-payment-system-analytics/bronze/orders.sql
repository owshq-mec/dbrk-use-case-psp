-- Bronze Layer: Orders
-- Raw order data with financial breakdowns (subtotal, tax, tips)
-- Source: Azure Blob Storage via ShadowTraffic generator

CREATE OR REFRESH STREAMING LIVE TABLE bronze_orders
COMMENT "Raw order transaction data from PSP system"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.zOrderCols" = "order_id,merchant_id,customer_id",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT
  order_id,
  merchant_id,
  customer_id,
  currency,
  subtotal_cents,
  tax_cents,
  tip_cents,
  total_amount_cents,
  channel,
  created_at,
  current_timestamp() AS ingestion_timestamp,
  input_file_name() AS source_file
FROM cloud_files(
  "/Volumes/psp/default/vol_landing_zone/orders*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaHints", "order_id STRING, merchant_id STRING, customer_id STRING, created_at TIMESTAMP",
    "cloudFiles.schemaLocation", "/Volumes/psp/default/vol_schema/orders"
  )
);
