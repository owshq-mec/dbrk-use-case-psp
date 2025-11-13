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
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/psp/analytics/vol-landing-zone/orders*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true"
  )
);
