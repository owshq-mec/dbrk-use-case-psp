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
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/psp/analytics/vol-landing-zone/payments*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true"
  )
);
