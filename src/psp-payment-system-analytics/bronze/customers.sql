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
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/psp/analytics/vol-landing-zone/customers*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true"
  )
);
