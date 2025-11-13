CREATE OR REFRESH STREAMING LIVE TABLE bronze_merchants
COMMENT "Raw merchant account data from PSP system"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.zOrderCols" = "merchant_id",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT
  merchant_id,
  legal_name,
  mcc,
  country,
  kyb_status,
  pricing_tier,
  risk_level,
  created_at,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/psp/analytics/vol-landing-zone/merchants*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true"
  )
);
