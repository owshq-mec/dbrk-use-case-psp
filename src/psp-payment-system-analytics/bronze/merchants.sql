-- Bronze Layer: Merchants
-- Raw merchant account data with KYB status, risk levels, and pricing tiers
-- Source: Azure Blob Storage via ShadowTraffic generator

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
  input_file_name() AS source_file
FROM cloud_files(
  "/Volumes/psp/default/vol_landing_zone/merchants*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaHints", "merchant_id STRING, created_at TIMESTAMP",
    "cloudFiles.schemaLocation", "/Volumes/psp/default/vol_schema/merchants"
  )
);
