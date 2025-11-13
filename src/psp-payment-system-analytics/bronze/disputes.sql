-- Bronze Layer: Disputes
-- Raw chargeback and dispute data with reason codes and liability
-- Source: Azure Blob Storage via ShadowTraffic generator

CREATE OR REFRESH STREAMING LIVE TABLE bronze_disputes
COMMENT "Raw dispute and chargeback data from PSP system"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.zOrderCols" = "dispute_id,txn_id",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT
  dispute_id,
  txn_id,
  reason_code,
  amount_cents,
  stage,
  opened_at,
  closed_at,
  liability,
  status,
  current_timestamp() AS ingestion_timestamp,
  input_file_name() AS source_file
FROM cloud_files(
  "/Volumes/psp/default/vol_landing_zone/disputes*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaHints", "dispute_id STRING, txn_id STRING, opened_at TIMESTAMP, closed_at TIMESTAMP",
    "cloudFiles.schemaLocation", "/Volumes/psp/default/vol_schema/disputes"
  )
);
