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
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/psp/analytics/vol-landing-zone/disputes*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true"
  )
);
