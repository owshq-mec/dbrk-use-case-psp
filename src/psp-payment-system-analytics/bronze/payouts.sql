CREATE OR REFRESH STREAMING LIVE TABLE bronze_payouts
COMMENT "Raw merchant payout and settlement data from PSP system"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.zOrderCols" = "payout_id,merchant_id",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT
  payout_id,
  merchant_id,
  batch_day,
  currency,
  gross_cents,
  fees_cents,
  reserve_cents,
  net_cents,
  status,
  paid_at,
  transaction_count,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/psp/analytics/vol-landing-zone/payouts*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true"
  )
);
