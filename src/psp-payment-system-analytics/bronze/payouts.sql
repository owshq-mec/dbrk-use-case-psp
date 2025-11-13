-- Bronze Layer: Payouts
-- Raw merchant payout/settlement data with fees and reserves
-- Source: Azure Blob Storage via ShadowTraffic generator

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
  input_file_name() AS source_file
FROM cloud_files(
  "/Volumes/psp/default/vol_landing_zone/payouts*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaHints", "payout_id STRING, merchant_id STRING, batch_day DATE, paid_at TIMESTAMP",
    "cloudFiles.schemaLocation", "/Volumes/psp/default/vol_schema/payouts"
  )
);
