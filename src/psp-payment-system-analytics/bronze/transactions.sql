CREATE OR REFRESH STREAMING LIVE TABLE bronze_transactions
COMMENT "Raw payment transaction data from PSP system with state lifecycle"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.autoOptimize.zOrderCols" = "txn_id,order_id,payment_id",
  "delta.enableChangeDataFeed" = "true"
)
AS SELECT
  txn_id,
  order_id,
  payment_id,
  amount_cents,
  currency,
  state.state_name AS state_name,
  state.timestamp AS state_timestamp,
  state,
  response_code,
  three_ds,
  authorized_at,
  fees_total_cents,
  network_fee_cents,
  processor_name,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "/Volumes/psp/analytics/vol-landing-zone/transactions*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true"
  )
);
