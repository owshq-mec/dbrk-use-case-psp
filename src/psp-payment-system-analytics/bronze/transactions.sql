-- Bronze Layer: Transactions
-- Raw payment transaction data with state machines, fees, and 3DS information
-- Source: Azure Blob Storage via ShadowTraffic generator
-- Note: State is a nested struct with state_name and timestamp

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
  input_file_name() AS source_file
FROM cloud_files(
  "/Volumes/psp/default/vol_landing_zone/transactions*",
  "json",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaHints", "txn_id STRING, order_id STRING, payment_id STRING, authorized_at TIMESTAMP",
    "cloudFiles.schemaLocation", "/Volumes/psp/default/vol_schema/transactions"
  )
);
