-- Silver Layer 1: Transactions
-- Cleansed and conformed transaction data with data quality checks
-- Transformations: Flatten state, convert timestamps, calculate fees, validate lifecycle

CREATE OR REFRESH STREAMING LIVE TABLE silver_transactions (
  CONSTRAINT valid_txn_id EXPECT (txn_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_payment_id EXPECT (payment_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount_cents > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_currency EXPECT (currency IN ('USD', 'GBP', 'CAD', 'AUD')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_state EXPECT (state_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_response_code EXPECT (response_code IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_authorized_at EXPECT (authorized_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_processor EXPECT (processor_name IN ('stripe', 'adyen', 'braintree', 'worldpay', 'authorize_net')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_3ds_status EXPECT (three_ds IN ('none', 'attempted', 'frictionless', 'challenge', 'failed')) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and conformed payment transaction data with state lifecycle"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.zOrderCols" = "txn_id,order_id,payment_id,state_name"
)
AS SELECT
  -- Primary Key
  txn_id,

  -- Foreign Keys
  order_id,
  payment_id,

  -- Transaction Amount
  currency AS transaction_currency,
  amount_cents,
  round(amount_cents / 100.0, 2) AS transaction_amount,

  -- Transaction State (Flattened from nested struct)
  lower(state_name) AS transaction_state,
  from_unixtime(state_timestamp / 1000) AS state_timestamp,

  -- State Classification
  CASE
    WHEN state_name IN ('completed', 'closed') THEN 'terminal_success'
    WHEN state_name IN ('failed', 'cancelled') THEN 'terminal_failure'
    WHEN state_name IN ('disputed', 'under_review', 'chargeback') THEN 'disputed'
    WHEN state_name IN ('refunded', 'refund_pending') THEN 'refunded'
    ELSE 'in_progress'
  END AS transaction_state_category,

  -- Response Code
  response_code,
  CASE
    WHEN response_code = '00' THEN 'approved'
    WHEN response_code = '05' THEN 'do_not_honor'
    WHEN response_code = '51' THEN 'insufficient_funds'
    WHEN response_code = '54' THEN 'expired_card'
    WHEN response_code = '61' THEN 'exceeds_limit'
    WHEN response_code = '65' THEN 'activity_limit'
    ELSE 'other'
  END AS response_code_description,

  -- 3D Secure
  lower(three_ds) AS three_ds_status,
  CASE
    WHEN three_ds IN ('frictionless', 'challenge') THEN true
    ELSE false
  END AS is_3ds_authenticated,

  -- Fees
  fees_total_cents,
  round(fees_total_cents / 100.0, 2) AS fees_total_amount,
  network_fee_cents,
  round(network_fee_cents / 100.0, 2) AS network_fee_amount,

  -- Calculated Metrics
  round((fees_total_cents / amount_cents) * 100, 2) AS effective_fee_rate_pct,
  amount_cents - fees_total_cents AS net_amount_cents,
  round((amount_cents - fees_total_cents) / 100.0, 2) AS net_amount,

  -- Processor
  lower(processor_name) AS processor_name,

  -- Derived Flags
  CASE
    WHEN state_name IN ('completed', 'closed') THEN true
    ELSE false
  END AS is_successful_transaction,
  CASE
    WHEN state_name IN ('declined', 'failed') THEN true
    ELSE false
  END AS is_failed_transaction,
  CASE
    WHEN state_name IN ('disputed', 'under_review', 'chargeback') THEN true
    ELSE false
  END AS is_disputed_transaction,
  CASE
    WHEN response_code != '00' THEN true
    ELSE false
  END AS is_declined,

  -- Timestamps
  authorized_at AS transaction_authorized_at,
  date(authorized_at) AS transaction_date,
  hour(authorized_at) AS transaction_hour,
  dayofweek(authorized_at) AS transaction_day_of_week,

  -- Metadata
  ingestion_timestamp,
  current_timestamp() AS silver_processed_at

FROM STREAM(LIVE.bronze_transactions);
