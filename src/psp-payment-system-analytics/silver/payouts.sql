-- Silver Layer 1: Payouts
-- Cleansed and conformed payout data with data quality checks
-- Transformations: Validate settlements, convert cents to dollars, calculate margins

CREATE OR REFRESH STREAMING LIVE TABLE silver_payouts (
  CONSTRAINT valid_payout_id EXPECT (payout_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_merchant_id EXPECT (merchant_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_batch_day EXPECT (batch_day IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_currency EXPECT (currency IN ('USD', 'GBP', 'CAD', 'AUD')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amounts EXPECT (gross_cents > 0 AND net_cents = gross_cents - fees_cents - reserve_cents) ON VIOLATION DROP ROW,
  CONSTRAINT valid_paid_at EXPECT (paid_at IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and conformed merchant payout and settlement data"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.zOrderCols" = "payout_id,merchant_id,batch_day"
)
AS SELECT
  -- Primary Key
  payout_id,

  -- Foreign Keys
  merchant_id,

  -- Batch Information
  batch_day AS payout_batch_date,
  currency AS payout_currency,

  -- Financial Amounts (Cents)
  gross_cents,
  fees_cents,
  reserve_cents,
  net_cents,

  -- Financial Amounts (Decimal Dollars)
  round(gross_cents / 100.0, 2) AS gross_amount,
  round(fees_cents / 100.0, 2) AS fees_amount,
  round(reserve_cents / 100.0, 2) AS reserve_amount,
  round(net_cents / 100.0, 2) AS net_amount,

  -- Calculated Rates
  round((fees_cents / gross_cents) * 100, 2) AS effective_fee_rate_pct,
  round((reserve_cents / gross_cents) * 100, 2) AS reserve_rate_pct,
  round((net_cents / gross_cents) * 100, 2) AS net_margin_pct,

  -- Payout Status
  lower(status) AS payout_status,

  -- Transaction Volume
  transaction_count AS payout_transaction_count,

  -- Average Transaction Value
  round(gross_cents / transaction_count / 100.0, 2) AS avg_transaction_amount,

  -- Derived Flags
  CASE
    WHEN status = 'paid' THEN true
    ELSE false
  END AS is_payout_completed,
  CASE
    WHEN status = 'failed' THEN true
    ELSE false
  END AS is_payout_failed,
  CASE
    WHEN gross_cents >= 100000 THEN true -- >= $1,000
    ELSE false
  END AS is_large_payout,

  -- Payout Size Classification
  CASE
    WHEN gross_cents < 50000 THEN 'small'        -- < $500
    WHEN gross_cents < 200000 THEN 'medium'      -- < $2,000
    WHEN gross_cents < 500000 THEN 'large'       -- < $5,000
    ELSE 'extra_large'                            -- >= $5,000
  END AS payout_size_category,

  -- Timestamps
  paid_at AS payout_paid_at,
  datediff(date(paid_at), batch_day) AS settlement_delay_days,

  -- Metadata
  ingestion_timestamp,
  current_timestamp() AS silver_processed_at

FROM STREAM(LIVE.bronze_payouts);
