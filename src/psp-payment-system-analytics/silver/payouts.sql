CREATE OR REFRESH STREAMING LIVE TABLE silver_payouts (
  CONSTRAINT valid_payout_id EXPECT (payout_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_merchant_id EXPECT (merchant_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_payout_batch_date EXPECT (payout_batch_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_payout_currency EXPECT (payout_currency IN ('USD', 'GBP', 'CAD', 'AUD')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amounts EXPECT (gross_cents > 0 AND net_cents = gross_cents - fees_cents - reserve_cents) ON VIOLATION DROP ROW,
  CONSTRAINT valid_payout_paid_at EXPECT (payout_paid_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_payout_status EXPECT (payout_status IN ('pending', 'processing', 'paid', 'failed', 'canceled', 'returned')) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and conformed merchant payout and settlement data"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.zOrderCols" = "payout_id,merchant_id,batch_day"
)
AS SELECT
  payout_id,
  merchant_id,

  batch_day AS payout_batch_date,
  currency AS payout_currency,

  gross_cents,
  fees_cents,
  reserve_cents,
  net_cents,

  round(gross_cents / 100.0, 2) AS gross_amount,
  round(fees_cents / 100.0, 2) AS fees_amount,
  round(reserve_cents / 100.0, 2) AS reserve_amount,
  round(net_cents / 100.0, 2) AS net_amount,
  round((fees_cents / gross_cents) * 100, 2) AS effective_fee_rate_pct,
  round((reserve_cents / gross_cents) * 100, 2) AS reserve_rate_pct,
  round((net_cents / gross_cents) * 100, 2) AS net_margin_pct,
  lower(status) AS payout_status,

  transaction_count AS payout_transaction_count,
  round(gross_cents / transaction_count / 100.0, 2) AS avg_transaction_amount,

  CASE
    WHEN status = 'paid' THEN true
    ELSE false
  END AS is_payout_completed,
  CASE
    WHEN status = 'failed' THEN true
    ELSE false
  END AS is_payout_failed,
  CASE
    WHEN gross_cents >= 100000 THEN true 
    ELSE false
  END AS is_large_payout,

  CASE
    WHEN gross_cents < 50000 THEN 'small' 
    WHEN gross_cents < 200000 THEN 'medium'
    WHEN gross_cents < 500000 THEN 'large'
    ELSE 'extra_large'
  END AS payout_size_category,

  paid_at AS payout_paid_at,
  datediff(date(paid_at), batch_day) AS settlement_delay_days,

  ingestion_timestamp,
  current_timestamp() AS silver_processed_at

FROM STREAM(LIVE.bronze_payouts);
