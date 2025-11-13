CREATE OR REFRESH STREAMING LIVE TABLE silver_disputes (
  CONSTRAINT valid_dispute_id EXPECT (dispute_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_txn_id EXPECT (txn_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_dispute_reason_code EXPECT (dispute_reason_code IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_dispute_amount_cents EXPECT (dispute_amount_cents > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_dispute_stage EXPECT (dispute_stage IN ('inquiry', 'chargeback', 'pre_arbitration', 'arbitration')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_dispute_opened_at EXPECT (dispute_opened_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_closed_dates EXPECT (dispute_closed_at IS NULL OR dispute_closed_at >= dispute_opened_at) ON VIOLATION DROP ROW,
  CONSTRAINT valid_dispute_status EXPECT (dispute_status IN ('open', 'pending', 'won', 'lost', 'expired', 'withdrawn')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_liability EXPECT (liability_party IN ('merchant', 'issuer', 'network', 'shared')) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and conformed dispute and chargeback data"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.zOrderCols" = "dispute_id,txn_id,stage"
)
AS SELECT
  dispute_id,
  txn_id,

  upper(reason_code) AS dispute_reason_code,
  lower(stage) AS dispute_stage,
  lower(liability) AS liability_party,
  lower(status) AS dispute_status,

  CASE
    WHEN reason_code = 'FRAUD' THEN 'fraud_related'
    WHEN reason_code IN ('PRODUCT_NOT_RECEIVED', 'NOT_AS_DESCRIBED') THEN 'service_issue'
    WHEN reason_code IN ('DUPLICATE', 'CREDIT_NOT_PROCESSED') THEN 'processing_error'
    WHEN reason_code = 'SUBSCRIPTION_CANCELED' THEN 'subscription_issue'
    ELSE 'other'
  END AS dispute_category,

  amount_cents AS dispute_amount_cents,
  round(amount_cents / 100.0, 2) AS dispute_amount,
  opened_at AS dispute_opened_at,
  closed_at AS dispute_closed_at,
  CASE
    WHEN closed_at IS NOT NULL THEN datediff(date(closed_at), date(opened_at))
    ELSE datediff(current_date(), date(opened_at))
  END AS dispute_age_days,

  CASE
    WHEN closed_at IS NOT NULL THEN true
    ELSE false
  END AS is_dispute_closed,
  CASE
    WHEN status = 'won' THEN true
    ELSE false
  END AS is_dispute_won,
  CASE
    WHEN status = 'lost' THEN true
    ELSE false
  END AS is_dispute_lost,
  CASE
    WHEN liability = 'merchant' THEN true
    ELSE false
  END AS is_merchant_liable,
  CASE
    WHEN reason_code = 'FRAUD' THEN true
    ELSE false
  END AS is_fraud_dispute,
  CASE
    WHEN stage IN ('pre_arbitration', 'arbitration') THEN true
    ELSE false
  END AS is_escalated,

  CASE
    WHEN stage = 'inquiry' THEN 1
    WHEN stage = 'chargeback' THEN 2
    WHEN stage = 'pre_arbitration' THEN 3
    WHEN stage = 'arbitration' THEN 4
  END AS stage_severity_level,

  date(opened_at) AS dispute_opened_date,

  ingestion_timestamp,
  current_timestamp() AS silver_processed_at

FROM STREAM(LIVE.bronze_disputes);
