-- Silver Layer 2: Unified Transactions
-- Single denormalized table at transaction grain combining all entities
-- This is the "single source of truth" for downstream analytics and gold tables

CREATE OR REFRESH STREAMING LIVE TABLE silver_unified_transactions (
  CONSTRAINT valid_txn_id EXPECT (txn_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_merchant_id EXPECT (merchant_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Unified transaction domain table at transaction grain - combines all PSP entities"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.zOrderCols" = "txn_id,transaction_date,merchant_id,customer_id"
)
AS SELECT
  -- ============================================================================
  -- TRANSACTION (Grain)
  -- ============================================================================
  t.txn_id,
  t.transaction_state,
  t.transaction_state_category,
  t.state_timestamp,
  t.transaction_amount,
  t.amount_cents,
  t.transaction_currency,
  t.response_code,
  t.response_code_description,
  t.three_ds_status,
  t.is_3ds_authenticated,
  t.fees_total_amount,
  t.fees_total_cents,
  t.network_fee_amount,
  t.network_fee_cents,
  t.effective_fee_rate_pct,
  t.net_amount,
  t.net_amount_cents,
  t.processor_name,
  t.is_successful_transaction,
  t.is_failed_transaction,
  t.is_disputed_transaction,
  t.is_declined,
  t.transaction_authorized_at,
  t.transaction_date,
  t.transaction_hour,
  t.transaction_day_of_week,

  -- ============================================================================
  -- ORDER
  -- ============================================================================
  o.order_id,
  o.order_currency,
  o.subtotal_amount,
  o.subtotal_cents,
  o.tax_amount,
  o.tax_cents,
  o.tip_amount,
  o.tip_cents,
  o.total_amount AS order_total_amount,
  o.total_amount_cents AS order_total_amount_cents,
  o.tax_rate,
  o.tip_rate,
  o.order_channel,
  o.is_ecommerce_order,
  o.has_tip,
  o.is_high_value_order,
  o.order_size_category,
  o.order_created_at,
  o.order_date,
  o.order_hour,
  o.order_day_of_week,

  -- ============================================================================
  -- MERCHANT
  -- ============================================================================
  m.merchant_id,
  m.legal_name AS merchant_legal_name,
  m.merchant_category_code,
  m.country_code AS merchant_country,
  m.kyb_status AS merchant_kyb_status,
  m.pricing_tier AS merchant_pricing_tier,
  m.risk_level AS merchant_risk_level,
  m.is_kyb_approved AS is_merchant_kyb_approved,
  m.is_high_risk AS is_merchant_high_risk,
  m.is_enterprise AS is_merchant_enterprise,
  m.merchant_created_at,

  -- ============================================================================
  -- CUSTOMER
  -- ============================================================================
  c.customer_id,
  c.email_hash AS customer_email_hash,
  c.phone_hash AS customer_phone_hash,
  c.customer_type,
  c.is_vip_customer,
  c.is_flagged_customer,
  c.customer_tenure_days,
  c.customer_created_at,

  -- ============================================================================
  -- PAYMENT INSTRUMENT
  -- ============================================================================
  p.payment_id,
  p.card_brand,
  p.card_bin,
  p.card_last4_masked,
  p.card_expiry_month,
  p.card_expiry_year,
  p.wallet_type,
  p.payment_status,
  p.is_active_payment,
  p.is_wallet_payment,
  p.is_expired AS is_payment_expired,
  p.card_network_tier,
  p.payment_first_seen_at,

  -- ============================================================================
  -- DISPUTE (Left Join - Not all transactions have disputes)
  -- ============================================================================
  d.dispute_id,
  d.dispute_reason_code,
  d.dispute_stage,
  d.dispute_category,
  d.liability_party,
  d.dispute_status,
  d.dispute_amount,
  d.dispute_amount_cents,
  d.dispute_opened_at,
  d.dispute_closed_at,
  d.dispute_age_days,
  d.is_dispute_closed,
  d.is_dispute_won,
  d.is_dispute_lost,
  d.is_merchant_liable,
  d.is_fraud_dispute,
  d.is_escalated AS is_dispute_escalated,
  d.stage_severity_level AS dispute_severity_level,

  -- ============================================================================
  -- DERIVED BUSINESS METRICS
  -- ============================================================================

  -- Has Dispute Flag
  CASE
    WHEN d.dispute_id IS NOT NULL THEN true
    ELSE false
  END AS has_dispute,

  -- Customer Lifetime Indicators
  datediff(t.transaction_date, c.customer_created_at) AS days_since_customer_created,

  -- Merchant Relationship Age
  datediff(t.transaction_date, m.merchant_created_at) AS days_since_merchant_created,

  -- Payment Instrument Age
  datediff(t.transaction_date, p.payment_first_seen_at) AS days_since_payment_first_seen,

  -- Time Deltas
  unix_timestamp(t.transaction_authorized_at) - unix_timestamp(o.order_created_at) AS order_to_auth_seconds,

  -- Revenue Metrics
  t.net_amount AS merchant_net_revenue,
  t.fees_total_amount AS psp_revenue,
  o.total_amount - t.net_amount AS total_psp_fees,

  -- ============================================================================
  -- METADATA
  -- ============================================================================
  current_timestamp() AS unified_created_at

FROM STREAM(LIVE.silver_transactions) t

-- Core Joins (Inner - all transactions must have these)
INNER JOIN LIVE.silver_orders o
  ON t.order_id = o.order_id

INNER JOIN LIVE.silver_merchants m
  ON o.merchant_id = m.merchant_id

INNER JOIN LIVE.silver_customers c
  ON o.customer_id = c.customer_id

INNER JOIN LIVE.silver_payments p
  ON t.payment_id = p.payment_id

-- Optional Joins (Left - not all transactions have these)
LEFT JOIN LIVE.silver_disputes d
  ON t.txn_id = d.txn_id;
