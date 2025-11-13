-- Gold Layer: Risk & Fraud Monitoring
-- Real-time risk indicators, fraud patterns, and dispute analytics
-- Source: Silver Layer 2 unified_transactions table
-- Grain: One row per transaction with risk scoring

CREATE OR REFRESH LIVE TABLE gold_risk_fraud_monitoring
COMMENT "Transaction-level risk scoring and fraud detection indicators"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  -- Transaction Identity
  txn_id,
  transaction_date,
  transaction_authorized_at,
  order_id,

  -- Core Entities
  merchant_id,
  merchant_legal_name,
  merchant_category_code,
  merchant_country,
  merchant_risk_level,
  is_merchant_high_risk,

  customer_id,
  customer_type,
  is_vip_customer,
  is_flagged_customer,
  customer_tenure_days,

  payment_id,
  card_brand,
  card_bin,
  wallet_type,
  is_wallet_payment,

  -- Transaction Details
  transaction_amount,
  transaction_currency,
  transaction_state,
  transaction_state_category,
  response_code,
  response_code_description,
  is_successful_transaction,
  is_failed_transaction,
  is_declined,

  -- Security Indicators
  three_ds_status,
  is_3ds_authenticated,

  -- Dispute Information
  has_dispute,
  dispute_id,
  dispute_reason_code,
  dispute_category,
  dispute_stage,
  dispute_status,
  liability_party,
  is_dispute_won,
  is_dispute_lost,
  is_merchant_liable,
  is_fraud_dispute,
  is_dispute_escalated,
  dispute_severity_level,
  dispute_amount,
  dispute_opened_at,
  dispute_closed_at,
  dispute_age_days,

  -- Order Context
  order_channel,
  is_ecommerce_order,
  order_size_category,
  is_high_value_order,
  order_hour,
  order_day_of_week,

  -- Relationship Ages (Fraud Indicator)
  days_since_customer_created,
  days_since_merchant_created,
  days_since_payment_first_seen,

  -- Financial Impact
  psp_revenue AS transaction_fees,
  merchant_net_revenue,

  -- ============================================================================
  -- RISK SCORING COMPONENTS
  -- ============================================================================

  -- 1. Merchant Risk Score (0-30 points)
  CASE
    WHEN merchant_risk_level = 'critical' THEN 30
    WHEN merchant_risk_level = 'high' THEN 20
    WHEN merchant_risk_level = 'medium' THEN 10
    ELSE 0
  END +
  CASE WHEN NOT is_merchant_kyb_approved THEN 10 ELSE 0 END
  AS merchant_risk_score,

  -- 2. Customer Risk Score (0-30 points)
  CASE
    WHEN is_flagged_customer THEN 30
    WHEN customer_tenure_days < 7 THEN 20
    WHEN customer_tenure_days < 30 THEN 10
    ELSE 0
  END +
  CASE WHEN is_vip_customer THEN -10 ELSE 0 END -- VIP reduces risk
  AS customer_risk_score,

  -- 3. Transaction Pattern Risk Score (0-40 points)
  CASE
    WHEN transaction_amount > 1000 THEN 15
    WHEN transaction_amount > 500 THEN 10
    WHEN transaction_amount > 100 THEN 5
    ELSE 0
  END +
  CASE
    WHEN NOT is_3ds_authenticated THEN 15
    ELSE 0
  END +
  CASE
    WHEN order_hour < 6 OR order_hour > 23 THEN 10 -- Late night transactions
    ELSE 0
  END +
  CASE
    WHEN days_since_payment_first_seen < 1 THEN 10 -- New payment method
    ELSE 0
  END
  AS transaction_pattern_risk_score,

  -- ============================================================================
  -- TOTAL RISK SCORE (0-100)
  -- ============================================================================
  LEAST(
    CASE
      WHEN merchant_risk_level = 'critical' THEN 30
      WHEN merchant_risk_level = 'high' THEN 20
      WHEN merchant_risk_level = 'medium' THEN 10
      ELSE 0
    END +
    CASE WHEN NOT is_merchant_kyb_approved THEN 10 ELSE 0 END +
    CASE
      WHEN is_flagged_customer THEN 30
      WHEN customer_tenure_days < 7 THEN 20
      WHEN customer_tenure_days < 30 THEN 10
      ELSE 0
    END +
    CASE WHEN is_vip_customer THEN -10 ELSE 0 END +
    CASE
      WHEN transaction_amount > 1000 THEN 15
      WHEN transaction_amount > 500 THEN 10
      WHEN transaction_amount > 100 THEN 5
      ELSE 0
    END +
    CASE WHEN NOT is_3ds_authenticated THEN 15 ELSE 0 END +
    CASE WHEN order_hour < 6 OR order_hour > 23 THEN 10 ELSE 0 END +
    CASE WHEN days_since_payment_first_seen < 1 THEN 10 ELSE 0 END,
    100
  ) AS total_risk_score,

  -- ============================================================================
  -- RISK CLASSIFICATION
  -- ============================================================================
  CASE
    WHEN LEAST(
      CASE WHEN merchant_risk_level = 'critical' THEN 30 WHEN merchant_risk_level = 'high' THEN 20 WHEN merchant_risk_level = 'medium' THEN 10 ELSE 0 END +
      CASE WHEN NOT is_merchant_kyb_approved THEN 10 ELSE 0 END +
      CASE WHEN is_flagged_customer THEN 30 WHEN customer_tenure_days < 7 THEN 20 WHEN customer_tenure_days < 30 THEN 10 ELSE 0 END +
      CASE WHEN is_vip_customer THEN -10 ELSE 0 END +
      CASE WHEN transaction_amount > 1000 THEN 15 WHEN transaction_amount > 500 THEN 10 WHEN transaction_amount > 100 THEN 5 ELSE 0 END +
      CASE WHEN NOT is_3ds_authenticated THEN 15 ELSE 0 END +
      CASE WHEN order_hour < 6 OR order_hour > 23 THEN 10 ELSE 0 END +
      CASE WHEN days_since_payment_first_seen < 1 THEN 10 ELSE 0 END,
      100
    ) >= 70 THEN 'critical'
    WHEN LEAST(
      CASE WHEN merchant_risk_level = 'critical' THEN 30 WHEN merchant_risk_level = 'high' THEN 20 WHEN merchant_risk_level = 'medium' THEN 10 ELSE 0 END +
      CASE WHEN NOT is_merchant_kyb_approved THEN 10 ELSE 0 END +
      CASE WHEN is_flagged_customer THEN 30 WHEN customer_tenure_days < 7 THEN 20 WHEN customer_tenure_days < 30 THEN 10 ELSE 0 END +
      CASE WHEN is_vip_customer THEN -10 ELSE 0 END +
      CASE WHEN transaction_amount > 1000 THEN 15 WHEN transaction_amount > 500 THEN 10 WHEN transaction_amount > 100 THEN 5 ELSE 0 END +
      CASE WHEN NOT is_3ds_authenticated THEN 15 ELSE 0 END +
      CASE WHEN order_hour < 6 OR order_hour > 23 THEN 10 ELSE 0 END +
      CASE WHEN days_since_payment_first_seen < 1 THEN 10 ELSE 0 END,
      100
    ) >= 50 THEN 'high'
    WHEN LEAST(
      CASE WHEN merchant_risk_level = 'critical' THEN 30 WHEN merchant_risk_level = 'high' THEN 20 WHEN merchant_risk_level = 'medium' THEN 10 ELSE 0 END +
      CASE WHEN NOT is_merchant_kyb_approved THEN 10 ELSE 0 END +
      CASE WHEN is_flagged_customer THEN 30 WHEN customer_tenure_days < 7 THEN 20 WHEN customer_tenure_days < 30 THEN 10 ELSE 0 END +
      CASE WHEN is_vip_customer THEN -10 ELSE 0 END +
      CASE WHEN transaction_amount > 1000 THEN 15 WHEN transaction_amount > 500 THEN 10 WHEN transaction_amount > 100 THEN 5 ELSE 0 END +
      CASE WHEN NOT is_3ds_authenticated THEN 15 ELSE 0 END +
      CASE WHEN order_hour < 6 OR order_hour > 23 THEN 10 ELSE 0 END +
      CASE WHEN days_since_payment_first_seen < 1 THEN 10 ELSE 0 END,
      100
    ) >= 30 THEN 'medium'
    ELSE 'low'
  END AS risk_classification,

  -- ============================================================================
  -- FRAUD INDICATORS (Boolean Flags)
  -- ============================================================================
  CASE WHEN is_fraud_dispute THEN true ELSE false END AS confirmed_fraud,
  CASE WHEN has_dispute AND dispute_category = 'fraud_related' THEN true ELSE false END AS suspected_fraud,
  CASE WHEN is_flagged_customer AND is_declined THEN true ELSE false END AS flagged_customer_decline,
  CASE WHEN NOT is_3ds_authenticated AND transaction_amount > 200 THEN true ELSE false END AS high_value_no_auth,
  CASE WHEN days_since_payment_first_seen = 0 AND transaction_amount > 100 THEN true ELSE false END AS new_card_high_value,
  CASE WHEN days_since_customer_created < 1 AND transaction_amount > 200 THEN true ELSE false END AS new_customer_high_value,
  CASE WHEN (order_hour < 2 OR order_hour > 23) AND transaction_amount > 300 THEN true ELSE false END AS late_night_high_value,

  -- Multiple Risk Flags Count
  (
    CASE WHEN is_fraud_dispute THEN 1 ELSE 0 END +
    CASE WHEN has_dispute AND dispute_category = 'fraud_related' THEN 1 ELSE 0 END +
    CASE WHEN is_flagged_customer AND is_declined THEN 1 ELSE 0 END +
    CASE WHEN NOT is_3ds_authenticated AND transaction_amount > 200 THEN 1 ELSE 0 END +
    CASE WHEN days_since_payment_first_seen = 0 AND transaction_amount > 100 THEN 1 ELSE 0 END +
    CASE WHEN days_since_customer_created < 1 AND transaction_amount > 200 THEN 1 ELSE 0 END +
    CASE WHEN (order_hour < 2 OR order_hour > 23) AND transaction_amount > 300 THEN 1 ELSE 0 END
  ) AS fraud_indicator_count,

  -- ============================================================================
  -- ACTION RECOMMENDATION
  -- ============================================================================
  CASE
    WHEN is_fraud_dispute OR (
      CASE WHEN is_fraud_dispute THEN 1 ELSE 0 END +
      CASE WHEN has_dispute AND dispute_category = 'fraud_related' THEN 1 ELSE 0 END +
      CASE WHEN is_flagged_customer AND is_declined THEN 1 ELSE 0 END +
      CASE WHEN NOT is_3ds_authenticated AND transaction_amount > 200 THEN 1 ELSE 0 END +
      CASE WHEN days_since_payment_first_seen = 0 AND transaction_amount > 100 THEN 1 ELSE 0 END +
      CASE WHEN days_since_customer_created < 1 AND transaction_amount > 200 THEN 1 ELSE 0 END +
      CASE WHEN (order_hour < 2 OR order_hour > 23) AND transaction_amount > 300 THEN 1 ELSE 0 END
    ) >= 3 THEN 'block_merchant'
    WHEN LEAST(
      CASE WHEN merchant_risk_level = 'critical' THEN 30 WHEN merchant_risk_level = 'high' THEN 20 WHEN merchant_risk_level = 'medium' THEN 10 ELSE 0 END +
      CASE WHEN NOT is_merchant_kyb_approved THEN 10 ELSE 0 END +
      CASE WHEN is_flagged_customer THEN 30 WHEN customer_tenure_days < 7 THEN 20 WHEN customer_tenure_days < 30 THEN 10 ELSE 0 END +
      CASE WHEN is_vip_customer THEN -10 ELSE 0 END +
      CASE WHEN transaction_amount > 1000 THEN 15 WHEN transaction_amount > 500 THEN 10 WHEN transaction_amount > 100 THEN 5 ELSE 0 END +
      CASE WHEN NOT is_3ds_authenticated THEN 15 ELSE 0 END +
      CASE WHEN order_hour < 6 OR order_hour > 23 THEN 10 ELSE 0 END +
      CASE WHEN days_since_payment_first_seen < 1 THEN 10 ELSE 0 END,
      100
    ) >= 70 THEN 'manual_review'
    WHEN has_dispute AND NOT is_dispute_closed THEN 'monitor'
    WHEN LEAST(
      CASE WHEN merchant_risk_level = 'critical' THEN 30 WHEN merchant_risk_level = 'high' THEN 20 WHEN merchant_risk_level = 'medium' THEN 10 ELSE 0 END +
      CASE WHEN NOT is_merchant_kyb_approved THEN 10 ELSE 0 END +
      CASE WHEN is_flagged_customer THEN 30 WHEN customer_tenure_days < 7 THEN 20 WHEN customer_tenure_days < 30 THEN 10 ELSE 0 END +
      CASE WHEN is_vip_customer THEN -10 ELSE 0 END +
      CASE WHEN transaction_amount > 1000 THEN 15 WHEN transaction_amount > 500 THEN 10 WHEN transaction_amount > 100 THEN 5 ELSE 0 END +
      CASE WHEN NOT is_3ds_authenticated THEN 15 ELSE 0 END +
      CASE WHEN order_hour < 6 OR order_hour > 23 THEN 10 ELSE 0 END +
      CASE WHEN days_since_payment_first_seen < 1 THEN 10 ELSE 0 END,
      100
    ) >= 50 THEN 'enhanced_monitoring'
    ELSE 'normal'
  END AS recommended_action,

  -- Metadata
  current_timestamp() AS gold_created_at

FROM LIVE.silver_unified_transactions;
