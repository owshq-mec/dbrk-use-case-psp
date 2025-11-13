CREATE OR REFRESH LIVE TABLE gold_merchant_performance
COMMENT "Daily merchant performance analytics - transaction volume, revenue, fees, payouts"
TBLPROPERTIES ("quality" = "gold")
AS
WITH daily_transactions AS (
  SELECT
    m.merchant_id,
    m.legal_name AS merchant_legal_name,
    m.merchant_category_code,
    m.country_code AS merchant_country,
    m.kyb_status AS merchant_kyb_status,
    m.pricing_tier AS merchant_pricing_tier,
    m.risk_level AS merchant_risk_level,
    m.is_kyb_approved,
    m.is_high_risk,
    m.is_enterprise,
    t.transaction_date,

    COUNT(DISTINCT t.txn_id) AS daily_transaction_count,
    COUNT(DISTINCT o.order_id) AS daily_order_count,
    COUNT(DISTINCT o.customer_id) AS daily_unique_customers,

    SUM(CASE WHEN t.is_successful_transaction THEN 1 ELSE 0 END) AS successful_transactions,
    SUM(CASE WHEN t.is_failed_transaction THEN 1 ELSE 0 END) AS failed_transactions,
    SUM(CASE WHEN t.is_declined THEN 1 ELSE 0 END) AS declined_transactions,

    ROUND(
      SUM(CASE WHEN t.is_successful_transaction THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
      2
    ) AS success_rate_pct,
    ROUND(
      SUM(CASE WHEN t.is_declined THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
      2
    ) AS decline_rate_pct,

    SUM(o.total_amount) AS daily_gross_revenue,
    AVG(o.total_amount) AS avg_order_value,
    MIN(o.total_amount) AS min_order_value,
    MAX(o.total_amount) AS max_order_value,

    SUM(o.subtotal_amount) AS daily_subtotal,
    SUM(o.tax_amount) AS daily_tax,
    SUM(o.tip_amount) AS daily_tips,

    SUM(t.fees_total_amount) AS daily_total_fees,
    SUM(t.network_fee_amount) AS daily_network_fees,
    AVG(t.effective_fee_rate_pct) AS avg_fee_rate_pct,

    SUM(t.net_amount) AS daily_net_revenue,
    ROUND(
      SUM(t.net_amount) * 100.0 / NULLIF(SUM(o.total_amount), 0),
      2
    ) AS net_margin_pct,

    SUM(CASE WHEN o.is_ecommerce_order THEN 1 ELSE 0 END) AS ecommerce_transactions,
    SUM(CASE WHEN o.order_channel = 'pos' THEN 1 ELSE 0 END) AS pos_transactions,
    SUM(CASE WHEN o.order_channel = 'mobile' THEN 1 ELSE 0 END) AS mobile_transactions,

    SUM(CASE WHEN o.has_tip THEN 1 ELSE 0 END) AS orders_with_tips,
    SUM(CASE WHEN o.is_high_value_order THEN 1 ELSE 0 END) AS high_value_orders,
    AVG(o.tip_rate) AS avg_tip_rate,
    AVG(o.tax_rate) AS avg_tax_rate,

    COUNT(DISTINCT t.payment_id) AS unique_payment_instruments,

    SUM(CASE WHEN t.is_3ds_authenticated THEN 1 ELSE 0 END) AS authenticated_3ds_count,
    ROUND(
      SUM(CASE WHEN t.is_3ds_authenticated THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
      2
    ) AS authentication_rate_pct

  FROM LIVE.silver_transactions t
  INNER JOIN LIVE.silver_orders o ON t.order_id = o.order_id
  INNER JOIN LIVE.silver_merchants m ON o.merchant_id = m.merchant_id

  GROUP BY
    m.merchant_id,
    m.legal_name,
    m.merchant_category_code,
    m.country_code,
    m.kyb_status,
    m.pricing_tier,
    m.risk_level,
    m.is_kyb_approved,
    m.is_high_risk,
    m.is_enterprise,
    t.transaction_date
),

daily_payouts AS (
  SELECT
    merchant_id,
    payout_batch_date,

    COUNT(DISTINCT payout_id) AS daily_payout_count,
    SUM(gross_amount) AS daily_payout_gross,
    SUM(fees_amount) AS daily_payout_fees,
    SUM(reserve_amount) AS daily_payout_reserves,
    SUM(net_amount) AS daily_payout_net,
    SUM(payout_transaction_count) AS payout_transaction_volume,
    AVG(effective_fee_rate_pct) AS avg_payout_fee_rate_pct,

    SUM(CASE WHEN is_payout_completed THEN 1 ELSE 0 END) AS completed_payouts,
    SUM(CASE WHEN is_payout_failed THEN 1 ELSE 0 END) AS failed_payouts,
    AVG(settlement_delay_days) AS avg_settlement_delay_days

  FROM LIVE.silver_payouts
  GROUP BY merchant_id, payout_batch_date
)

SELECT
  dt.merchant_id,
  dt.merchant_legal_name,
  dt.merchant_category_code,
  dt.merchant_country,
  dt.merchant_kyb_status,
  dt.merchant_pricing_tier,
  dt.merchant_risk_level,
  dt.is_kyb_approved,
  dt.is_high_risk,
  dt.is_enterprise,

  dt.transaction_date,
  dayofweek(dt.transaction_date) AS day_of_week,
  weekofyear(dt.transaction_date) AS week_of_year,
  month(dt.transaction_date) AS month,
  quarter(dt.transaction_date) AS quarter,
  year(dt.transaction_date) AS year,

  dt.daily_transaction_count,
  dt.daily_order_count,
  dt.daily_unique_customers,

  dt.successful_transactions,
  dt.failed_transactions,
  dt.declined_transactions,
  dt.success_rate_pct,
  dt.decline_rate_pct,

  dt.daily_gross_revenue,
  dt.avg_order_value,
  dt.min_order_value,
  dt.max_order_value,
  dt.daily_subtotal,
  dt.daily_tax,
  dt.daily_tips,

  dt.daily_total_fees,
  dt.daily_network_fees,
  dt.avg_fee_rate_pct,
  dt.daily_net_revenue,
  dt.net_margin_pct,

  dt.ecommerce_transactions,
  dt.pos_transactions,
  dt.mobile_transactions,
  ROUND(dt.ecommerce_transactions * 100.0 / dt.daily_transaction_count, 2) AS ecommerce_pct,

  dt.orders_with_tips,
  dt.high_value_orders,
  dt.avg_tip_rate,
  dt.avg_tax_rate,

  dt.unique_payment_instruments,
  dt.authenticated_3ds_count,
  dt.authentication_rate_pct,

  dp.daily_payout_count,
  dp.daily_payout_gross,
  dp.daily_payout_fees,
  dp.daily_payout_reserves,
  dp.daily_payout_net,
  dp.payout_transaction_volume,
  dp.avg_payout_fee_rate_pct,
  dp.completed_payouts,
  dp.failed_payouts,
  dp.avg_settlement_delay_days,

  CASE
    WHEN dt.success_rate_pct >= 95 THEN 'excellent'
    WHEN dt.success_rate_pct >= 90 THEN 'good'
    WHEN dt.success_rate_pct >= 85 THEN 'fair'
    ELSE 'poor'
  END AS performance_rating,

  CASE
    WHEN dt.daily_gross_revenue >= 10000 THEN 'high'
    WHEN dt.daily_gross_revenue >= 5000 THEN 'medium'
    WHEN dt.daily_gross_revenue >= 1000 THEN 'low'
    ELSE 'minimal'
  END AS revenue_tier,

  current_timestamp() AS gold_created_at,
  current_date() AS snapshot_date,
  current_timestamp() AS metrics_calculated_at

FROM daily_transactions dt
LEFT JOIN daily_payouts dp
  ON dt.merchant_id = dp.merchant_id
  AND dt.transaction_date = dp.payout_batch_date;
