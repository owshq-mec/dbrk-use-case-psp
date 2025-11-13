CREATE OR REFRESH STREAMING LIVE TABLE silver_orders (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_merchant_id EXPECT (merchant_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_currency EXPECT (order_currency IN ('USD', 'GBP', 'CAD', 'AUD')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amounts EXPECT (total_amount_cents > 0 AND total_amount_cents = subtotal_cents + tax_cents + tip_cents) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_created_at EXPECT (created_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_channel EXPECT (channel IN ('ecommerce', 'pos', 'mobile', 'ivr', 'api')) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and conformed order transaction data"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.zOrderCols" = "order_id,merchant_id,customer_id"
)
AS SELECT
  order_id,

  merchant_id,
  customer_id,

  currency AS order_currency,
  subtotal_cents,
  tax_cents,
  tip_cents,
  total_amount_cents,

  round(subtotal_cents / 100.0, 2) AS subtotal_amount,
  round(tax_cents / 100.0, 2) AS tax_amount,
  round(tip_cents / 100.0, 2) AS tip_amount,
  round(total_amount_cents / 100.0, 2) AS total_amount,
  round(tax_cents / subtotal_cents, 4) AS tax_rate,
  round(tip_cents / subtotal_cents, 4) AS tip_rate,
  lower(channel) AS order_channel,

  CASE
    WHEN channel = 'ecommerce' THEN true
    ELSE false
  END AS is_ecommerce_order,
  CASE
    WHEN tip_cents > 0 THEN true
    ELSE false
  END AS has_tip,
  CASE
    WHEN total_amount_cents >= 10000 THEN true
    ELSE false
  END AS is_high_value_order,

  CASE
    WHEN total_amount_cents < 2000 THEN 'small' 
    WHEN total_amount_cents < 5000 THEN 'medium'
    WHEN total_amount_cents < 10000 THEN 'large'
    ELSE 'extra_large'                          
  END AS order_size_category,

  created_at AS order_created_at,
  date(created_at) AS order_date,
  hour(created_at) AS order_hour,
  dayofweek(created_at) AS order_day_of_week,

  ingestion_timestamp,
  current_timestamp() AS silver_processed_at

FROM STREAM(LIVE.bronze_orders);
