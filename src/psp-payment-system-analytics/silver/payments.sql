CREATE OR REFRESH STREAMING LIVE TABLE silver_payments (
  CONSTRAINT valid_payment_id EXPECT (payment_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_brand EXPECT (brand IN ('visa', 'mastercard', 'amex', 'discover', 'diners')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_bin EXPECT (length(bin) = 6 AND bin RLIKE '^[0-9]{6}$') ON VIOLATION DROP ROW,
  CONSTRAINT valid_last4 EXPECT (length(last4) = 4 AND last4 RLIKE '^[0-9]{4}$') ON VIOLATION DROP ROW,
  CONSTRAINT valid_expiry_month EXPECT (expiry_month BETWEEN 1 AND 12) ON VIOLATION DROP ROW,
  CONSTRAINT valid_expiry_year EXPECT (expiry_year BETWEEN 2024 AND 2035) ON VIOLATION DROP ROW,
  CONSTRAINT valid_wallet_type EXPECT (wallet_type IS NULL OR wallet_type IN ('apple_pay', 'google_pay', 'samsung_pay', 'paypal')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_payment_status EXPECT (status IN ('active', 'inactive', 'expired', 'suspended')) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and conformed payment instrument data"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.zOrderCols" = "payment_id,customer_id,brand"
)
AS SELECT
  payment_id,
  customer_id,

  lower(brand) AS card_brand,
  bin AS card_bin,
  concat('****', last4) AS card_last4_masked,
  last4 AS card_last4,
  expiry_month AS card_expiry_month,
  expiry_year AS card_expiry_year,

  CASE
    WHEN wallet_type IS NULL THEN 'card'
    ELSE lower(wallet_type)
  END AS wallet_type,

  lower(status) AS payment_status,

  CASE
    WHEN status = 'active' THEN true
    ELSE false
  END AS is_active_payment,
  CASE
    WHEN wallet_type IS NOT NULL THEN true
    ELSE false
  END AS is_wallet_payment,
  CASE
    WHEN make_date(expiry_year, expiry_month, 1) < current_date() THEN true
    ELSE false
  END AS is_expired,

  CASE
    WHEN brand IN ('visa', 'mastercard', 'discover') THEN 'general'
    WHEN brand = 'amex' THEN 'premium'
    WHEN brand = 'diners' THEN 'specialty'
  END AS card_network_tier,

  first_seen_at AS payment_first_seen_at,
  ingestion_timestamp,
  current_timestamp() AS silver_processed_at

FROM STREAM(LIVE.bronze_payments);
