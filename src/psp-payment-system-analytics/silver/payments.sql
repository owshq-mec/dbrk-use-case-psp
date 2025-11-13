-- Silver Layer 1: Payments
-- Cleansed and conformed payment instrument data with data quality checks
-- Transformations: Validate card data, mask PAN fragments, check expiry

CREATE OR REFRESH STREAMING LIVE TABLE silver_payments (
  CONSTRAINT valid_payment_id EXPECT (payment_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_brand EXPECT (brand IN ('visa', 'mastercard', 'amex', 'discover', 'diners')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_bin EXPECT (length(bin) = 6 AND bin RLIKE '^[0-9]{6}$') ON VIOLATION DROP ROW,
  CONSTRAINT valid_last4 EXPECT (length(last4) = 4 AND last4 RLIKE '^[0-9]{4}$') ON VIOLATION DROP ROW,
  CONSTRAINT valid_expiry_month EXPECT (expiry_month BETWEEN 1 AND 12) ON VIOLATION DROP ROW,
  CONSTRAINT valid_expiry_year EXPECT (expiry_year BETWEEN 2024 AND 2035) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and conformed payment instrument data"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.zOrderCols" = "payment_id,customer_id,brand"
)
AS SELECT
  -- Primary Key
  payment_id,

  -- Foreign Keys
  customer_id,

  -- Card Details
  lower(brand) AS card_brand,
  bin AS card_bin,
  concat('****', last4) AS card_last4_masked,
  last4 AS card_last4,
  expiry_month AS card_expiry_month,
  expiry_year AS card_expiry_year,

  -- Wallet Information
  CASE
    WHEN wallet_type IS NULL THEN 'card'
    ELSE lower(wallet_type)
  END AS wallet_type,

  -- Card Status
  lower(status) AS payment_status,

  -- Derived Flags
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

  -- Card Network Classification
  CASE
    WHEN brand IN ('visa', 'mastercard', 'discover') THEN 'general'
    WHEN brand = 'amex' THEN 'premium'
    WHEN brand = 'diners' THEN 'specialty'
  END AS card_network_tier,

  -- Timestamps
  first_seen_at AS payment_first_seen_at,

  -- Metadata
  ingestion_timestamp,
  current_timestamp() AS silver_processed_at

FROM STREAM(LIVE.bronze_payments);
