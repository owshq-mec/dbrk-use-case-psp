CREATE OR REFRESH STREAMING LIVE TABLE silver_merchants (
  CONSTRAINT valid_merchant_id EXPECT (merchant_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_legal_name EXPECT (legal_name IS NOT NULL AND length(trim(legal_name)) > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_country_code EXPECT (country_code IN ('US', 'GB', 'CA', 'AU')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_mcc EXPECT (length(merchant_category_code) = 4 AND merchant_category_code RLIKE '^[0-9]{4}$') ON VIOLATION DROP ROW,
  CONSTRAINT valid_merchant_created_at EXPECT (merchant_created_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_kyb_status EXPECT (kyb_status IN ('pending', 'approved', 'rejected', 'under_review')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_pricing_tier EXPECT (pricing_tier IN ('starter', 'growth', 'enterprise', 'custom')) ON VIOLATION DROP ROW,
  CONSTRAINT valid_risk_level EXPECT (risk_level IN ('low', 'medium', 'high', 'critical')) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and conformed merchant account data"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.zOrderCols" = "merchant_id,country"
)
AS SELECT
  merchant_id,
  trim(legal_name) AS legal_name,
  mcc AS merchant_category_code,
  upper(country) AS country_code,
  lower(kyb_status) AS kyb_status,
  lower(pricing_tier) AS pricing_tier,
  lower(risk_level) AS risk_level,
  CASE
    WHEN kyb_status = 'approved' THEN true
    ELSE false
  END AS is_kyb_approved,
  CASE
    WHEN risk_level IN ('high', 'critical') THEN true
    ELSE false
  END AS is_high_risk,
  CASE
    WHEN pricing_tier = 'enterprise' THEN true
    ELSE false
  END AS is_enterprise,
  created_at AS merchant_created_at,
  ingestion_timestamp,
  current_timestamp() AS silver_processed_at
FROM STREAM(LIVE.bronze_merchants);
