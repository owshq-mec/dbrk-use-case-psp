-- Silver Layer 1: Customers
-- Cleansed and conformed customer data with data quality checks
-- Transformations: Validate hashes, standardize types, calculate tenure

CREATE OR REFRESH STREAMING LIVE TABLE silver_customers (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_email_hash EXPECT (email_hash IS NOT NULL AND email_hash LIKE 'hash_%') ON VIOLATION DROP ROW,
  CONSTRAINT valid_phone_hash EXPECT (phone_hash IS NOT NULL AND phone_hash LIKE 'hash_%') ON VIOLATION DROP ROW,
  CONSTRAINT valid_created_at EXPECT (created_at IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_type EXPECT (customer_type IN ('individual', 'business', 'vip', 'flagged')) ON VIOLATION DROP ROW
)
COMMENT "Cleaned and conformed customer profile data"
TBLPROPERTIES (
  "quality" = "silver",
  "pipelines.autoOptimize.zOrderCols" = "customer_id,customer_type"
)
AS SELECT
  -- Primary Key
  customer_id,

  -- PII (Hashed)
  lower(email_hash) AS email_hash,
  lower(phone_hash) AS phone_hash,

  -- Customer Segmentation
  lower(customer_type) AS customer_type,

  -- Derived Flags
  CASE
    WHEN customer_type = 'vip' THEN true
    ELSE false
  END AS is_vip_customer,
  CASE
    WHEN customer_type = 'flagged' THEN true
    ELSE false
  END AS is_flagged_customer,

  -- Customer Tenure (days since created)
  datediff(current_date(), date(created_at)) AS customer_tenure_days,

  -- Timestamps
  created_at AS customer_created_at,

  -- Metadata
  ingestion_timestamp,
  current_timestamp() AS silver_processed_at

FROM STREAM(LIVE.bronze_customers);
