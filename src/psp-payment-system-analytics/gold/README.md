# Gold Layer - Analytics & Business Intelligence

The Gold layer contains curated, business-ready analytics tables optimized for reporting, dashboards, and data science workloads.

## Architecture

```
Silver Layer 1 (Domain Tables)
       ↓
   Gold Table 1: merchant_performance_kpis
       (Daily aggregations)

Silver Layer 2 (Unified)
       ↓
   ├─→ Gold Table 2: customer_analytics_kpis
   │   (Customer-grain analytics)
   │
   └─→ Gold Table 3: risk_fraud_monitoring
       (Transaction-grain risk scoring)
```

## Tables

| Table | Source | Grain | Records | Description |
|-------|--------|-------|---------|-------------|
| **gold_merchant_performance_kpis** | Silver L1 | Merchant + Date | ~2-3K | Daily performance metrics |
| **gold_customer_analytics_kpis** | Unified | Customer | ~24K | Lifetime value & behavior |
| **gold_risk_fraud_monitoring** | Unified | Transaction | ~11.2K | Real-time risk scoring |

---

## 1. Merchant Performance KPIs

**File:** [merchant_performance_kpis.sql](merchant_performance_kpis.sql)

**Source:** Silver Layer 1 domain tables (merchants + orders + transactions + payouts)

**Grain:** One row per merchant per day

**Use Cases:**
- Executive dashboards
- Merchant onboarding analysis
- Revenue forecasting
- Payment success rate monitoring
- Settlement reconciliation

### Key Metrics

#### Volume Metrics
- `daily_transaction_count` - Total transactions
- `daily_order_count` - Total orders
- `daily_unique_customers` - Distinct customers
- `unique_payment_instruments` - Distinct cards used

#### Success Metrics
- `successful_transactions` - Count
- `failed_transactions` - Count
- `declined_transactions` - Count
- `success_rate_pct` - % successful (target: >95%)
- `decline_rate_pct` - % declined (target: <5%)

#### Revenue Metrics
- `daily_gross_revenue` - Total order value
- `avg_order_value` - Average ticket size
- `daily_total_fees` - PSP fees collected
- `daily_net_revenue` - Merchant net after fees
- `net_margin_pct` - Net as % of gross

#### Channel Distribution
- `ecommerce_transactions` - Online orders
- `pos_transactions` - Point of sale
- `mobile_transactions` - Mobile app
- `ecommerce_pct` - % of transactions online

#### Payout Metrics
- `daily_payout_count` - Settlements issued
- `daily_payout_gross` - Total payout value
- `daily_payout_net` - Net after reserves
- `avg_settlement_delay_days` - Days to settlement

#### Derived Indicators
- `performance_rating` - excellent / good / fair / poor
- `revenue_tier` - high / medium / low / minimal

### Example Queries

**Top merchants by revenue:**
```sql
SELECT
  merchant_legal_name,
  SUM(daily_gross_revenue) as total_revenue,
  AVG(success_rate_pct) as avg_success_rate,
  AVG(avg_order_value) as avg_ticket
FROM gold_merchant_performance_kpis
WHERE transaction_date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY merchant_legal_name
ORDER BY total_revenue DESC
LIMIT 10;
```

**Daily performance trends:**
```sql
SELECT
  transaction_date,
  SUM(daily_transaction_count) as total_transactions,
  AVG(success_rate_pct) as platform_success_rate,
  SUM(daily_gross_revenue) as platform_revenue
FROM gold_merchant_performance_kpis
WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE)
GROUP BY transaction_date
ORDER BY transaction_date;
```

**Merchant segmentation:**
```sql
SELECT
  merchant_risk_level,
  performance_rating,
  COUNT(DISTINCT merchant_id) as merchant_count,
  SUM(daily_gross_revenue) as segment_revenue
FROM gold_merchant_performance_kpis
WHERE transaction_date = CURRENT_DATE
GROUP BY merchant_risk_level, performance_rating;
```

---

## 2. Customer Analytics KPIs

**File:** [customer_analytics_kpis.sql](customer_analytics_kpis.sql)

**Source:** Silver Layer 2 unified_transactions

**Grain:** One row per customer

**Use Cases:**
- Customer lifetime value (CLV) analysis
- Segmentation & targeting
- Churn prediction
- Loyalty program design
- Marketing campaign optimization

### Key Metrics

#### Lifetime Metrics
- `lifetime_transaction_count` - Total transactions
- `lifetime_order_count` - Total orders
- `unique_merchants_count` - Merchants shopped
- `unique_payment_methods` - Cards on file

#### Lifetime Value
- `lifetime_gross_value` - Total spent (all txns)
- `lifetime_net_value` - Total spent (successful only)
- `avg_transaction_amount` - Average ticket
- `lifetime_tips_given` - Total tips

#### Behavior Patterns
- `customer_success_rate_pct` - Transaction success %
- `avg_tip_rate` - Tipping generosity
- `tip_frequency_pct` - % of orders with tips
- `wallet_usage_pct` - % using digital wallets

#### Channel Preferences
- `ecommerce_orders` - Online transactions
- `pos_orders` - In-store transactions
- `mobile_orders` - Mobile app
- `most_recent_channel` - Last used channel

#### Temporal Analysis
- `first_transaction_date` - First purchase
- `last_transaction_date` - Last purchase
- `customer_active_days` - Days between first & last
- `days_since_last_transaction` - Recency
- `avg_transactions_per_day` - Frequency

#### Recent Activity
- `transactions_last_30d` - Recent volume
- `spend_last_30d` - Recent spend
- `transactions_last_90d` - Quarterly volume
- `spend_last_90d` - Quarterly spend

#### Risk Indicators
- `total_disputes` - Dispute count
- `fraud_disputes` - Fraud-related disputes
- `total_disputed_amount` - $ in dispute

### Segmentation

#### Value Segments
- `whale` - Lifetime value >= $10,000
- `high_value` - >= $5,000
- `medium_value` - >= $1,000
- `low_value` - >= $100
- `minimal` - < $100

#### Lifecycle Stages
- `active` - Transacted in last 7 days
- `engaged` - Last 30 days
- `at_risk` - Last 90 days
- `churned` - > 90 days inactive

#### Frequency Segments
- `very_frequent` - >= 1 txn/day
- `frequent` - >= 0.1 txn/day (every 10 days)
- `occasional` - >= 0.01 txn/day (every 100 days)
- `rare` - < 0.01 txn/day

#### Risk Profiles
- `flagged` - Explicitly flagged
- `fraud_history` - Has fraud disputes
- `high_decline_rate` - Success rate < 80%
- `dispute_prone` - > 3 disputes
- `normal` - Clean history

#### Health Score (0-100)
Composite score from:
- Success rate (30% weight)
- Frequency (20%)
- Recency (30%)
- Transaction count (20%)

### Example Queries

**High-value at-risk customers:**
```sql
SELECT
  customer_id,
  lifetime_net_value,
  days_since_last_transaction,
  customer_health_score
FROM gold_customer_analytics_kpis
WHERE value_segment IN ('whale', 'high_value')
  AND lifecycle_stage = 'at_risk'
ORDER BY lifetime_net_value DESC;
```

**Churn prediction cohort:**
```sql
SELECT
  lifecycle_stage,
  COUNT(*) as customer_count,
  AVG(lifetime_net_value) as avg_ltv,
  AVG(customer_health_score) as avg_health
FROM gold_customer_analytics_kpis
GROUP BY lifecycle_stage;
```

**Payment method preferences:**
```sql
SELECT
  value_segment,
  AVG(wallet_usage_pct) as avg_wallet_adoption,
  SUM(visa_transactions) / SUM(lifetime_transaction_count) as visa_pct,
  SUM(amex_transactions) / SUM(lifetime_transaction_count) as amex_pct
FROM gold_customer_analytics_kpis
GROUP BY value_segment;
```

---

## 3. Risk & Fraud Monitoring

**File:** [risk_fraud_monitoring.sql](risk_fraud_monitoring.sql)

**Source:** Silver Layer 2 unified_transactions

**Grain:** One row per transaction (real-time scoring)

**Use Cases:**
- Real-time fraud detection
- Transaction approval workflows
- Merchant risk assessment
- Chargeback prevention
- Compliance reporting

### Risk Scoring Model

**Total Risk Score (0-100)** = Sum of three components:

#### 1. Merchant Risk Score (0-30 points)
- Critical risk level: 30 pts
- High risk level: 20 pts
- Medium risk level: 10 pts
- KYB not approved: +10 pts

#### 2. Customer Risk Score (0-30 points)
- Flagged customer: 30 pts
- New customer (<7 days): 20 pts
- Recent customer (<30 days): 10 pts
- VIP customer: -10 pts (bonus)

#### 3. Transaction Pattern Risk (0-40 points)
- Amount > $1,000: 15 pts
- Amount > $500: 10 pts
- Amount > $100: 5 pts
- No 3DS authentication: 15 pts
- Late night (2am-6am): 10 pts
- New payment method (<1 day): 10 pts

### Risk Classification

- **Critical (70-100)** - Immediate review required
- **High (50-69)** - Enhanced monitoring
- **Medium (30-49)** - Standard monitoring
- **Low (0-29)** - Normal processing

### Fraud Indicators (Boolean Flags)

- `confirmed_fraud` - Has fraud dispute
- `suspected_fraud` - Dispute with fraud reason
- `flagged_customer_decline` - Flagged customer + decline
- `high_value_no_auth` - >$200 without 3DS
- `new_card_high_value` - New card + >$100
- `new_customer_high_value` - New customer + >$200
- `late_night_high_value` - 2am-6am + >$300
- `fraud_indicator_count` - Total flags triggered

### Recommended Actions

- **block_merchant** - Confirmed fraud OR 3+ fraud indicators
- **manual_review** - Risk score >= 70
- **monitor** - Open dispute
- **enhanced_monitoring** - Risk score >= 50
- **normal** - Risk score < 50

### Example Queries

**High-risk transactions requiring review:**
```sql
SELECT
  txn_id,
  merchant_legal_name,
  customer_id,
  transaction_amount,
  total_risk_score,
  risk_classification,
  fraud_indicator_count,
  recommended_action
FROM gold_risk_fraud_monitoring
WHERE recommended_action IN ('block_merchant', 'manual_review')
  AND transaction_date >= CURRENT_DATE
ORDER BY total_risk_score DESC;
```

**Fraud pattern analysis:**
```sql
SELECT
  merchant_category_code,
  COUNT(*) as transaction_count,
  SUM(CASE WHEN confirmed_fraud THEN 1 ELSE 0 END) as confirmed_frauds,
  AVG(total_risk_score) as avg_risk_score,
  SUM(transaction_amount) as total_at_risk
FROM gold_risk_fraud_monitoring
WHERE risk_classification IN ('critical', 'high')
GROUP BY merchant_category_code
ORDER BY confirmed_frauds DESC;
```

**Daily risk dashboard:**
```sql
SELECT
  transaction_date,
  risk_classification,
  COUNT(*) as transaction_count,
  SUM(transaction_amount) as volume,
  AVG(total_risk_score) as avg_score
FROM gold_risk_fraud_monitoring
WHERE transaction_date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY transaction_date, risk_classification
ORDER BY transaction_date, risk_classification;
```

**Dispute correlation analysis:**
```sql
SELECT
  CASE WHEN has_dispute THEN 'Has Dispute' ELSE 'No Dispute' END as dispute_status,
  risk_classification,
  COUNT(*) as transaction_count,
  AVG(total_risk_score) as avg_risk_score,
  SUM(CASE WHEN is_fraud_dispute THEN 1 ELSE 0 END) as fraud_disputes
FROM gold_risk_fraud_monitoring
GROUP BY CASE WHEN has_dispute THEN 'Has Dispute' ELSE 'No Dispute' END, risk_classification
ORDER BY avg_risk_score DESC;
```

---

## Performance Optimization

### Materialization Strategy

All Gold tables are **LIVE TABLES** (non-streaming):
- Fully materialized views
- Refresh on pipeline updates
- Optimized for query performance
- No incremental processing needed

### Query Optimization

**Merchant Performance:**
```sql
-- Pre-aggregated by merchant + date
-- Optimized for time-series analysis
-- Supports fast date range queries
```

**Customer Analytics:**
```sql
-- Aggregated by customer
-- Small record count (~24K)
-- Fast full table scans
-- Ideal for segmentation
```

**Risk Monitoring:**
```sql
-- Transaction grain (~11.2K)
-- Real-time scoring
-- Supports point lookups by txn_id
-- Filtered queries by risk_classification
```

### Indexing & Partitioning

**Consider Z-Ordering:**
```sql
-- Merchant Performance
OPTIMIZE gold_merchant_performance_kpis ZORDER BY (merchant_id, transaction_date);

-- Customer Analytics
OPTIMIZE gold_customer_analytics_kpis ZORDER BY (customer_id, value_segment);

-- Risk Monitoring
OPTIMIZE gold_risk_fraud_monitoring ZORDER BY (txn_id, risk_classification, transaction_date);
```

**Optional Partitioning:**
```sql
-- For large volumes, partition by date
PARTITIONED BY (transaction_date)
```

---

## Deployment

### Pipeline Configuration

```json
{
  "name": "psp_gold_pipeline",
  "storage": "/pipelines/psp",
  "target": "psp_gold",
  "continuous": false,
  "libraries": [
    {"notebook": {"path": "gold/merchant_performance_kpis"}},
    {"notebook": {"path": "gold/customer_analytics_kpis"}},
    {"notebook": {"path": "gold/risk_fraud_monitoring"}}
  ]
}
```

**Note:** Gold layer uses **non-streaming** refresh since it's aggregated data.

### Validation Queries

```sql
-- Check record counts
SELECT 'merchant_performance' as table_name, COUNT(*) as records FROM gold_merchant_performance_kpis
UNION ALL
SELECT 'customer_analytics', COUNT(*) FROM gold_customer_analytics_kpis
UNION ALL
SELECT 'risk_fraud_monitoring', COUNT(*) FROM gold_risk_fraud_monitoring;

-- Expected counts:
-- merchant_performance: ~2-3K (merchant-days)
-- customer_analytics: ~24K (customers)
-- risk_fraud_monitoring: ~11.2K (transactions)

-- Verify merchant performance aggregations
SELECT
  transaction_date,
  COUNT(DISTINCT merchant_id) as merchants,
  SUM(daily_transaction_count) as total_txns
FROM gold_merchant_performance_kpis
GROUP BY transaction_date
ORDER BY transaction_date DESC
LIMIT 10;

-- Verify customer segmentation distribution
SELECT
  value_segment,
  lifecycle_stage,
  COUNT(*) as customer_count
FROM gold_customer_analytics_kpis
GROUP BY value_segment, lifecycle_stage;

-- Verify risk score distribution
SELECT
  risk_classification,
  COUNT(*) as transaction_count,
  AVG(total_risk_score) as avg_score
FROM gold_risk_fraud_monitoring
GROUP BY risk_classification;
```

---

## Business Intelligence Integration

### BI Tool Connections

**Tableau / Power BI:**
```sql
-- Use Gold tables as data sources
-- Pre-aggregated for fast rendering
-- Join-free single table queries
```

**Python / R Analytics:**
```python
# Read Gold tables directly
df_merchants = spark.read.table("psp_gold.gold_merchant_performance_kpis")
df_customers = spark.read.table("psp_gold.gold_customer_analytics_kpis")
df_risk = spark.read.table("psp_gold.gold_risk_fraud_monitoring")
```

**SQL Analytics:**
```sql
-- Gold tables support complex analytics
-- No joins to Silver/Bronze needed
-- All context pre-enriched
```

### Dashboard Examples

**Executive Dashboard:**
- Source: `gold_merchant_performance_kpis`
- Metrics: Platform revenue, success rate, merchant count
- Filters: Date range, country, risk level

**Customer Insights:**
- Source: `gold_customer_analytics_kpis`
- Metrics: CLV distribution, churn rate, segment sizes
- Filters: Value segment, lifecycle stage, tenure

**Risk Operations:**
- Source: `gold_risk_fraud_monitoring`
- Metrics: High-risk transactions, fraud rate, dispute trends
- Filters: Risk classification, merchant, date range

---

## Refresh Strategy

### Full Refresh (Recommended)

```sql
-- Gold tables refresh completely on each run
-- Ensures consistency across all metrics
-- Typical runtime: 2-5 minutes for 11K transactions
```

### Incremental Updates (Advanced)

For very large volumes (>100M transactions):
```sql
-- Partition Gold tables by date
-- Refresh only recent partitions
-- Trade-off: More complex logic
```

---

## Monitoring & Alerts

### Key Metrics to Monitor

**Data Quality:**
- Record counts match expectations
- No NULL values in key dimensions
- Aggregations sum correctly

**Business Metrics:**
- Platform success rate > 90%
- Average risk score < 40
- Customer health score > 60

**Performance:**
- Gold refresh time < 5 minutes
- Query response time < 3 seconds
- No failed pipeline runs

### Sample Alerts

```sql
-- Alert: Platform success rate drop
SELECT AVG(success_rate_pct)
FROM gold_merchant_performance_kpis
WHERE transaction_date = CURRENT_DATE
HAVING AVG(success_rate_pct) < 90;

-- Alert: Spike in high-risk transactions
SELECT COUNT(*)
FROM gold_risk_fraud_monitoring
WHERE transaction_date = CURRENT_DATE
  AND risk_classification = 'critical'
HAVING COUNT(*) > 100;

-- Alert: Customer churn spike
SELECT COUNT(*)
FROM gold_customer_analytics_kpis
WHERE lifecycle_stage = 'churned'
  AND days_since_last_transaction BETWEEN 90 AND 97
HAVING COUNT(*) > 500;
```

---

## Next Steps

1. **Deploy Pipeline:** Create Gold DLT pipeline with all 3 tables
2. **Connect BI Tools:** Configure Tableau/Power BI connections
3. **Create Dashboards:** Build executive and operational dashboards
4. **Set Up Alerts:** Configure monitoring for key metrics
5. **ML Models:** Use Gold tables for churn prediction, fraud detection
6. **Optimization:** Implement Z-ordering and partitioning as needed

The Gold layer provides production-ready analytics tables optimized for business consumption!
