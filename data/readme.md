# PSP Sample Data Files

This directory contains synthetic Payment Service Provider (PSP) data files ready for upload to Databricks.

## Files Generated

All files are in JSONL format (JSON Lines) with approximately 4 MB each:

| File | Size | Records | Description |
|------|------|---------|-------------|
| [merchants.jsonl](merchants.jsonl) | 4.0 MB | 19,700 | Merchant accounts with KYB status, risk levels, pricing tiers |
| [customers.jsonl](customers.jsonl) | 4.0 MB | 24,000 | Customer profiles with hashed PII and customer types |
| [payments.jsonl](payments.jsonl) | 4.0 MB | 17,400 | Payment instruments (cards) with brand, BIN, wallet types |
| [orders.jsonl](orders.jsonl) | 4.0 MB | 16,100 | Order transactions with financial breakdowns |
| [transactions.jsonl](transactions.jsonl) | 4.0 MB | 11,200 | Payment transactions with state machines and fee calculations |
| [payouts.jsonl](payouts.jsonl) | 4.0 MB | 15,300 | Merchant payouts with settlements and reserves |
| [disputes.jsonl](disputes.jsonl) | 4.0 MB | 17,200 | Chargebacks and disputes with reason codes |

**Total Size:** 28 MB

## Schema Compliance

All files match the exact schema defined in [gen/psp.json](../gen/psp.json):

- **Primary Keys:** Follow ShadowTraffic bothify patterns
  - Merchants: `m_#####??##`
  - Customers: `c_#####??##`
  - Payments: `pm_####??####`
  - Orders: `ord_#####??####`
  - Transactions: `txn_#####??####`
  - Payouts: `pay_#####??####`
  - Disputes: `cb_#####??####`

- **Timestamps:** ISO8601 format (`YYYY-MM-DDTHH:mm:ssZ`)
- **Amounts:** Integer cents (e.g., `5185` = $51.85)
- **Currencies:** ISO 4217 codes (USD, GBP, CAD, AUD)

## Referential Integrity

All foreign key relationships are maintained:

```
merchants (19,700)
    ↓
    ├─→ orders (16,100) ──→ customers (24,000)
    │       ↓
    │       └─→ transactions (11,200) ──→ payments (17,400) ──→ customers
    │               ↓
    │               └─→ disputes (17,200)
    │
    └─→ payouts (15,300)
```

**Verified Relationships:**
- 17,400 payments → 24,000 customers
- 16,100 orders → 19,700 merchants & 24,000 customers
- 11,200 transactions → 16,100 orders & 17,400 payments
- 15,300 payouts → 19,700 merchants
- 17,200 disputes → 11,200 transactions

## Data Characteristics

### Realistic Distributions

- **Transaction States:** 92% authorized, 8% declined
- **Dispute Rate:** ~2% of transactions result in disputes
- **KYB Status:** 85% approved, 10% pending, 5% review/rejected
- **Payment Brands:** 45% Visa, 35% Mastercard, 10% Amex, 10% others
- **Channels:** 55% ecommerce, 30% POS, 10% mobile, 5% IVR

### Financial Accuracy

- **Fees:** Calculated using realistic rates (2.4-3.2% + $0.20-$0.35)
- **Tax:** Variable by region (5-15%)
- **Tips:** 0-25% of subtotal
- **Reserves:** 0.1-0.5% for merchant protection
- **Network Fees:** $0.08-$0.18 per transaction

### State Machines

Transactions follow realistic lifecycle paths:
```
pending → authorized → captured → settled → completed
            ↓            ↓          ↓
         declined    refund     disputed
            ↓         pending      ↓
         failed       ↓         under_review
                   refunded       ↓
                      ↓      completed/chargeback
                   closed         ↓
                               closed
```

## Usage

### Upload to Databricks

```python
# Upload to DBFS
dbutils.fs.cp("file:/path/to/merchants.jsonl", "dbfs:/mnt/bronze/psp/merchants/")
dbutils.fs.cp("file:/path/to/customers.jsonl", "dbfs:/mnt/bronze/psp/customers/")
# ... repeat for all files
```

### Read with Spark

```python
# Read merchants
df_merchants = spark.read.json("dbfs:/mnt/bronze/psp/merchants/*.jsonl")

# Read transactions with nested state
df_transactions = spark.read.json("dbfs:/mnt/bronze/psp/transactions/*.jsonl")
df_transactions = df_transactions.withColumn(
    "state_name", col("state.state_name")
).withColumn(
    "state_timestamp", (col("state.timestamp") / 1000).cast("timestamp")
)
```

### Verify Referential Integrity

```sql
-- Check orphaned transactions (should return 0)
SELECT COUNT(*)
FROM transactions t
LEFT JOIN orders o ON t.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Check orphaned disputes (should return 0)
SELECT COUNT(*)
FROM disputes d
LEFT JOIN transactions t ON d.txn_id = t.txn_id
WHERE t.txn_id IS NULL;
```

## Generation

Files were generated using [gen/gen.py](../gen/gen.py).

To regenerate:
```bash
cd gen
python3 gen.py
```

## Next Steps

1. Upload files to Databricks DBFS or external storage (Azure Blob, S3)
2. Create Bronze tables using Delta Live Tables
3. Build Silver layer with data quality checks
4. Create unified domain table at transaction grain
5. Build Gold OBT tables for analytics

Refer to the main [readme](../gen/readme.md) for medallion architecture details.
