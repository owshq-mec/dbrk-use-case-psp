# PSP Synthetic Data Generator

## Execution
```
docker run \
  --env-file key.env \
  -v $(pwd)/psp.json:/home/config.json \
  shadowtraffic/shadowtraffic:latest \
  --config /home/config.json
```

## Overview

This ShadowTraffic-based data generator creates **realistic Payment Service Provider (PSP) data** that accurately models the complete payment lifecycle. The generated data includes proper referential integrity, production-like failure rates, and realistic timing patterns that make it ideal for demonstrating data engineering patterns in the financial services domain.

### What This Generator Creates

The system generates **7 interconnected data tables** representing the complete PSP ecosystem:

| Table | Purpose | Domain |
|-------|---------|--------|
| **merchants** | Business entities accepting payments | Merchant Management |
| **customers** | End users making purchases | Customer Management |
| **payments** | Tokenized cards and digital wallets | Payment Methods |
| **orders** | Purchase transactions with amounts | Commerce |
| **transactions** | Payment lifecycle state machine | Payment Processing |
| **payouts** | Settlement batches to merchants | Financial Settlement |
| **disputes** | Chargebacks and contested payments | Risk & Compliance |

---

## Architecture & Design

### Entity Relationship Model

The data model follows industry-standard PSP architecture with clear domain boundaries:

```
┌─────────────────────────────────────────────────────────┐
│                    MERCHANT DOMAIN                      │
│                                                         │
│  ┌──────────┐                    ┌──────────┐         │
│  │MERCHANTS │───────────────────▶│ PAYOUTS  │         │
│  └────┬─────┘    1:N (daily)     └──────────┘         │
│       │                                                 │
└───────┼─────────────────────────────────────────────────┘
        │
        │ 1:N
        │
        ▼
┌───────────────────────────────────────────────────────────┐
│                   COMMERCE DOMAIN                         │
│                                                           │
│  ┌────────┐        ┌─────────────┐        ┌──────────┐  │
│  │ORDERS  │───────▶│TRANSACTIONS │───────▶│ DISPUTES │  │
│  └────┬───┘  1:1   └──────┬──────┘  1:0.1 └──────────┘  │
│       │                   │                               │
└───────┼───────────────────┼───────────────────────────────┘
        │                   │
        │ N:1               │ N:1
        │                   │
┌───────┴───────────────────┴───────────────────────────────┐
│                  CUSTOMER DOMAIN                          │
│                                                           │
│  ┌──────────┐             ┌──────────┐                   │
│  │CUSTOMERS │────────────▶│ PAYMENTS │                   │
│  └──────────┘    1:N      └──────────┘                   │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

### Data Flow Lifecycle

**1. Merchant Onboarding**
- Merchants register with business details
- KYB (Know Your Business) verification
- Pricing tier assignment based on risk

**2. Customer & Payment Setup**
- Customers create accounts (hashed PII)
- Payment methods tokenized (cards, wallets)
- Multiple payment methods per customer

**3. Order Creation**
- Customer initiates purchase at merchant
- Order includes amounts, tax, tips, channel
- Links merchant and customer

**4. Transaction Processing** (State Machine)
```
pending → authorized → captured → settled → completed
   ↓         ↓           ↓           ↓
declined   void    refund_pending  disputed
```

**5. Settlement & Payout**
- Daily batch processing
- Fee calculation and deduction
- Reserve withholding
- Net payout to merchant

**6. Dispute Resolution**
- Chargebacks from customers
- Fraud investigation
- Win/loss determination

---

## Realistic Data Characteristics

### Production-Like Behavior

#### Success & Failure Rates

| Metric | Rate | Purpose |
|--------|------|---------|
| Authorization Success | 92% | Realistic approval patterns |
| Authorization Decline | 8% | Simulates fraud, insufficient funds, expired cards |
| Capture Success | 95% | Most authorized payments captured |
| Void Rate | 5% | Merchant cancellations |
| Refund Rate | 3% | Customer returns |
| Dispute Rate | 2% | Chargebacks and fraud claims |
| Payout Success | 85% | Most settlements succeed |

#### Transaction State Machine

The transaction generator implements a **realistic payment lifecycle state machine**:

- **pending** → Initial authorization request
- **authorized** → Bank approves (92% of attempts)
- **declined** → Bank rejects (8% of attempts)
- **captured** → Funds reserved for settlement
- **settled** → Daily batch processed
- **completed** → Final successful state
- **disputed** → Customer initiates chargeback
- **refunded** → Merchant processes return

**Realistic Timing:**
- Authorization: 0.5-3 seconds
- Capture: 1-5 seconds
- Settlement: 12-24 hours (overnight batch)
- Refunds: 1-3 days
- Disputes: 7-60 days (late-arriving data)

### Financial Accuracy

#### Fee Structures

**Transaction Fees:**
- Base percentage: 2.4% - 3.2%
- Fixed per-transaction: $0.20 - $0.35
- Network interchange: $0.08 - $0.18

**Payout Calculations:**
- Gross = Sum of settled transactions
- Fees = Transaction fees + network fees
- Reserves = 0.1% - 0.5% (fraud protection)
- Net = Gross - Fees - Reserves

#### Amount Distributions

**Order Values:**
- Subtotal: $5 - $250
- Tax: 5% - 15% of subtotal
- Tips: 0% - 25% of subtotal (weighted toward 15-20%)
- Total = Subtotal + Tax + Tip

### Geographic & Channel Distribution

**Currencies:**
- USD: 70%
- GBP: 15%
- CAD: 10%
- AUD: 5%

**Payment Channels:**
- E-commerce: 55%
- Point-of-Sale: 30%
- Mobile: 10%
- IVR/Phone: 5%

**Card Brands:**
- Visa: 45%
- Mastercard: 35%
- Amex: 10%
- Discover: 7%
- Diners: 3%

**Digital Wallets:**
- No wallet: 50%
- Apple Pay: 25%
- Google Pay: 20%
- Samsung Pay: 5%

### Security & Compliance

**3D Secure Authentication:**
- Frictionless: 65%
- Challenge: 20%
- Attempted: 10%
- Not Supported: 5%

**Risk Levels (Merchants):**
- Low: 70%
- Medium: 20%
- High: 8%
- Critical: 2%

---

## Data Quality Features

### Intentional Edge Cases

The generator creates **realistic data quality issues** to simulate production environments:

#### 1. Late-Arriving Data
- Disputes arrive 7-60 days after transaction settlement
- Tests late-join scenarios and SCD patterns
- Requires temporal data handling

#### 2. Out-of-Order Events
- Transaction state updates may arrive non-sequentially
- Tests event-time vs processing-time handling
- Requires watermarking strategies

#### 3. Orphaned Records (~1%)
- Some lookups intentionally fail
- Simulates data consistency issues
- Tests data quality rules and expectations

#### 4. Partial Data
- Open disputes with null `closed_at`
- Pending payouts without completion dates
- Tests handling of incomplete records

#### 5. Duplicate Scenarios
- Retry logic creates multiple transaction attempts per order
- Tests deduplication strategies
- Represents real payment retry patterns

#### 6. Settlement Delays
- Payouts occur 1-2 days after batch day
- 10% pending, 3% in-transit, 2% failed
- Tests reconciliation logic

---

## Output Structure

### Storage Format

**Technology:** Azure Blob Storage
**Format:** JSON Lines (JSONL)
**Container:** `payment-service-provider`

**Batch Configuration:**
- **Linger Time:** 5 seconds (groups events)
- **Max Elements:** 1,000 records per file
- **Max Size:** 5MB per file

### Folder Organization

```
payment-service-provider/
│
├── merchants/
│   └── part-00001.jsonl
│   └── part-00002.jsonl
│   └── ...
│
├── customers/
│   └── part-00001.jsonl
│   └── ...
│
├── payments/
│   └── part-00001.jsonl
│   └── ...
│
├── orders/
│   └── part-00001.jsonl
│   └── ...
│
├── transactions/
│   └── part-00001.jsonl
│   └── ...
│
├── payouts/
│   └── part-00001.jsonl
│   └── ...
│
└── disputes/
    └── part-00001.jsonl
    └── ...
```

### Record Distribution

For **10,000 orders** baseline:

| Entity | Count | Ratio | Notes |
|--------|-------|-------|-------|
| Merchants | ~500 | 1:20 | Each merchant has ~20 orders |
| Customers | ~8,000 | 1:1.25 | Some repeat customers |
| Payments | ~12,000 | 1.5:1 | Average 1.5 cards per customer |
| Orders | 10,000 | baseline | Starting point |
| Transactions | ~10,500 | 1.05:1 | Includes retries (5%) |
| Successful Txns | ~9,200 | 92% | Authorization success rate |
| Settled Txns | ~8,700 | 95% | Capture success rate |
| Payouts | ~100 | 1% | Daily batches per merchant |
| Disputes | ~200 | 2% | Chargeback rate |

**Total Records:** ~41,300

---

## Medallion Architecture Integration

### Bronze Layer (Raw Ingestion)

The generator writes directly to Bronze-ready format:

- **merchants_bronze** ← `merchants/` folder
- **customers_bronze** ← `customers/` folder
- **payments_bronze** ← `payments/` folder
- **orders_bronze** ← `orders/` folder
- **transactions_bronze** ← `transactions/` folder
- **payouts_bronze** ← `payouts/` folder
- **disputes_bronze** ← `disputes/` folder

**Ingestion Method:** Auto Loader (schema inference enabled)

### Silver Layer (Conformed & Cleansed)

**Transformations Required:**
- Column renaming to standard conventions
- Type casting (cents → decimals)
- Timestamp parsing and timezone handling
- Data quality expectations
- Deduplication on primary keys
- Referential integrity validation

**Silver Tables:**
- merchants_silver
- customers_silver
- payments_silver
- orders_silver
- transactions_silver
- payouts_silver
- disputes_silver

### Unified Domain Table

**domain_transactions_silver** - Transaction-grain fact table

**Purpose:** Single source of truth joining all domains

**Grain:** One row per transaction

**Joins:**
- transactions (base)
- ↔ orders
- ↔ customers
- ↔ payments
- ↔ merchants

### Gold Layer (Analytics)

**Recommended OBT (One Big Table) Designs:**

**1. merchant_performance_gold**
- Merchant-centric metrics
- Revenue, volume, transaction counts
- Success rates by channel
- Fee analysis

**2. payment_analytics_gold**
- Payment method performance
- Card brand success rates
- Wallet adoption trends
- 3DS authentication patterns

**3. settlement_reconciliation_gold**
- Daily settlement tracking
- Fee and reserve calculations
- Payout timing analysis
- Outstanding balances

**4. risk_and_disputes_gold**
- Chargeback monitoring
- Fraud indicators
- Dispute win/loss rates
- Risk scoring trends

---

## Use Cases

### Demo & Training
- PSP payment lifecycle education
- Medallion architecture workshops
- Data quality patterns demonstration
- Real-time streaming examples

### Development & Testing
- Bronze/Silver/Gold pipeline development
- Data quality rule testing
- Late-arriving data handling
- Reconciliation logic validation

### Performance & Scale
- Load testing data pipelines
- Benchmarking transformation performance
- Testing incremental processing
- Validating SLA adherence

---

## Key Differentiators

### vs Basic Synthetic Data

| Feature | Basic Generators | This Generator |
|---------|------------------|----------------|
| **Relationships** | Random IDs | Proper FK lookups |
| **State Machines** | Static values | Multi-step lifecycle |
| **Timing** | Instant | Hours/days delays |
| **Failures** | All success | 8% decline rate |
| **Calculations** | Random amounts | Accurate fee math |
| **Edge Cases** | Clean data | Production-like issues |
| **Disputes** | Missing | 2% chargeback rate |
| **Reconciliation** | No settlement | Daily batch processing |

### Real-World Accuracy

✅ **Transaction Lifecycle** - Full state machine from authorization through settlement
✅ **Financial Math** - Accurate fee, tax, and tip calculations
✅ **Referential Integrity** - All foreign keys properly maintained
✅ **Temporal Patterns** - Realistic timing delays (seconds to months)
✅ **Failure Scenarios** - Declines, voids, refunds, disputes at real rates
✅ **Data Quality Issues** - Late-arriving, out-of-order, orphaned records
✅ **Settlement Complexity** - Batch processing, reserves, reconciliation

---

## Getting Started

### Prerequisites

1. **Docker** - Container runtime
2. **Azure Storage** - Blob storage account
3. **Configuration** - Connection string in `key.env`
