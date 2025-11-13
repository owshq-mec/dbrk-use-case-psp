"""
Silver Layer 2: Unified Transactions
====================================

Single denormalized table at transaction grain combining all PSP entities.
This is the "single source of truth" for downstream analytics and gold tables.

Grain: One row per transaction
Source: Silver Layer 1 domain tables (transactions, orders, merchants, customers, payments, disputes)
Pattern: Medallion Architecture - Silver L2 (Unified/Wide)
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ==============================================================================
# DATA QUALITY CONSTRAINTS
# ==============================================================================
# Silver L2 uses DROP violation policy to ensure downstream Gold tables
# have clean, complete data for analytics

@dlt.table(
    name="silver_unified_transactions",
    comment="Unified transaction domain table at transaction grain - combines all PSP entities",
    table_properties={
        "quality": "silver",
        "layer": "silver_l2",
        "grain": "transaction",
        "pipelines.autoOptimize.zOrderCols": "txn_id,transaction_date,merchant_id,customer_id"
    }
)
@dlt.expect_or_drop("valid_txn_id", "txn_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def silver_unified_transactions():
    """
    Unified transaction table combining all PSP entities with derived business metrics.

    Join Strategy:
    - INNER JOIN: Required entities (orders, merchants, customers, payments)
    - LEFT JOIN: Optional entities (disputes - not all transactions have disputes)

    Streaming Semantics:
    - Uses STREAM() for all Silver L1 tables to maintain end-to-end streaming
    - Streaming-to-streaming joins for required entities
    - Streaming-to-batch join for disputes (infrequent updates)

    Returns:
        DataFrame: Unified transaction records at transaction grain
    """

    # ==========================================================================
    # READ SILVER L1 TABLES (Streaming)
    # ==========================================================================
    # Note: Using dlt.read_stream() to maintain streaming semantics
    # This enables incremental processing and reduces latency

    transactions = dlt.read_stream("silver_transactions").alias("t")
    orders = dlt.read_stream("silver_orders").alias("o")
    merchants = dlt.read_stream("silver_merchants").alias("m")
    customers = dlt.read_stream("silver_customers").alias("c")
    payments = dlt.read_stream("silver_payments").alias("p")

    # Disputes: Batch read (LEFT JOIN) - disputes are rare and can be batch processed
    disputes = dlt.read("silver_disputes").alias("d")

    # ==========================================================================
    # CORE JOINS - Required Entities (INNER)
    # ==========================================================================
    # All transactions MUST have order, merchant, customer, and payment

    # Step 1: Join transactions with orders
    txn_orders = transactions.join(
        orders,
        transactions.order_id == orders.order_id,
        how="inner"
    )

    # Step 2: Join with merchants
    txn_orders_merchants = txn_orders.join(
        merchants,
        F.col("o.merchant_id") == merchants.merchant_id,
        how="inner"
    )

    # Step 3: Join with customers
    txn_orders_merchants_customers = txn_orders_merchants.join(
        customers,
        F.col("o.customer_id") == customers.customer_id,
        how="inner"
    )

    # Step 4: Join with payment instruments
    txn_full = txn_orders_merchants_customers.join(
        payments,
        F.col("t.payment_id") == payments.payment_id,
        how="inner"
    )

    # ==========================================================================
    # OPTIONAL JOINS - Left Join for Disputes
    # ==========================================================================
    # Not all transactions have disputes - LEFT JOIN to preserve all transactions

    txn_complete = txn_full.join(
        disputes,
        F.col("t.txn_id") == disputes.txn_id,
        how="left"
    )

    # ==========================================================================
    # SELECT COLUMNS WITH DERIVED BUSINESS METRICS
    # ==========================================================================

    return txn_complete.select(
        # ======================================================================
        # TRANSACTION (Grain) - Base transaction attributes
        # ======================================================================
        F.col("t.txn_id"),
        F.col("t.transaction_state"),
        F.col("t.transaction_state_category"),
        F.col("t.state_timestamp"),
        F.col("t.transaction_amount"),
        F.col("t.amount_cents"),
        F.col("t.transaction_currency"),
        F.col("t.response_code"),
        F.col("t.response_code_description"),
        F.col("t.three_ds_status"),
        F.col("t.is_3ds_authenticated"),
        F.col("t.fees_total_amount"),
        F.col("t.fees_total_cents"),
        F.col("t.network_fee_amount"),
        F.col("t.network_fee_cents"),
        F.col("t.effective_fee_rate_pct"),
        F.col("t.net_amount"),
        F.col("t.net_amount_cents"),
        F.col("t.processor_name"),
        F.col("t.is_successful_transaction"),
        F.col("t.is_failed_transaction"),
        F.col("t.is_disputed_transaction"),
        F.col("t.is_declined"),
        F.col("t.transaction_authorized_at"),
        F.col("t.transaction_date"),
        F.col("t.transaction_hour"),
        F.col("t.transaction_day_of_week"),

        # ======================================================================
        # ORDER - Order-level attributes
        # ======================================================================
        F.col("o.order_id"),
        F.col("o.order_currency"),
        F.col("o.subtotal_amount"),
        F.col("o.subtotal_cents"),
        F.col("o.tax_amount"),
        F.col("o.tax_cents"),
        F.col("o.tip_amount"),
        F.col("o.tip_cents"),
        F.col("o.total_amount").alias("order_total_amount"),
        F.col("o.total_amount_cents").alias("order_total_amount_cents"),
        F.col("o.tax_rate"),
        F.col("o.tip_rate"),
        F.col("o.order_channel"),
        F.col("o.is_ecommerce_order"),
        F.col("o.has_tip"),
        F.col("o.is_high_value_order"),
        F.col("o.order_size_category"),
        F.col("o.order_created_at"),
        F.col("o.order_date"),
        F.col("o.order_hour"),
        F.col("o.order_day_of_week"),

        # ======================================================================
        # MERCHANT - Merchant-level attributes
        # ======================================================================
        F.col("m.merchant_id"),
        F.col("m.legal_name").alias("merchant_legal_name"),
        F.col("m.merchant_category_code"),
        F.col("m.country_code").alias("merchant_country"),
        F.col("m.kyb_status").alias("merchant_kyb_status"),
        F.col("m.pricing_tier").alias("merchant_pricing_tier"),
        F.col("m.risk_level").alias("merchant_risk_level"),
        F.col("m.is_kyb_approved").alias("is_merchant_kyb_approved"),
        F.col("m.is_high_risk").alias("is_merchant_high_risk"),
        F.col("m.is_enterprise").alias("is_merchant_enterprise"),
        F.col("m.merchant_created_at"),

        # ======================================================================
        # CUSTOMER - Customer-level attributes
        # ======================================================================
        F.col("c.customer_id"),
        F.col("c.email_hash").alias("customer_email_hash"),
        F.col("c.phone_hash").alias("customer_phone_hash"),
        F.col("c.customer_type"),
        F.col("c.is_vip_customer"),
        F.col("c.is_flagged_customer"),
        F.col("c.customer_tenure_days"),
        F.col("c.customer_created_at"),

        # ======================================================================
        # PAYMENT INSTRUMENT - Payment method attributes
        # ======================================================================
        F.col("p.payment_id"),
        F.col("p.card_brand"),
        F.col("p.card_bin"),
        F.col("p.card_last4_masked"),
        F.col("p.card_expiry_month"),
        F.col("p.card_expiry_year"),
        F.col("p.wallet_type"),
        F.col("p.payment_status"),
        F.col("p.is_active_payment"),
        F.col("p.is_wallet_payment"),
        F.col("p.is_expired").alias("is_payment_expired"),
        F.col("p.card_network_tier"),
        F.col("p.payment_first_seen_at"),

        # ======================================================================
        # DISPUTE - Dispute attributes (nullable via LEFT JOIN)
        # ======================================================================
        F.col("d.dispute_id"),
        F.col("d.dispute_reason_code"),
        F.col("d.dispute_stage"),
        F.col("d.dispute_category"),
        F.col("d.liability_party"),
        F.col("d.dispute_status"),
        F.col("d.dispute_amount"),
        F.col("d.dispute_amount_cents"),
        F.col("d.dispute_opened_at"),
        F.col("d.dispute_closed_at"),
        F.col("d.dispute_age_days"),
        F.col("d.is_dispute_closed"),
        F.col("d.is_dispute_won"),
        F.col("d.is_dispute_lost"),
        F.col("d.is_merchant_liable"),
        F.col("d.is_fraud_dispute"),
        F.col("d.is_escalated").alias("is_dispute_escalated"),
        F.col("d.stage_severity_level").alias("dispute_severity_level"),

        # ======================================================================
        # DERIVED BUSINESS METRICS
        # ======================================================================
        # These metrics provide additional analytical value by combining
        # attributes from multiple entities

        # Has Dispute Flag - Indicates if transaction has associated dispute
        F.when(F.col("d.dispute_id").isNotNull(), True)
         .otherwise(False)
         .alias("has_dispute"),

        # Customer Lifetime Indicators - Days since customer was created
        F.datediff(
            F.col("t.transaction_date"),
            F.col("c.customer_created_at")
        ).alias("days_since_customer_created"),

        # Merchant Relationship Age - Days since merchant was onboarded
        F.datediff(
            F.col("t.transaction_date"),
            F.col("m.merchant_created_at")
        ).alias("days_since_merchant_created"),

        # Payment Instrument Age - Days since payment method was first used
        F.datediff(
            F.col("t.transaction_date"),
            F.col("p.payment_first_seen_at")
        ).alias("days_since_payment_first_seen"),

        # Time Delta: Order to Authorization - Processing latency in seconds
        (
            F.unix_timestamp(F.col("t.transaction_authorized_at")) -
            F.unix_timestamp(F.col("o.order_created_at"))
        ).alias("order_to_auth_seconds"),

        # Revenue Metrics - Break down revenue between merchant and PSP
        F.col("t.net_amount").alias("merchant_net_revenue"),
        F.col("t.fees_total_amount").alias("psp_revenue"),
        (
            F.col("o.total_amount") - F.col("t.net_amount")
        ).alias("total_psp_fees"),

        # ======================================================================
        # METADATA
        # ======================================================================
        F.current_timestamp().alias("unified_created_at")
    )


# ==============================================================================
# USAGE NOTES
# ==============================================================================
"""
Downstream Consumption Patterns:

1. Gold Layer Aggregations:
   @dlt.table()
   def gold_merchant_kpis():
       return dlt.read("silver_unified_transactions") \\
           .groupBy("merchant_id", "transaction_date") \\
           .agg(...)

2. Real-time Fraud Monitoring:
   @dlt.table()
   def gold_risk_monitoring():
       return dlt.read_stream("silver_unified_transactions") \\
           .withColumn("risk_score", calculate_risk_udf(...))

3. Customer Analytics:
   @dlt.table()
   def gold_customer_ltv():
       return dlt.read("silver_unified_transactions") \\
           .groupBy("customer_id") \\
           .agg(...)

Performance Optimization:
- Z-ordered on (txn_id, transaction_date, merchant_id, customer_id)
- Partitioned implicitly by event time (transaction_date)
- Use Photon engine for 3-5x performance improvement
- Enable Auto Optimize for automatic file compaction

Data Quality:
- All core entities validated via INNER JOINs (referential integrity)
- Expectations enforce NOT NULL on primary/foreign keys
- Downstream consumers can rely on complete, denormalized records
"""
