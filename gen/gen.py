"""
Generate sample PSP data files (3-5 MB each) with perfect referential integrity.
Matches exact schema from gen/psp.json for Databricks upload.
"""

import json
import random
import string
from datetime import datetime, timedelta
from typing import List, Dict, Any
import os

random.seed(42)

MERCHANT_START_DATE = datetime(2021, 1, 1)
MERCHANT_END_DATE = datetime(2024, 1, 1)
CUSTOMER_START_DATE = datetime(2022, 1, 1)
CUSTOMER_END_DATE = datetime(2024, 1, 1)
PAYMENT_START_DATE = datetime(2022, 1, 1)
PAYMENT_END_DATE = datetime(2024, 1, 1)
ORDER_START_DATE = datetime(2024, 1, 1)
ORDER_END_DATE = datetime.now()

merchants: List[Dict[str, Any]] = []
customers: List[Dict[str, Any]] = []
payments: List[Dict[str, Any]] = []
orders: List[Dict[str, Any]] = []
transactions: List[Dict[str, Any]] = []
payouts: List[Dict[str, Any]] = []
disputes: List[Dict[str, Any]] = []

def bothify(pattern: str) -> str:
    """Generate string matching bothify pattern (# for digit, ? for letter)"""
    result = []
    for char in pattern:
        if char == '#':
            result.append(random.choice(string.digits))
        elif char == '?':
            result.append(random.choice(string.ascii_uppercase))
        else:
            result.append(char)
    return ''.join(result)

def random_date(start: datetime, end: datetime) -> str:
    """Generate random ISO8601 datetime string"""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    dt = start + timedelta(seconds=random_seconds)
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def random_company_name() -> str:
    """Generate random company name"""
    prefixes = ["Global", "United", "Premier", "First", "Elite", "Prime", "Royal", "Grand", "Superior"]
    middles = ["Tech", "Food", "Retail", "Services", "Solutions", "Systems", "Industries", "Group", "Partners"]
    suffixes = ["Inc", "Corp", "LLC", "Ltd", "Group", "Co"]
    return f"{random.choice(prefixes)} {random.choice(middles)} {random.choice(suffixes)}"

def weighted_choice(choices: List[tuple]) -> Any:
    """Select from weighted choices [(weight, value), ...]"""
    weights, values = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]

def generate_merchants(target_size_mb: float = 4.0) -> None:
    """Generate merchants.jsonl"""
    print("Generating merchants...")

    mcc_codes = ["5812", "5411", "5541", "7011", "5999", "5735", "5651", "5942", "5311", "5912"]
    country_choices = [(70, "US"), (15, "GB"), (10, "CA"), (5, "AU")]
    kyb_choices = [(85, "approved"), (10, "pending"), (3, "review"), (2, "rejected")]
    pricing_choices = [(60, "standard"), (25, "premium"), (15, "enterprise")]
    risk_choices = [(70, "low"), (20, "medium"), (8, "high"), (2, "critical")]

    count = 0
    while True:
        merchant = {
            "merchant_id": bothify('m_#####??##'),
            "legal_name": random_company_name(),
            "mcc": random.choice(mcc_codes),
            "country": weighted_choice(country_choices),
            "kyb_status": weighted_choice(kyb_choices),
            "pricing_tier": weighted_choice(pricing_choices),
            "risk_level": weighted_choice(risk_choices),
            "created_at": random_date(MERCHANT_START_DATE, MERCHANT_END_DATE)
        }
        merchants.append(merchant)
        count += 1

        if count % 100 == 0:
            size_mb = len(json.dumps(merchants)) / (1024 * 1024)
            if size_mb >= target_size_mb:
                break

    print(f"  Generated {len(merchants)} merchants ({size_mb:.2f} MB)")

def generate_customers(target_size_mb: float = 4.0) -> None:
    """Generate customers.jsonl"""
    print("Generating customers...")

    type_choices = [(80, "regular"), (15, "vip"), (5, "flagged")]

    count = 0
    while True:
        customer = {
            "customer_id": bothify('c_#####??##'),
            "email_hash": bothify('hash_????????????????'),
            "phone_hash": bothify('hash_????????????????'),
            "customer_type": weighted_choice(type_choices),
            "created_at": random_date(CUSTOMER_START_DATE, CUSTOMER_END_DATE)
        }
        customers.append(customer)
        count += 1

        if count % 100 == 0:
            size_mb = len(json.dumps(customers)) / (1024 * 1024)
            if size_mb >= target_size_mb:
                break

    print(f"  Generated {len(customers)} customers ({size_mb:.2f} MB)")

def generate_payments(target_size_mb: float = 4.0) -> None:
    """Generate payments.jsonl"""
    print("Generating payments...")

    brand_choices = [(45, "visa"), (35, "mastercard"), (10, "amex"), (7, "discover"), (3, "diners")]
    bins = ["411111", "542800", "378282", "601100", "370000", "555555", "424242", "500000"]
    year_choices = [(5, 2024), (25, 2025), (30, 2026), (25, 2027), (10, 2028), (5, 2029)]
    wallet_choices = [(50, None), (25, "applepay"), (20, "googlepay"), (5, "samsungpay")]
    status_choices = [(90, "active"), (5, "expired"), (3, "blocked"), (2, "lost_stolen")]

    count = 0
    while True:
        payment = {
            "payment_id": bothify('pm_####??####'),
            "customer_id": random.choice(customers)["customer_id"],
            "brand": weighted_choice(brand_choices),
            "bin": random.choice(bins),
            "last4": bothify('####'),
            "expiry_month": random.randint(1, 12),
            "expiry_year": weighted_choice(year_choices),
            "wallet_type": weighted_choice(wallet_choices),
            "status": weighted_choice(status_choices),
            "first_seen_at": random_date(PAYMENT_START_DATE, PAYMENT_END_DATE)
        }
        payments.append(payment)
        count += 1

        if count % 100 == 0:
            size_mb = len(json.dumps(payments)) / (1024 * 1024)
            if size_mb >= target_size_mb:
                break

    print(f"  Generated {len(payments)} payments ({size_mb:.2f} MB)")

def generate_orders(target_size_mb: float = 4.0) -> None:
    """Generate orders.jsonl"""
    print("Generating orders...")

    currency_choices = [(70, "USD"), (15, "GBP"), (10, "CAD"), (5, "AUD")]
    channel_choices = [(55, "ecommerce"), (30, "pos"), (10, "mobile"), (5, "ivr")]

    count = 0
    while True:
        subtotal_cents = random.randint(500, 25000)
        tax_rate = random.uniform(0.05, 0.15)
        tip_rate = random.uniform(0, 0.25)
        tax_cents = int(subtotal_cents * tax_rate)
        tip_cents = int(subtotal_cents * tip_rate)
        total_amount_cents = subtotal_cents + tax_cents + tip_cents

        order = {
            "order_id": bothify('ord_#####??####'),
            "merchant_id": random.choice(merchants)["merchant_id"],
            "customer_id": random.choice(customers)["customer_id"],
            "currency": weighted_choice(currency_choices),
            "subtotal_cents": subtotal_cents,
            "tax_cents": tax_cents,
            "tip_cents": tip_cents,
            "total_amount_cents": total_amount_cents,
            "channel": weighted_choice(channel_choices),
            "created_at": random_date(ORDER_START_DATE, ORDER_END_DATE)
        }
        orders.append(order)
        count += 1

        if count % 100 == 0:
            size_mb = len(json.dumps(orders)) / (1024 * 1024)
            if size_mb >= target_size_mb:
                break

    print(f"  Generated {len(orders)} orders ({size_mb:.2f} MB)")

def generate_transactions(target_size_mb: float = 4.0) -> None:
    """Generate transactions.jsonl"""
    print("Generating transactions...")

    state_transitions = {
        "pending": [(92, "authorized"), (8, "declined")],
        "authorized": [(95, "captured"), (5, "void")],
        "captured": [(97, "settled"), (3, "refund_pending")],
        "refund_pending": [(100, "refunded")],
        "settled": [(98, "completed"), (2, "disputed")],
        "declined": [(100, "failed")],
        "void": [(100, "cancelled")],
        "refunded": [(100, "closed")],
        "disputed": [(100, "under_review")],
        "under_review": [(60, "completed"), (40, "chargeback")],
        "chargeback": [(100, "closed")],
        "completed": [],
        "failed": [],
        "cancelled": [],
        "closed": []
    }

    response_choices = [(92, "00"), (3, "05"), (2, "51"), (1, "54"), (1, "61"), (1, "65")]
    three_ds_choices = [(65, "frictionless"), (20, "challenge"), (10, "attempted"), (5, "not_supported")]
    processors = ["visa_network", "mastercard_network", "amex_network", "discover_network"]

    count = 0
    while True:
        order = random.choice(orders)
        payment = random.choice(payments)

        current_state = "pending"
        auth_timestamp = datetime.fromisoformat(order["created_at"].replace('Z', '+00:00'))

        state_history = []
        while state_transitions.get(current_state):
            choices = state_transitions[current_state]
            if not choices:
                break
            next_state = weighted_choice(choices)
            current_state = next_state

        final_state = {
            "state_name": current_state,
            "timestamp": int(auth_timestamp.timestamp() * 1000)
        }

        base_fee_rate = random.uniform(0.024, 0.032)
        fixed_fee = random.randint(20, 35)
        fees_total_cents = int((order["total_amount_cents"] * base_fee_rate) + fixed_fee)

        transaction = {
            "txn_id": bothify('txn_#####??####'),
            "order_id": order["order_id"],
            "payment_id": payment["payment_id"],
            "amount_cents": order["total_amount_cents"],
            "currency": order["currency"],
            "state": final_state,
            "response_code": weighted_choice(response_choices),
            "three_ds": weighted_choice(three_ds_choices),
            "authorized_at": auth_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "fees_total_cents": fees_total_cents,
            "network_fee_cents": random.randint(8, 18),
            "processor_name": random.choice(processors)
        }
        transactions.append(transaction)
        count += 1

        if count % 100 == 0:
            size_mb = len(json.dumps(transactions)) / (1024 * 1024)
            if size_mb >= target_size_mb:
                break

    print(f"  Generated {len(transactions)} transactions ({size_mb:.2f} MB)")

def generate_payouts(target_size_mb: float = 4.0) -> None:
    """Generate payouts.jsonl"""
    print("Generating payouts...")

    currency_choices = [(70, "USD"), (15, "GBP"), (10, "CAD"), (5, "AUD")]
    status_choices = [(85, "paid"), (10, "pending"), (3, "in_transit"), (2, "failed")]

    count = 0
    while True:
        batch_date = random_date(ORDER_START_DATE, ORDER_END_DATE)
        batch_dt = datetime.fromisoformat(batch_date.replace('Z', '+00:00'))

        gross_amount = random.randint(10000, 500000)
        fee_rate = random.uniform(0.025, 0.035)
        reserve_rate = random.uniform(0.001, 0.005)
        fees_cents = int(gross_amount * fee_rate)
        reserve_cents = int(gross_amount * reserve_rate)
        net_cents = gross_amount - fees_cents - reserve_cents
        delay_hours = random.randint(24, 48)
        paid_dt = batch_dt + timedelta(hours=delay_hours)

        payout = {
            "payout_id": bothify('pay_#####??####'),
            "merchant_id": random.choice(merchants)["merchant_id"],
            "batch_day": batch_dt.strftime('%Y-%m-%d'),
            "currency": weighted_choice(currency_choices),
            "gross_cents": gross_amount,
            "fees_cents": fees_cents,
            "reserve_cents": reserve_cents,
            "net_cents": net_cents,
            "status": weighted_choice(status_choices),
            "paid_at": paid_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "transaction_count": random.randint(10, 500)
        }
        payouts.append(payout)
        count += 1

        if count % 100 == 0:
            size_mb = len(json.dumps(payouts)) / (1024 * 1024)
            if size_mb >= target_size_mb:
                break

    print(f"  Generated {len(payouts)} payouts ({size_mb:.2f} MB)")

def generate_disputes(target_size_mb: float = 4.0) -> None:
    """Generate disputes.jsonl"""
    print("Generating disputes...")

    reason_choices = [
        (40, "FRAUD"), (25, "PRODUCT_NOT_RECEIVED"), (15, "NOT_AS_DESCRIBED"),
        (10, "DUPLICATE"), (5, "CREDIT_NOT_PROCESSED"), (5, "SUBSCRIPTION_CANCELED")
    ]
    stage_choices = [(40, "inquiry"), (35, "chargeback"), (15, "pre_arbitration"), (10, "arbitration")]
    liability_choices = [(65, "merchant"), (25, "issuer"), (10, "shared")]
    status_choices = [(40, "open"), (30, "lost"), (20, "won"), (10, "pending_evidence")]

    count = 0
    while True:
        transaction = random.choice(transactions)
        opened_at = random_date(ORDER_START_DATE, ORDER_END_DATE)

        closed_at = None
        if random.random() < 0.30:
            closed_at = random_date(ORDER_START_DATE, ORDER_END_DATE)

        dispute = {
            "dispute_id": bothify('cb_#####??####'),
            "txn_id": transaction["txn_id"],
            "reason_code": weighted_choice(reason_choices),
            "amount_cents": transaction["amount_cents"],
            "stage": weighted_choice(stage_choices),
            "opened_at": opened_at,
            "closed_at": closed_at,
            "liability": weighted_choice(liability_choices),
            "status": weighted_choice(status_choices)
        }
        disputes.append(dispute)
        count += 1

        if count % 100 == 0:
            size_mb = len(json.dumps(disputes)) / (1024 * 1024)
            if size_mb >= target_size_mb:
                break

    print(f"  Generated {len(disputes)} disputes ({size_mb:.2f} MB)")

def write_jsonl(filename: str, data: List[Dict[str, Any]]) -> None:
    """Write data to JSONL file"""
    output_dir = "../data"
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)

    with open(filepath, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')

    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  Wrote {filepath} ({size_mb:.2f} MB, {len(data)} records)")

def main():
    print("PSP Sample Data Generator")
    print("=" * 60)
    print("Generating 3-5 MB files with perfect referential integrity\n")
    
    generate_merchants(target_size_mb=4.0)
    write_jsonl("merchants.jsonl", merchants)

    generate_customers(target_size_mb=4.0)
    write_jsonl("customers.jsonl", customers)

    generate_payments(target_size_mb=4.0)
    write_jsonl("payments.jsonl", payments)

    generate_orders(target_size_mb=4.0)
    write_jsonl("orders.jsonl", orders)

    generate_transactions(target_size_mb=4.0)
    write_jsonl("transactions.jsonl", transactions)

    generate_payouts(target_size_mb=4.0)
    write_jsonl("payouts.jsonl", payouts)

    generate_disputes(target_size_mb=4.0)
    write_jsonl("disputes.jsonl", disputes)

    print("\n" + "=" * 60)
    print("✓ All files generated successfully!")
    print("\nReferential Integrity Summary:")
    print(f"  • {len(payments)} payments reference {len(customers)} customers")
    print(f"  • {len(orders)} orders reference {len(merchants)} merchants & {len(customers)} customers")
    print(f"  • {len(transactions)} transactions reference {len(orders)} orders & {len(payments)} payments")
    print(f"  • {len(payouts)} payouts reference {len(merchants)} merchants")
    print(f"  • {len(disputes)} disputes reference {len(transactions)} transactions")

if __name__ == "__main__":
    main()
