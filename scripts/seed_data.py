"""Seed data generator for the Contoso Fabric Platform.

Generates realistic sample data for all four data sources using the Faker library.
Writes CSV/JSON files to lakehouse/bronze/raw/ for use in development and testing.

Usage:
    python scripts/seed_data.py --small    # 1,000 rows per entity
    python scripts/seed_data.py --full     # 100,000 rows per entity
"""

import argparse
import csv
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    from faker import Faker
except ImportError as err:
    raise ImportError("Install faker: pip install faker") from err

fake = Faker()
Faker.seed(42)
random.seed(42)

BASE_OUTPUT_DIR = Path("lakehouse/bronze/raw")
STORE_IDS = [f"STORE{i:03d}" for i in range(1, 21)]
PRODUCT_IDS = [f"PROD{i:04d}" for i in range(1, 101)]
CUSTOMER_IDS = [f"CUST{i:06d}" for i in range(1, 5001)]
WAREHOUSE_IDS = [f"WH{i:03d}" for i in range(1, 6)]
PAYMENT_METHODS = ["CASH", "CARD", "DIGITAL", "VOUCHER"]
MOVEMENT_TYPES = ["IN", "OUT"]
EVENT_TYPES = ["page_view", "product_click", "add_to_cart", "checkout", "search"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
PAGE_URLS = [
    "/home",
    "/products",
    "/products/electronics",
    "/products/clothing",
    "/cart",
    "/checkout",
    "/account",
    "/search",
    "/offers",
]


def random_date(start_days_ago: int = 365, end_days_ago: int = 0) -> datetime:
    """Generate a random datetime within the past N days."""
    start = datetime.now(tz=timezone.utc) - timedelta(days=start_days_ago)
    end = datetime.now(tz=timezone.utc) - timedelta(days=end_days_ago)
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def generate_pos_transactions(n: int, output_dir: Path) -> None:
    """Generate POS transaction CSV files.

    Args:
        n: Number of rows to generate.
        output_dir: Output directory for CSV files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "pos_transactions_seed.csv"
    fieldnames = [
        "transaction_id",
        "store_id",
        "product_id",
        "customer_id",
        "quantity",
        "unit_price",
        "sale_date",
        "payment_method",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(n):
            writer.writerow({
                "transaction_id": str(uuid.uuid4()),
                "store_id": random.choice(STORE_IDS),
                "product_id": random.choice(PRODUCT_IDS),
                "customer_id": random.choice(CUSTOMER_IDS),
                "quantity": random.randint(1, 20),
                "unit_price": round(random.uniform(0.99, 999.99), 2),
                "sale_date": random_date().strftime("%Y-%m-%d %H:%M:%S"),
                "payment_method": random.choice(PAYMENT_METHODS),
            })
    print(f"Generated {n} POS transactions → {filepath}")


def generate_inventory_movements(n: int, output_dir: Path) -> None:
    """Generate inventory movement CSV files.

    Args:
        n: Number of rows to generate.
        output_dir: Output directory for CSV files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "inventory_movements_seed.csv"
    fieldnames = [
        "movement_id",
        "warehouse_id",
        "product_id",
        "movement_type",
        "quantity",
        "movement_date",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(n):
            writer.writerow({
                "movement_id": str(uuid.uuid4()),
                "warehouse_id": random.choice(WAREHOUSE_IDS),
                "product_id": random.choice(PRODUCT_IDS),
                "movement_type": random.choice(MOVEMENT_TYPES),
                "quantity": random.randint(1, 500),
                "movement_date": random_date().strftime("%Y-%m-%d %H:%M:%S"),
            })
    print(f"Generated {n} inventory movements → {filepath}")


def generate_customers(n: int, output_dir: Path) -> None:
    """Generate customer CSV files with realistic PII.

    Args:
        n: Number of rows to generate.
        output_dir: Output directory for CSV files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "customers_seed.csv"
    fieldnames = [
        "customer_id",
        "name",
        "email",
        "phone",
        "address",
        "city",
        "country",
        "signup_date",
    ]
    customer_count = min(n, len(CUSTOMER_IDS))
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(customer_count):
            writer.writerow({
                "customer_id": CUSTOMER_IDS[i],
                "name": fake.name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "address": fake.street_address(),
                "city": fake.city(),
                "country": fake.country(),
                "signup_date": random_date(start_days_ago=1825).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            })
    print(f"Generated {customer_count} customers → {filepath}")


def generate_clickstream(n: int, output_dir: Path) -> None:
    """Generate web clickstream JSON files.

    Args:
        n: Number of rows to generate.
        output_dir: Output directory for JSON files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / "clickstream_seed.json"
    records: list[dict[str, Any]] = []
    for _ in range(n):
        records.append({
            "session_id": str(uuid.uuid4()),
            "customer_id": random.choice(CUSTOMER_IDS + [None, None]),
            "page_url": random.choice(PAGE_URLS),
            "event_type": random.choice(EVENT_TYPES),
            "timestamp": random_date(start_days_ago=90).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "device_type": random.choice(DEVICE_TYPES),
        })
    with open(filepath, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    print(f"Generated {n} clickstream events → {filepath}")


def main() -> None:
    """Entry point for seed data generation."""
    parser = argparse.ArgumentParser(
        description="Generate seed data for the Contoso Fabric Platform"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--small",
        action="store_true",
        help="Generate small dataset (1,000 rows per entity)",
    )
    group.add_argument(
        "--full",
        action="store_true",
        help="Generate full dataset (100,000 rows per entity)",
    )
    args = parser.parse_args()

    n_rows = 1000 if args.small else (100_000 if args.full else 1000)
    print(f"Generating seed data: {n_rows} rows per entity")

    generate_pos_transactions(n_rows, BASE_OUTPUT_DIR / "pos_transactions")
    generate_inventory_movements(n_rows, BASE_OUTPUT_DIR / "inventory_movements")
    generate_customers(min(n_rows, 5000), BASE_OUTPUT_DIR / "customers")
    generate_clickstream(n_rows, BASE_OUTPUT_DIR / "clickstream")

    print("\nSeed data generation complete!")
    print(f"Output directory: {BASE_OUTPUT_DIR}")


if __name__ == "__main__":
    main()
