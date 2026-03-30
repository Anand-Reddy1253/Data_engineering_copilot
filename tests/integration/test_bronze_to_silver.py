"""Integration tests for the Bronze → Silver pipeline flow."""

import os
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "../../notebooks/_shared")
)
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "../../notebooks/bronze")
)
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "../../notebooks/silver")
)


@pytest.mark.integration
def test_bronze_to_silver_pos_transactions_end_to_end(
    spark, tmp_path
) -> None:
    """End-to-end test: seed Bronze CSV → ingest to Bronze → clean to Silver."""
    import csv

    from ingest_pos_transactions import ingest_pos_transactions
    from clean_transactions import clean_transactions

    # Arrange: create raw CSV files
    raw_dir = str(tmp_path / "raw" / "pos_transactions")
    os.makedirs(raw_dir, exist_ok=True)
    csv_file = os.path.join(raw_dir, "transactions_2024_01.csv")
    rows = [
        ["transaction_id", "store_id", "product_id", "customer_id", "quantity", "unit_price", "sale_date", "payment_method"],
        ["TXN001", "STORE01", "PROD001", "CUST001", "2", "49.99", "2024-01-15 10:00:00", "CARD"],
        ["TXN002", "STORE01", "PROD002", "CUST002", "1", "99.99", "2024-01-15 11:00:00", "DIGITAL"],
        ["TXN003", "STORE02", "PROD001", "CUST001", "3", "49.99", "2024-01-16 09:00:00", "CASH"],
    ]
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    bronze_path = str(tmp_path / "bronze" / "pos_transactions")
    silver_path = str(tmp_path / "silver" / "pos_transactions")

    # Act: run Bronze ingestion
    bronze_df = ingest_pos_transactions(spark, raw_dir, bronze_path)

    # Assert Bronze
    assert bronze_df.count() == 3
    assert "_ingested_at" in bronze_df.columns
    assert "_source_file" in bronze_df.columns

    # Act: run Silver cleaning
    silver_df = clean_transactions(spark, bronze_path, silver_path)

    # Assert Silver
    assert silver_df.count() == 3
    assert "_ingested_at" not in silver_df.columns
    assert "total_amount" in silver_df.columns
    assert "_processed_at" in silver_df.columns

    # Verify total_amount is computed correctly for TXN001: 2 * 49.99 = 99.98
    txn1 = silver_df.filter(silver_df["transaction_id"] == "TXN001").collect()[0]
    assert float(txn1["total_amount"]) == pytest.approx(99.98, rel=1e-3)


@pytest.mark.integration
def test_bronze_to_silver_idempotency(spark, tmp_path) -> None:
    """Test that running Bronze and Silver twice produces the same result."""
    import csv

    from ingest_pos_transactions import ingest_pos_transactions
    from clean_transactions import clean_transactions

    raw_dir = str(tmp_path / "raw2" / "pos_transactions")
    os.makedirs(raw_dir, exist_ok=True)
    csv_file = os.path.join(raw_dir, "transactions.csv")
    rows = [
        ["transaction_id", "store_id", "product_id", "customer_id", "quantity", "unit_price", "sale_date", "payment_method"],
        ["TXN100", "S01", "P01", "C01", "1", "10.00", "2024-02-01 08:00:00", "CASH"],
    ]
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    bronze_path = str(tmp_path / "bronze2" / "pos_transactions")
    silver_path = str(tmp_path / "silver2" / "pos_transactions")

    # First run
    ingest_pos_transactions(spark, raw_dir, bronze_path)
    clean_transactions(spark, bronze_path, silver_path)
    first_count = spark.read.format("delta").load(silver_path).count()

    # Second run (re-ingest same data)
    ingest_pos_transactions(spark, raw_dir, bronze_path)
    clean_transactions(spark, bronze_path, silver_path)
    second_count = spark.read.format("delta").load(silver_path).count()

    assert first_count == second_count, (
        f"Idempotency violation: first run={first_count}, second run={second_count}"
    )
