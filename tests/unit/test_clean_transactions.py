"""Unit tests for notebooks/silver/clean_transactions.py."""

import os
import sys

import pytest
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "../../notebooks/_shared")
)
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "../../notebooks/silver")
)

from clean_transactions import MERGE_KEY, clean_transactions


@pytest.fixture
def bronze_schema() -> StructType:
    """Return the Bronze POS transactions schema (all strings, as from CSV)."""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("sale_date", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_source_file", StringType(), True),
    ])


@pytest.fixture
def bronze_df(spark: SparkSession, bronze_schema: StructType, tmp_delta_path: str):
    """Create a Bronze Delta table with sample transaction data."""
    from delta_utils import upsert_to_delta

    data = [
        ("T001", "S01", "P01", "C01", "5", "19.99", "2024-01-15 10:00:00", "CARD", None, "file.csv"),
        ("T002", "S02", "P02", "C02", "3", "29.99", "2024-01-15 11:00:00", "cash", None, "file.csv"),
        ("T003", "S01", "P03", None, "10", "9.99", "2024-01-15 12:00:00", "DIGITAL", None, "file.csv"),
    ]
    df = spark.createDataFrame(data, schema=bronze_schema)
    upsert_to_delta(df, tmp_delta_path, merge_key=["transaction_id"])
    return tmp_delta_path


def test_clean_transactions_happy_path(
    spark: SparkSession, bronze_df: str, tmp_path
) -> None:
    """Test that clean_transactions processes valid data correctly."""
    target_path = str(tmp_path / "silver_txn")

    result_df = clean_transactions(spark, bronze_df, target_path)

    assert result_df.count() == 3
    assert "transaction_id" in result_df.columns
    assert "total_amount" in result_df.columns
    assert "_processed_at" in result_df.columns
    assert "_ingested_at" not in result_df.columns
    assert "_source_file" not in result_df.columns


def test_clean_transactions_normalises_payment_method(
    spark: SparkSession, bronze_df: str, tmp_path
) -> None:
    """Test that payment_method is normalised to uppercase."""
    target_path = str(tmp_path / "silver_txn")

    result_df = clean_transactions(spark, bronze_df, target_path)

    payment_methods = {row["payment_method"] for row in result_df.collect()}
    for pm in payment_methods:
        assert pm == pm.upper(), f"Payment method not uppercase: {pm}"


def test_clean_transactions_removes_duplicates(
    spark: SparkSession, tmp_path, bronze_schema: StructType
) -> None:
    """Test that duplicates on transaction_id are removed."""
    from delta_utils import upsert_to_delta

    source_path = str(tmp_path / "bronze_dups")
    target_path = str(tmp_path / "silver_deduped")

    dup_data = [
        ("T001", "S01", "P01", "C01", "5", "19.99", "2024-01-15 10:00:00", "CARD", None, "a.csv"),
        ("T001", "S01", "P01", "C01", "5", "19.99", "2024-01-15 10:00:00", "CARD", None, "b.csv"),
    ]
    dup_df = spark.createDataFrame(dup_data, schema=bronze_schema)
    dup_df.write.format("delta").mode("overwrite").save(source_path)

    result_df = clean_transactions(spark, source_path, target_path)
    assert result_df.count() == 1


def test_clean_transactions_filters_null_transaction_id(
    spark: SparkSession, tmp_path, bronze_schema: StructType
) -> None:
    """Test that rows with null transaction_id are filtered out."""
    from delta_utils import upsert_to_delta

    source_path = str(tmp_path / "bronze_nulls")
    target_path = str(tmp_path / "silver_no_nulls")

    null_data = [
        ("T001", "S01", "P01", "C01", "5", "19.99", "2024-01-15 10:00:00", "CARD", None, "a.csv"),
        (None, "S02", "P02", "C02", "3", "9.99", "2024-01-15 11:00:00", "CASH", None, "a.csv"),
    ]
    null_df = spark.createDataFrame(null_data, schema=bronze_schema)
    null_df.write.format("delta").mode("overwrite").save(source_path)

    result_df = clean_transactions(spark, source_path, target_path)
    assert result_df.count() == 1
    assert result_df.filter(F.col("transaction_id").isNull()).count() == 0


def test_clean_transactions_empty_dataframe(
    spark: SparkSession, tmp_path, bronze_schema: StructType
) -> None:
    """Test that clean_transactions handles empty input DataFrame."""
    source_path = str(tmp_path / "bronze_empty")
    target_path = str(tmp_path / "silver_empty")

    empty_df = spark.createDataFrame([], schema=bronze_schema)
    empty_df.write.format("delta").mode("overwrite").save(source_path)

    result_df = clean_transactions(spark, source_path, target_path)
    assert result_df.count() == 0
