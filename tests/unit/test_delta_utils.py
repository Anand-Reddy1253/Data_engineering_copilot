"""Unit tests for notebooks/_shared/delta_utils.py."""

import os
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "../../notebooks/_shared")
)
from delta_utils import upsert_to_delta


@pytest.fixture
def sample_schema() -> StructType:
    """Return a simple schema for test DataFrames."""
    return StructType([
        StructField("id", StringType(), False),
        StructField("value", IntegerType(), True),
    ])


def test_upsert_to_delta_inserts_new_records(
    spark: SparkSession, tmp_delta_path: str, sample_schema: StructType
) -> None:
    """Test that upsert_to_delta inserts new records when table does not exist."""
    data = [("id_001", 100), ("id_002", 200)]
    df = spark.createDataFrame(data, schema=sample_schema)

    upsert_to_delta(df, tmp_delta_path, merge_key=["id"])

    result = spark.read.format("delta").load(tmp_delta_path)
    assert result.count() == 2
    assert set(result.columns) == {"id", "value"}


def test_upsert_to_delta_updates_existing_records(
    spark: SparkSession, tmp_delta_path: str, sample_schema: StructType
) -> None:
    """Test that upsert_to_delta updates existing records on merge key match."""
    initial_data = [("id_001", 100), ("id_002", 200)]
    initial_df = spark.createDataFrame(initial_data, schema=sample_schema)
    upsert_to_delta(initial_df, tmp_delta_path, merge_key=["id"])

    updated_data = [("id_001", 999)]
    updated_df = spark.createDataFrame(updated_data, schema=sample_schema)
    upsert_to_delta(updated_df, tmp_delta_path, merge_key=["id"])

    result = spark.read.format("delta").load(tmp_delta_path)
    assert result.count() == 2

    updated_row = result.filter(result["id"] == "id_001").collect()[0]
    assert updated_row["value"] == 999

    unchanged_row = result.filter(result["id"] == "id_002").collect()[0]
    assert unchanged_row["value"] == 200


def test_upsert_to_delta_inserts_new_and_updates_existing(
    spark: SparkSession, tmp_delta_path: str, sample_schema: StructType
) -> None:
    """Test that upsert handles mix of new inserts and updates in same batch."""
    initial_data = [("id_001", 100)]
    initial_df = spark.createDataFrame(initial_data, schema=sample_schema)
    upsert_to_delta(initial_df, tmp_delta_path, merge_key=["id"])

    mixed_data = [("id_001", 777), ("id_003", 300)]
    mixed_df = spark.createDataFrame(mixed_data, schema=sample_schema)
    upsert_to_delta(mixed_df, tmp_delta_path, merge_key=["id"])

    result = spark.read.format("delta").load(tmp_delta_path)
    assert result.count() == 2

    ids = {row["id"] for row in result.collect()}
    assert ids == {"id_001", "id_003"}


def test_upsert_to_delta_handles_null_value_column(
    spark: SparkSession, tmp_delta_path: str, sample_schema: StructType
) -> None:
    """Test that upsert correctly handles null values in non-key columns."""
    data = [("id_001", None), ("id_002", 200)]
    df = spark.createDataFrame(data, schema=sample_schema)

    upsert_to_delta(df, tmp_delta_path, merge_key=["id"])

    result = spark.read.format("delta").load(tmp_delta_path)
    assert result.count() == 2
    null_row = result.filter(result["id"] == "id_001").collect()[0]
    assert null_row["value"] is None


def test_upsert_to_delta_raises_on_empty_merge_key(
    spark: SparkSession, tmp_delta_path: str, sample_schema: StructType
) -> None:
    """Test that upsert raises ValueError when merge_key is empty."""
    data = [("id_001", 100)]
    df = spark.createDataFrame(data, schema=sample_schema)

    with pytest.raises(ValueError, match="merge_key must contain at least one column"):
        upsert_to_delta(df, tmp_delta_path, merge_key=[])


def test_upsert_to_delta_raises_on_missing_merge_key_column(
    spark: SparkSession, tmp_delta_path: str, sample_schema: StructType
) -> None:
    """Test that upsert raises ValueError when merge_key column is not in DataFrame."""
    data = [("id_001", 100)]
    df = spark.createDataFrame(data, schema=sample_schema)

    with pytest.raises(ValueError, match="merge_key columns not in DataFrame"):
        upsert_to_delta(df, tmp_delta_path, merge_key=["nonexistent_column"])
