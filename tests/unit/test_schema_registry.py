"""Unit tests for notebooks/_shared/schema_registry.py."""

import os
import sys

import pytest
from pyspark.sql.types import StructType

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "../../notebooks/_shared")
)
from schema_registry import SCHEMAS, get_schema, list_schemas


def test_get_schema_returns_struct_type_for_all_tables() -> None:
    """Test that get_schema returns a StructType for every registered table."""
    for table_name in SCHEMAS:
        schema = get_schema(table_name)
        assert isinstance(schema, StructType), f"{table_name} schema is not StructType"


def test_get_schema_bronze_pos_transactions_has_required_columns() -> None:
    """Test that bronze_pos_transactions schema has all expected columns."""
    schema = get_schema("bronze_pos_transactions")
    column_names = {field.name for field in schema.fields}
    expected_columns = {
        "transaction_id",
        "store_id",
        "product_id",
        "customer_id",
        "quantity",
        "unit_price",
        "sale_date",
        "payment_method",
        "_ingested_at",
        "_source_file",
    }
    assert expected_columns.issubset(column_names), (
        f"Missing columns: {expected_columns - column_names}"
    )


def test_get_schema_silver_customers_has_hashed_pii_columns() -> None:
    """Test that silver_customers schema uses hashed PII columns."""
    schema = get_schema("silver_customers")
    column_names = {field.name for field in schema.fields}

    # Original PII must NOT be present
    raw_pii_columns = {"email", "phone", "address", "name"}
    for pii_col in raw_pii_columns:
        assert pii_col not in column_names, (
            f"Raw PII column '{pii_col}' found in silver_customers schema"
        )

    # Hashed versions must be present
    hashed_columns = {"email_hash", "phone_hash", "address_hash", "name_hash"}
    assert hashed_columns.issubset(column_names), (
        f"Missing hash columns: {hashed_columns - column_names}"
    )


def test_get_schema_gold_fact_sales_has_surrogate_keys() -> None:
    """Test that gold_fact_sales has the expected dimension surrogate keys."""
    schema = get_schema("gold_fact_sales")
    column_names = {field.name for field in schema.fields}
    expected_sk_columns = {"customer_sk", "product_sk", "store_sk", "date_sk"}
    assert expected_sk_columns.issubset(column_names), (
        f"Missing surrogate key columns: {expected_sk_columns - column_names}"
    )


def test_get_schema_raises_key_error_for_unknown_table() -> None:
    """Test that get_schema raises KeyError for unregistered table names."""
    with pytest.raises(KeyError, match="not found in registry"):
        get_schema("nonexistent_table_xyz")


def test_list_schemas_returns_sorted_list() -> None:
    """Test that list_schemas returns a sorted list of table names."""
    schemas = list_schemas()
    assert schemas == sorted(schemas), "list_schemas() should return sorted list"
    assert len(schemas) > 0, "list_schemas() should not be empty"


def test_all_bronze_schemas_have_ingestion_metadata() -> None:
    """Test that all bronze schemas include _ingested_at and _source_file."""
    bronze_schemas = [name for name in SCHEMAS if name.startswith("bronze_")]
    for table_name in bronze_schemas:
        schema = get_schema(table_name)
        column_names = {field.name for field in schema.fields}
        assert "_ingested_at" in column_names, (
            f"{table_name} missing _ingested_at column"
        )
        assert "_source_file" in column_names, (
            f"{table_name} missing _source_file column"
        )


def test_all_gold_dim_schemas_have_scd_columns() -> None:
    """Test that all Gold dimension schemas include SCD Type 2 columns."""
    dim_schemas = [name for name in SCHEMAS if name.startswith("gold_dim_")]
    for table_name in dim_schemas:
        schema = get_schema(table_name)
        column_names = {field.name for field in schema.fields}
        scd_columns = {"_valid_from", "_valid_to", "_is_current"}
        assert scd_columns.issubset(column_names), (
            f"{table_name} missing SCD columns: {scd_columns - column_names}"
        )
