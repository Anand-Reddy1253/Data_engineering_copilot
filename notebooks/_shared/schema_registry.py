"""Schema registry for the Contoso Fabric Platform.

Defines PySpark StructType schemas for all Bronze, Silver, and Gold tables.
This is the single source of truth for table schemas across all notebooks.
"""

from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# fmt: off
SCHEMAS: dict[str, StructType] = {

    # -------------------------------------------------------------------------
    # BRONZE SCHEMAS — raw ingestion, all nullable, string types where uncertain
    # -------------------------------------------------------------------------
    "bronze_pos_transactions": StructType([
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
    ]),

    "bronze_inventory_movements": StructType([
        StructField("movement_id", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("movement_type", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("movement_date", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_source_file", StringType(), True),
    ]),

    "bronze_customers": StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_source_file", StringType(), True),
    ]),

    "bronze_clickstream": StructType([
        StructField("session_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_source_file", StringType(), True),
    ]),

    # -------------------------------------------------------------------------
    # SILVER SCHEMAS — cleaned, typed, PII hashed
    # -------------------------------------------------------------------------
    "silver_pos_transactions": StructType([
        StructField("transaction_id", StringType(), False),
        StructField("store_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(12, 2), False),
        StructField("sale_date", TimestampType(), False),
        StructField("payment_method", StringType(), False),
        StructField("total_amount", DecimalType(14, 2), True),
        StructField("_processed_at", TimestampType(), False),
    ]),

    "silver_inventory_movements": StructType([
        StructField("movement_id", StringType(), False),
        StructField("warehouse_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("movement_type", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("movement_date", TimestampType(), False),
        StructField("_processed_at", TimestampType(), False),
    ]),

    "silver_customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("name_hash", StringType(), True),
        StructField("email_hash", StringType(), True),
        StructField("phone_hash", StringType(), True),
        StructField("address_hash", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", TimestampType(), True),
        StructField("_processed_at", TimestampType(), False),
    ]),

    "silver_clickstream": StructType([
        StructField("session_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("page_url", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("device_type", StringType(), True),
        StructField("session_start", TimestampType(), True),
        StructField("session_duration_seconds", LongType(), True),
        StructField("_processed_at", TimestampType(), False),
    ]),

    # -------------------------------------------------------------------------
    # GOLD SCHEMAS — star schema and aggregates
    # -------------------------------------------------------------------------
    "gold_dim_customer": StructType([
        StructField("customer_sk", LongType(), False),
        StructField("customer_bk", StringType(), False),
        StructField("name_hash", StringType(), True),
        StructField("email_hash", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", TimestampType(), True),
        StructField("_valid_from", TimestampType(), False),
        StructField("_valid_to", TimestampType(), True),
        StructField("_is_current", BooleanType(), False),
    ]),

    "gold_dim_product": StructType([
        StructField("product_sk", LongType(), False),
        StructField("product_bk", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("unit_cost", DecimalType(12, 2), True),
        StructField("_valid_from", TimestampType(), False),
        StructField("_valid_to", TimestampType(), True),
        StructField("_is_current", BooleanType(), False),
    ]),

    "gold_dim_store": StructType([
        StructField("store_sk", LongType(), False),
        StructField("store_bk", StringType(), False),
        StructField("store_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("_valid_from", TimestampType(), False),
        StructField("_valid_to", TimestampType(), True),
        StructField("_is_current", BooleanType(), False),
    ]),

    "gold_dim_date": StructType([
        StructField("date_sk", IntegerType(), False),
        StructField("full_date", TimestampType(), False),
        StructField("year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), False),
        StructField("day", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), False),
        StructField("is_weekend", BooleanType(), False),
    ]),

    "gold_fact_sales": StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_sk", LongType(), True),
        StructField("product_sk", LongType(), True),
        StructField("store_sk", LongType(), True),
        StructField("date_sk", IntegerType(), True),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(12, 2), False),
        StructField("total_amount", DecimalType(14, 2), False),
        StructField("payment_method", StringType(), True),
        StructField("_processed_at", TimestampType(), False),
    ]),

    "gold_fact_inventory_snapshot": StructType([
        StructField("snapshot_id", StringType(), False),
        StructField("warehouse_id", StringType(), False),
        StructField("product_sk", LongType(), True),
        StructField("date_sk", IntegerType(), True),
        StructField("units_in", IntegerType(), False),
        StructField("units_out", IntegerType(), False),
        StructField("net_movement", IntegerType(), False),
        StructField("_processed_at", TimestampType(), False),
    ]),

    "gold_agg_customer_360": StructType([
        StructField("customer_bk", StringType(), False),
        StructField("total_orders", LongType(), False),
        StructField("total_spend", DecimalType(16, 2), False),
        StructField("avg_order_value", DecimalType(12, 2), True),
        StructField("first_purchase_date", TimestampType(), True),
        StructField("last_purchase_date", TimestampType(), True),
        StructField("preferred_payment_method", StringType(), True),
        StructField("lifetime_value_tier", StringType(), True),
        StructField("_processed_at", TimestampType(), False),
    ]),
}
# fmt: on


def get_schema(table_name: str) -> StructType:
    """Retrieve the PySpark StructType schema for a given table.

    Args:
        table_name: Table name in the format '{layer}_{entity}'.
                    Examples: 'bronze_pos_transactions', 'silver_customers',
                    'gold_fact_sales'.

    Returns:
        PySpark StructType schema for the table.

    Raises:
        KeyError: If the table name is not registered in the schema registry.

    Example:
        >>> schema = get_schema("silver_customers")
        >>> df = spark.createDataFrame(data, schema=schema)
    """
    if table_name not in SCHEMAS:
        available = sorted(SCHEMAS.keys())
        raise KeyError(
            f"Schema '{table_name}' not found in registry. "
            f"Available schemas: {available}"
        )
    return SCHEMAS[table_name]


def list_schemas() -> list[str]:
    """Return a sorted list of all registered table names.

    Returns:
        Sorted list of table names registered in the schema registry.
    """
    return sorted(SCHEMAS.keys())
