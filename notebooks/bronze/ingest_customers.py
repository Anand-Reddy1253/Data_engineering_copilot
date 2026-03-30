# METADATA
# owner: data-engineering-team
# layer: bronze
# source: lakehouse/bronze/raw/customers/
# target: lakehouse/bronze/customers/
# description: Ingest customer records from raw CSV files to Bronze Delta table
# created: 2024-01-01

import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../_shared"))
from delta_utils import upsert_to_delta
from logging_config import get_logger
from spark_session import get_spark

logger = get_logger(__name__)

SOURCE_PATH: str = os.environ.get(
    "BRONZE_CUST_SOURCE", "lakehouse/bronze/raw/customers/"
)
TARGET_PATH: str = os.environ.get(
    "BRONZE_CUST_TARGET", "lakehouse/bronze/customers/"
)


def ingest_customers(
    spark: SparkSession, source_path: str, target_path: str
) -> DataFrame:
    """Ingest customer data from raw CSV to Bronze Delta table.

    Note: PII columns (name, email, phone, address) are stored raw in
    Bronze. They will be hashed in the Silver layer.

    Args:
        spark: Active SparkSession.
        source_path: Path to directory containing raw customer CSV files.
        target_path: Target Bronze Delta table path.

    Returns:
        Enriched DataFrame written to Delta.
    """
    logger.info("Starting Bronze ingestion: customers from %s", source_path)

    raw_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(source_path)
    )

    row_count = raw_df.count()
    logger.info("Read %d raw rows from %s", row_count, source_path)

    enriched_df = (
        raw_df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    enriched_df = enriched_df.repartition(4)

    upsert_to_delta(
        df=enriched_df,
        path=target_path,
        merge_key=["customer_id"],
    )

    logger.info(
        "Bronze ingestion complete: customers. Rows written: %d", row_count
    )
    return enriched_df


def main() -> None:
    """Entry point for Bronze customers ingestion."""
    spark = get_spark("Bronze-IngestCustomers")
    try:
        ingest_customers(spark, SOURCE_PATH, TARGET_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
