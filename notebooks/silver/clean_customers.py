# METADATA
# owner: data-engineering-team
# layer: silver
# source: lakehouse/bronze/customers/
# target: lakehouse/silver/customers/
# description: Clean customers data and hash PII columns from Bronze to Silver
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
    "SILVER_CUST_SOURCE", "lakehouse/bronze/customers/"
)
TARGET_PATH: str = os.environ.get(
    "SILVER_CUST_TARGET", "lakehouse/silver/customers/"
)
MERGE_KEY: list[str] = ["customer_id"]


def clean_customers(
    spark: SparkSession, source_path: str, target_path: str
) -> DataFrame:
    """Clean customer data and hash PII columns from Bronze to Silver.

    PII columns (name, email, phone, address) are hashed using SHA-256.
    Original PII columns are dropped before writing to Silver.

    Args:
        spark: Active SparkSession.
        source_path: Bronze Delta table path.
        target_path: Silver Delta table path.

    Returns:
        Cleaned DataFrame with hashed PII written to Silver Delta.
    """
    logger.info("Starting Silver clean: customers")

    bronze_df = spark.read.format("delta").load(source_path)
    raw_count = bronze_df.count()
    logger.info("Read %d rows from Bronze", raw_count)

    deduped_df = bronze_df.dropDuplicates(MERGE_KEY)

    cleaned_df = (
        deduped_df
        .filter(F.col("customer_id").isNotNull())
        # Hash PII columns
        .withColumn("name_hash", F.sha2(F.col("name"), 256))
        .withColumn("email_hash", F.sha2(F.col("email"), 256))
        .withColumn("phone_hash", F.sha2(F.col("phone"), 256))
        .withColumn("address_hash", F.sha2(F.col("address"), 256))
        # Drop raw PII
        .drop("name", "email", "phone", "address")
        .withColumn("signup_date", F.to_timestamp(F.col("signup_date")))
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_ingested_at", "_source_file")
    )

    final_count = cleaned_df.count()
    logger.info("After cleaning: %d customer rows (PII hashed)", final_count)

    cleaned_df = cleaned_df.repartition(4)

    upsert_to_delta(
        df=cleaned_df,
        path=target_path,
        merge_key=MERGE_KEY,
    )

    logger.info("Silver clean complete: customers. Rows: %d", final_count)
    return cleaned_df


def main() -> None:
    """Entry point for Silver customers cleaning."""
    spark = get_spark("Silver-CleanCustomers")
    try:
        clean_customers(spark, SOURCE_PATH, TARGET_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
