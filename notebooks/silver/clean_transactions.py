# METADATA
# owner: data-engineering-team
# layer: silver
# source: lakehouse/bronze/pos_transactions/
# target: lakehouse/silver/pos_transactions/
# description: Clean and transform POS transactions from Bronze to Silver
# created: 2024-01-01

import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DecimalType, IntegerType, TimestampType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../_shared"))
from delta_utils import upsert_to_delta
from logging_config import get_logger
from spark_session import get_spark

logger = get_logger(__name__)

SOURCE_PATH: str = os.environ.get(
    "SILVER_TXN_SOURCE", "lakehouse/bronze/pos_transactions/"
)
TARGET_PATH: str = os.environ.get(
    "SILVER_TXN_TARGET", "lakehouse/silver/pos_transactions/"
)
MERGE_KEY: list[str] = ["transaction_id"]
VALID_PAYMENT_METHODS: list[str] = ["CASH", "CARD", "DIGITAL", "VOUCHER"]


def clean_transactions(
    spark: SparkSession, source_path: str, target_path: str
) -> DataFrame:
    """Clean and transform POS transactions from Bronze to Silver.

    Steps:
        1. Read Bronze Delta table
        2. Deduplicate on transaction_id
        3. Filter out rows with null transaction_id
        4. Cast types: quantity (int), unit_price (decimal), sale_date (timestamp)
        5. Compute total_amount
        6. Normalise payment_method to uppercase
        7. Merge to Silver Delta

    Args:
        spark: Active SparkSession.
        source_path: Bronze Delta table path.
        target_path: Silver Delta table path.

    Returns:
        Cleaned DataFrame written to Silver Delta.
    """
    logger.info("Starting Silver clean: pos_transactions from %s", source_path)

    bronze_df = spark.read.format("delta").load(source_path)
    raw_count = bronze_df.count()
    logger.info("Read %d rows from Bronze", raw_count)

    deduped_df = bronze_df.dropDuplicates(MERGE_KEY)
    dedup_count = deduped_df.count()
    logger.info(
        "After dedup: %d rows (removed %d duplicates)",
        dedup_count,
        raw_count - dedup_count,
    )

    cleaned_df = (
        deduped_df
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("store_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DecimalType(12, 2)))
        .withColumn("sale_date", F.to_timestamp(F.col("sale_date")))
        .withColumn(
            "total_amount",
            (F.col("quantity").cast(DecimalType(12, 2)) * F.col("unit_price")).cast(
                DecimalType(14, 2)
            ),
        )
        .withColumn(
            "payment_method",
            F.upper(F.trim(F.col("payment_method"))),
        )
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_ingested_at", "_source_file")
    )

    final_count = cleaned_df.count()
    logger.info(
        "After cleaning: %d rows (filtered %d invalid rows)",
        final_count,
        dedup_count - final_count,
    )

    cleaned_df = cleaned_df.repartition(4)

    upsert_to_delta(
        df=cleaned_df,
        path=target_path,
        merge_key=MERGE_KEY,
    )

    logger.info("Silver clean complete: pos_transactions. Rows: %d", final_count)
    return cleaned_df


def main() -> None:
    """Entry point for Silver POS transactions cleaning."""
    spark = get_spark("Silver-CleanTransactions")
    try:
        clean_transactions(spark, SOURCE_PATH, TARGET_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
