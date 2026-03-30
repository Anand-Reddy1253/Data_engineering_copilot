# METADATA
# owner: data-engineering-team
# layer: silver
# source: lakehouse/bronze/inventory_movements/
# target: lakehouse/silver/inventory_movements/
# description: Clean and transform inventory movements from Bronze to Silver
# created: 2024-01-01

import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, TimestampType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../_shared"))
from delta_utils import upsert_to_delta
from logging_config import get_logger
from spark_session import get_spark

logger = get_logger(__name__)

SOURCE_PATH: str = os.environ.get(
    "SILVER_INV_SOURCE", "lakehouse/bronze/inventory_movements/"
)
TARGET_PATH: str = os.environ.get(
    "SILVER_INV_TARGET", "lakehouse/silver/inventory_movements/"
)
MERGE_KEY: list[str] = ["movement_id"]
VALID_MOVEMENT_TYPES: list[str] = ["IN", "OUT"]


def clean_inventory(
    spark: SparkSession, source_path: str, target_path: str
) -> DataFrame:
    """Clean and transform inventory movements from Bronze to Silver.

    Steps:
        1. Read Bronze Delta table
        2. Deduplicate on movement_id
        3. Filter null movement_ids
        4. Cast types and validate movement_type
        5. Merge to Silver Delta

    Args:
        spark: Active SparkSession.
        source_path: Bronze Delta table path.
        target_path: Silver Delta table path.

    Returns:
        Cleaned DataFrame written to Silver Delta.
    """
    logger.info("Starting Silver clean: inventory_movements")

    bronze_df = spark.read.format("delta").load(source_path)
    raw_count = bronze_df.count()
    logger.info("Read %d rows from Bronze", raw_count)

    deduped_df = bronze_df.dropDuplicates(MERGE_KEY)

    cleaned_df = (
        deduped_df
        .filter(F.col("movement_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("warehouse_id").isNotNull())
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("movement_date", F.to_timestamp(F.col("movement_date")))
        .withColumn("movement_type", F.upper(F.trim(F.col("movement_type"))))
        .filter(F.col("movement_type").isin(VALID_MOVEMENT_TYPES))
        .filter(F.col("quantity") > 0)
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_ingested_at", "_source_file")
    )

    final_count = cleaned_df.count()
    logger.info("After cleaning: %d rows", final_count)

    cleaned_df = cleaned_df.repartition(4)

    upsert_to_delta(
        df=cleaned_df,
        path=target_path,
        merge_key=MERGE_KEY,
    )

    logger.info("Silver clean complete: inventory_movements. Rows: %d", final_count)
    return cleaned_df


def main() -> None:
    """Entry point for Silver inventory cleaning."""
    spark = get_spark("Silver-CleanInventory")
    try:
        clean_inventory(spark, SOURCE_PATH, TARGET_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
