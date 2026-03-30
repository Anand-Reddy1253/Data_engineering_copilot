# METADATA
# owner: data-engineering-team
# layer: bronze
# source: lakehouse/bronze/raw/pos_transactions/
# target: lakehouse/bronze/pos_transactions/
# description: Ingest POS transactions from raw CSV files to Bronze Delta table
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
    "BRONZE_POS_SOURCE", "lakehouse/bronze/raw/pos_transactions/"
)
TARGET_PATH: str = os.environ.get(
    "BRONZE_POS_TARGET", "lakehouse/bronze/pos_transactions/"
)


def ingest_pos_transactions(
    spark: SparkSession, source_path: str, target_path: str
) -> DataFrame:
    """Ingest POS transaction data from raw CSV to Bronze Delta table.

    Reads all CSV files from the source path, adds ingestion metadata
    columns (_ingested_at, _source_file), and upserts to the Bronze
    Delta table. No business transformations are applied.

    Args:
        spark: Active SparkSession.
        source_path: Path to directory containing raw POS CSV files.
        target_path: Target Bronze Delta table path.

    Returns:
        Enriched DataFrame that was written to Delta.
    """
    logger.info("Starting Bronze ingestion: pos_transactions from %s", source_path)

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
        merge_key=["transaction_id"],
    )

    logger.info(
        "Bronze ingestion complete: pos_transactions. Rows written: %d", row_count
    )
    return enriched_df


def main() -> None:
    """Entry point for Bronze POS transactions ingestion."""
    spark = get_spark("Bronze-IngestPOSTransactions")
    try:
        ingest_pos_transactions(spark, SOURCE_PATH, TARGET_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
