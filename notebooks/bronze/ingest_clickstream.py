# METADATA
# owner: data-engineering-team
# layer: bronze
# source: lakehouse/bronze/raw/clickstream/
# target: lakehouse/bronze/clickstream/
# description: Ingest web clickstream events from raw JSON files to Bronze Delta table
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
    "BRONZE_CLICK_SOURCE", "lakehouse/bronze/raw/clickstream/"
)
TARGET_PATH: str = os.environ.get(
    "BRONZE_CLICK_TARGET", "lakehouse/bronze/clickstream/"
)


def ingest_clickstream(
    spark: SparkSession, source_path: str, target_path: str
) -> DataFrame:
    """Ingest web clickstream events from raw JSON to Bronze Delta table.

    Args:
        spark: Active SparkSession.
        source_path: Path to directory containing raw clickstream JSON files.
        target_path: Target Bronze Delta table path.

    Returns:
        Enriched DataFrame written to Delta.
    """
    logger.info("Starting Bronze ingestion: clickstream from %s", source_path)

    raw_df = (
        spark.read
        .option("multiline", "false")
        .json(source_path)
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
        merge_key=["session_id"],
    )

    logger.info(
        "Bronze ingestion complete: clickstream. Rows written: %d", row_count
    )
    return enriched_df


def main() -> None:
    """Entry point for Bronze clickstream ingestion."""
    spark = get_spark("Bronze-IngestClickstream")
    try:
        ingest_clickstream(spark, SOURCE_PATH, TARGET_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
