# METADATA
# owner: data-engineering-team
# layer: silver
# source: lakehouse/bronze/clickstream/
# target: lakehouse/silver/clickstream/
# description: Sessionise clickstream events from Bronze to Silver
# created: 2024-01-01

import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../_shared"))
from delta_utils import upsert_to_delta
from logging_config import get_logger
from spark_session import get_spark

logger = get_logger(__name__)

SOURCE_PATH: str = os.environ.get(
    "SILVER_CLICK_SOURCE", "lakehouse/bronze/clickstream/"
)
TARGET_PATH: str = os.environ.get(
    "SILVER_CLICK_TARGET", "lakehouse/silver/clickstream/"
)
MERGE_KEY: list[str] = ["session_id"]
SESSION_TIMEOUT_SECONDS: int = 1800  # 30 minutes


def sessionise_clicks(
    spark: SparkSession, source_path: str, target_path: str
) -> DataFrame:
    """Transform and sessionise clickstream events from Bronze to Silver.

    Computes session start time and duration from event timestamps.

    Args:
        spark: Active SparkSession.
        source_path: Bronze Delta table path.
        target_path: Silver Delta table path.

    Returns:
        Sessionised DataFrame written to Silver Delta.
    """
    logger.info("Starting Silver sessionisation: clickstream")

    bronze_df = spark.read.format("delta").load(source_path)
    raw_count = bronze_df.count()
    logger.info("Read %d raw clickstream events", raw_count)

    deduped_df = bronze_df.dropDuplicates(MERGE_KEY)

    session_window = Window.partitionBy("session_id").orderBy("event_timestamp")

    cleaned_df = (
        deduped_df
        .filter(F.col("session_id").isNotNull())
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("timestamp")),
        )
        .drop("timestamp")
        .withColumn(
            "session_start",
            F.min("event_timestamp").over(session_window),
        )
        .withColumn(
            "session_end",
            F.max("event_timestamp").over(session_window),
        )
        .withColumn(
            "session_duration_seconds",
            F.unix_timestamp(F.col("session_end"))
            - F.unix_timestamp(F.col("session_start")),
        )
        .drop("session_end")
        .withColumn("_processed_at", F.current_timestamp())
        .drop("_ingested_at", "_source_file")
    )

    final_count = cleaned_df.count()
    logger.info("Sessionised %d clickstream rows", final_count)

    cleaned_df = cleaned_df.repartition(4)

    upsert_to_delta(
        df=cleaned_df,
        path=target_path,
        merge_key=MERGE_KEY,
    )

    logger.info("Silver sessionisation complete: clickstream. Rows: %d", final_count)
    return cleaned_df


def main() -> None:
    """Entry point for Silver clickstream sessionisation."""
    spark = get_spark("Silver-SessioniseClicks")
    try:
        sessionise_clicks(spark, SOURCE_PATH, TARGET_PATH)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
