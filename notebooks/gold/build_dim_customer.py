# METADATA
# owner: data-engineering-team
# layer: gold
# source: lakehouse/silver/customers/
# target: lakehouse/gold/dim_customer/
# description: Build dim_customer star schema dimension from Silver customers
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
    "GOLD_DIM_CUST_SOURCE", "lakehouse/silver/customers/"
)
TARGET_PATH: str = os.environ.get(
    "GOLD_DIM_CUST_TARGET", "lakehouse/gold/dim_customer/"
)
MERGE_KEY: list[str] = ["customer_bk"]


def build_dim_customer(spark: SparkSession) -> DataFrame:
    """Build the dim_customer dimension table for the star schema.

    Reads from Silver customers, adds surrogate key, SCD Type 2 columns,
    and writes to Gold dim_customer.

    Args:
        spark: Active SparkSession.

    Returns:
        Dimension DataFrame written to Gold Delta.
    """
    logger.info("Building Gold dim_customer from %s", SOURCE_PATH)

    silver_df = spark.read.format("delta").load(SOURCE_PATH)
    source_count = silver_df.count()
    logger.info("Read %d rows from Silver customers", source_count)

    dim_df = (
        silver_df
        .withColumnRenamed("customer_id", "customer_bk")
        .withColumn(
            "customer_sk",
            F.monotonically_increasing_id(),
        )
        .withColumn("_valid_from", F.current_timestamp())
        .withColumn("_valid_to", F.lit(None).cast("timestamp"))
        .withColumn("_is_current", F.lit(True))
        .withColumn("_processed_at", F.current_timestamp())
        .select(
            "customer_sk",
            "customer_bk",
            "name_hash",
            "email_hash",
            "city",
            "country",
            "signup_date",
            "_valid_from",
            "_valid_to",
            "_is_current",
        )
    )

    dim_df = dim_df.repartition(4)

    upsert_to_delta(
        df=dim_df,
        path=TARGET_PATH,
        merge_key=MERGE_KEY,
    )

    logger.info("Gold dim_customer built. Rows: %d", dim_df.count())
    return dim_df


def main() -> None:
    """Entry point for Gold dim_customer build."""
    spark = get_spark("Gold-BuildDimCustomer")
    try:
        build_dim_customer(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
