# METADATA
# owner: data-engineering-team
# layer: gold
# source: lakehouse/silver/pos_transactions/
# target: lakehouse/gold/dim_product/
# description: Build dim_product dimension from product IDs in Silver transactions
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
    "GOLD_DIM_PROD_SOURCE", "lakehouse/silver/pos_transactions/"
)
TARGET_PATH: str = os.environ.get(
    "GOLD_DIM_PROD_TARGET", "lakehouse/gold/dim_product/"
)
MERGE_KEY: list[str] = ["product_bk"]


def build_dim_product(spark: SparkSession) -> DataFrame:
    """Build dim_product dimension from unique product IDs in Silver transactions.

    Args:
        spark: Active SparkSession.

    Returns:
        Product dimension DataFrame written to Gold Delta.
    """
    logger.info("Building Gold dim_product")

    silver_df = spark.read.format("delta").load(SOURCE_PATH)

    dim_df = (
        silver_df
        .select("product_id")
        .distinct()
        .withColumnRenamed("product_id", "product_bk")
        .withColumn("product_sk", F.monotonically_increasing_id())
        .withColumn("product_name", F.lit(None).cast("string"))
        .withColumn("category", F.lit(None).cast("string"))
        .withColumn("sub_category", F.lit(None).cast("string"))
        .withColumn("unit_cost", F.lit(None).cast("decimal(12,2)"))
        .withColumn("_valid_from", F.current_timestamp())
        .withColumn("_valid_to", F.lit(None).cast("timestamp"))
        .withColumn("_is_current", F.lit(True))
    )

    dim_df = dim_df.repartition(4)

    upsert_to_delta(
        df=dim_df,
        path=TARGET_PATH,
        merge_key=MERGE_KEY,
    )

    logger.info("Gold dim_product built. Rows: %d", dim_df.count())
    return dim_df


def main() -> None:
    """Entry point for Gold dim_product build."""
    spark = get_spark("Gold-BuildDimProduct")
    try:
        build_dim_product(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
