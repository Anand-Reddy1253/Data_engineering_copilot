# METADATA
# owner: data-engineering-team
# layer: gold
# source: lakehouse/silver/pos_transactions/, lakehouse/silver/customers/
# target: lakehouse/gold/agg_customer_360/
# description: Build agg_customer_360 with lifetime value, purchase frequency, preferred categories
# created: 2024-01-01

import os
import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../_shared"))
from delta_utils import upsert_to_delta
from logging_config import get_logger
from spark_session import get_spark

logger = get_logger(__name__)

TXN_PATH: str = os.environ.get(
    "GOLD_C360_TXN_SOURCE", "lakehouse/silver/pos_transactions/"
)
TARGET_PATH: str = os.environ.get(
    "GOLD_C360_TARGET", "lakehouse/gold/agg_customer_360/"
)
MERGE_KEY: list[str] = ["customer_bk"]


def build_customer_360(spark: SparkSession) -> DataFrame:
    """Build the agg_customer_360 aggregate table.

    Computes per-customer: total orders, total spend, average order value,
    first/last purchase dates, preferred payment method, and lifetime value tier.

    Args:
        spark: Active SparkSession.

    Returns:
        Customer 360 aggregate DataFrame written to Gold Delta.
    """
    logger.info("Building Gold agg_customer_360")

    txn_df = spark.read.format("delta").load(TXN_PATH)
    txn_count = txn_df.count()
    logger.info("Source transactions: %d", txn_count)

    agg_df = (
        txn_df
        .filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("total_orders"),
            F.sum("total_amount").cast(DecimalType(16, 2)).alias("total_spend"),
            F.avg("total_amount").cast(DecimalType(12, 2)).alias("avg_order_value"),
            F.min("sale_date").alias("first_purchase_date"),
            F.max("sale_date").alias("last_purchase_date"),
        )
        .withColumnRenamed("customer_id", "customer_bk")
    )

    # Determine preferred payment method (most frequent per customer)
    payment_counts = (
        txn_df
        .filter(F.col("customer_id").isNotNull())
        .groupBy("customer_id", "payment_method")
        .count()
    )
    rank_window = Window.partitionBy("customer_id").orderBy(F.col("count").desc())
    preferred_payment = (
        payment_counts
        .withColumn("rank", F.rank().over(rank_window))
        .filter(F.col("rank") == 1)
        .select(
            F.col("customer_id").alias("customer_bk_pay"),
            F.col("payment_method").alias("preferred_payment_method"),
        )
    )

    agg_df = (
        agg_df
        .join(
            F.broadcast(preferred_payment),
            agg_df["customer_bk"] == preferred_payment["customer_bk_pay"],
            how="left",
        )
        .drop("customer_bk_pay")
        .withColumn(
            "lifetime_value_tier",
            F.when(F.col("total_spend") >= 10000, "PLATINUM")
            .when(F.col("total_spend") >= 5000, "GOLD")
            .when(F.col("total_spend") >= 1000, "SILVER")
            .otherwise("BRONZE"),
        )
        .withColumn("_processed_at", F.current_timestamp())
    )

    agg_df = agg_df.repartition(4)

    upsert_to_delta(
        df=agg_df,
        path=TARGET_PATH,
        merge_key=MERGE_KEY,
    )

    logger.info("Gold agg_customer_360 built. Rows: %d", agg_df.count())
    return agg_df


def main() -> None:
    """Entry point for Gold customer 360 build."""
    spark = get_spark("Gold-BuildCustomer360")
    try:
        build_customer_360(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
