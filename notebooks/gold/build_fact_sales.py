# METADATA
# owner: data-engineering-team
# layer: gold
# source: lakehouse/silver/pos_transactions/, lakehouse/gold/dim_customer/, lakehouse/gold/dim_product/
# target: lakehouse/gold/fact_sales/
# description: Build fact_sales from Silver transactions joined with Gold dimensions
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

TXN_PATH: str = os.environ.get(
    "GOLD_FACT_SALES_TXN_SOURCE", "lakehouse/silver/pos_transactions/"
)
DIM_CUSTOMER_PATH: str = os.environ.get(
    "GOLD_DIM_CUST_PATH", "lakehouse/gold/dim_customer/"
)
DIM_PRODUCT_PATH: str = os.environ.get(
    "GOLD_DIM_PROD_PATH", "lakehouse/gold/dim_product/"
)
TARGET_PATH: str = os.environ.get(
    "GOLD_FACT_SALES_TARGET", "lakehouse/gold/fact_sales/"
)
MERGE_KEY: list[str] = ["transaction_id"]


def build_fact_sales(spark: SparkSession) -> DataFrame:
    """Build the fact_sales table by joining transactions with dimensions.

    Args:
        spark: Active SparkSession.

    Returns:
        Fact sales DataFrame written to Gold Delta.
    """
    logger.info("Building Gold fact_sales")

    txn_df = spark.read.format("delta").load(TXN_PATH)
    dim_customer_df = (
        spark.read.format("delta").load(DIM_CUSTOMER_PATH)
        .filter(F.col("_is_current"))
        .select("customer_sk", "customer_bk")
    )
    dim_product_df = (
        spark.read.format("delta").load(DIM_PRODUCT_PATH)
        .filter(F.col("_is_current"))
        .select("product_sk", "product_bk")
    )

    txn_count = txn_df.count()
    logger.info("Source transactions: %d", txn_count)

    fact_df = (
        txn_df
        .join(
            F.broadcast(dim_customer_df),
            txn_df["customer_id"] == dim_customer_df["customer_bk"],
            how="left",
        )
        .join(
            F.broadcast(dim_product_df),
            txn_df["product_id"] == dim_product_df["product_bk"],
            how="left",
        )
        .withColumn(
            "date_sk",
            F.date_format(F.col("sale_date"), "yyyyMMdd").cast("int"),
        )
        .select(
            txn_df["transaction_id"],
            dim_customer_df["customer_sk"],
            dim_product_df["product_sk"],
            txn_df["store_id"].alias("store_sk"),
            F.col("date_sk"),
            txn_df["quantity"],
            txn_df["unit_price"],
            txn_df["total_amount"],
            txn_df["payment_method"],
            F.current_timestamp().alias("_processed_at"),
        )
    )

    fact_df = fact_df.repartition(4)

    upsert_to_delta(
        df=fact_df,
        path=TARGET_PATH,
        merge_key=MERGE_KEY,
    )

    logger.info("Gold fact_sales built. Rows: %d", fact_df.count())
    return fact_df


def main() -> None:
    """Entry point for Gold fact_sales build."""
    spark = get_spark("Gold-BuildFactSales")
    try:
        build_fact_sales(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
