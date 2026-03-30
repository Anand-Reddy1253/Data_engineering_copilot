"""SparkSession factory for the Contoso Fabric Platform.

Provides a configured local SparkSession with Delta Lake extensions
for use in all Bronze, Silver, and Gold notebooks.
"""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "ContosoPlatform") -> SparkSession:
    """Create or retrieve a local SparkSession with Delta Lake support.

    Args:
        app_name: Name for the Spark application. Defaults to 'ContosoPlatform'.

    Returns:
        Configured SparkSession with Delta Lake extensions enabled.

    Example:
        >>> spark = get_spark()
        >>> df = spark.read.format("delta").load("lakehouse/silver/customers/")
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
