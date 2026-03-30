"""Shared pytest fixtures for the Contoso Fabric Platform test suite."""

import os
import sys
from typing import Generator

import pytest
from pyspark.sql import SparkSession

# Add shared modules to path
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "../notebooks/_shared"),
)


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Session-scoped SparkSession fixture with Delta Lake support.

    Yields:
        Configured SparkSession for testing.
    """
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("ContosoPlatformTests")
        .master("local[2]")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config(
            "spark.databricks.delta.retentionDurationCheck.enabled", "false"
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def tmp_delta_path(tmp_path: "os.PathLike[str]") -> str:
    """Fixture providing a temporary directory path for Delta tables.

    Args:
        tmp_path: pytest built-in temporary directory.

    Returns:
        String path to a temporary Delta table directory.
    """
    delta_dir = os.path.join(str(tmp_path), "delta_table")
    os.makedirs(delta_dir, exist_ok=True)
    return delta_dir


def make_df(spark: SparkSession, data: list[dict], schema=None):
    """Helper to create a DataFrame from a list of dicts.

    Args:
        spark: Active SparkSession.
        data: List of dicts where keys are column names.
        schema: Optional PySpark StructType schema.

    Returns:
        DataFrame created from the provided data.
    """
    from pyspark.sql import Row

    rows = [Row(**row) for row in data]
    if schema:
        return spark.createDataFrame(rows, schema=schema)
    return spark.createDataFrame(rows)
