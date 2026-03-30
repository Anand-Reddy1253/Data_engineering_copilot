"""Delta Lake utility functions for the Contoso Fabric Platform.

Provides upsert/merge operations and helper utilities for working
with Delta Lake tables.
"""

from typing import Optional

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def upsert_to_delta(
    df: DataFrame,
    path: str,
    merge_key: list[str],
    spark: Optional[SparkSession] = None,
) -> None:
    """Upsert (merge) a DataFrame into a Delta table at the given path.

    If the Delta table does not exist, creates it. If it exists, performs
    a merge: update matching rows and insert new rows.

    Args:
        df: Source DataFrame to merge into the target Delta table.
        path: File system path to the Delta table.
        merge_key: List of column names to use as the merge condition.
        spark: SparkSession instance. If None, uses the active session.

    Raises:
        ValueError: If merge_key is empty or contains columns not in df.

    Example:
        >>> upsert_to_delta(
        ...     df=cleaned_df,
        ...     path="lakehouse/silver/customers/",
        ...     merge_key=["customer_id"],
        ... )
    """
    if not merge_key:
        raise ValueError("merge_key must contain at least one column name")

    missing_cols = [col for col in merge_key if col not in df.columns]
    if missing_cols:
        raise ValueError(f"merge_key columns not in DataFrame: {missing_cols}")

    active_spark = spark or SparkSession.getActiveSession()
    if active_spark is None:
        raise RuntimeError("No active SparkSession found. Call get_spark() first.")

    if DeltaTable.isDeltaTable(active_spark, path):
        delta_table = DeltaTable.forPath(active_spark, path)
        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in merge_key]
        )
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(path)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Read a Delta table from the given path.

    Args:
        spark: Active SparkSession.
        path: File system path to the Delta table.

    Returns:
        DataFrame loaded from the Delta table.

    Example:
        >>> df = read_delta(spark, "lakehouse/bronze/customers/")
    """
    return spark.read.format("delta").load(path)


def read_delta_version(
    spark: SparkSession,
    path: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
) -> DataFrame:
    """Read a specific version or timestamp of a Delta table (time travel).

    Args:
        spark: Active SparkSession.
        path: File system path to the Delta table.
        version: Version number to read. Mutually exclusive with timestamp.
        timestamp: Timestamp string to read (e.g. "2024-01-15 00:00:00").
                   Mutually exclusive with version.

    Returns:
        DataFrame for the requested version/timestamp.

    Raises:
        ValueError: If both version and timestamp are provided.

    Example:
        >>> df = read_delta_version(spark, "lakehouse/gold/fact_sales/", version=3)
    """
    if version is not None and timestamp is not None:
        raise ValueError("Provide either version or timestamp, not both")

    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    elif timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)

    return reader.load(path)


def optimize_delta(spark: SparkSession, path: str, z_order_cols: Optional[list[str]] = None) -> None:
    """Run OPTIMIZE on a Delta table, optionally with Z-ORDER.

    Args:
        spark: Active SparkSession.
        path: File system path to the Delta table.
        z_order_cols: Columns to Z-ORDER by. If None, runs basic compaction.

    Example:
        >>> optimize_delta(spark, "lakehouse/gold/fact_sales/", ["sale_date", "store_id"])
    """
    delta_table = DeltaTable.forPath(spark, path)
    if z_order_cols:
        delta_table.optimize().executeZOrderBy(*z_order_cols)
    else:
        delta_table.optimize().executeCompaction()


def vacuum_delta(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """Run VACUUM on a Delta table to remove old files.

    Args:
        spark: Active SparkSession.
        path: File system path to the Delta table.
        retention_hours: Hours of history to retain. Minimum 168 (7 days).

    Raises:
        ValueError: If retention_hours is less than 168.

    Example:
        >>> vacuum_delta(spark, "lakehouse/silver/customers/")
    """
    if retention_hours < 168:
        raise ValueError("retention_hours must be >= 168 (7 days) for safety")
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.vacuum(retention_hours)
