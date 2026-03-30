---
name: delta-table-ops
description: Delta Lake table operations for the Contoso Fabric Platform. Covers OPTIMIZE, VACUUM, Z-ORDER, schema evolution, and time travel with runnable PySpark code examples.
---

# Delta Table Operations Skill

## OPTIMIZE
Compact small files in a Delta table for better query performance:
```python
from delta.tables import DeltaTable
from spark_session import get_spark

spark = get_spark()

# Optimize compacts small files
delta_table = DeltaTable.forPath(spark, "lakehouse/silver/pos_transactions/")
delta_table.optimize().executeCompaction()
```

## Z-ORDER
Co-locate related data to improve query performance on high-cardinality columns:
```python
# Z-ORDER on columns frequently used in filters/joins
delta_table.optimize().executeZOrderBy("sale_date", "store_id")
```

## VACUUM
Remove old files no longer needed (default retention: 7 days):
```python
# Default: removes files older than 7 days
delta_table.vacuum()

# Custom retention (minimum 168 hours for safety):
delta_table.vacuum(168)  # 7 days in hours
```

**Warning:** Do NOT set retention below 7 days (168 hours) in production.

## Schema Evolution
Allow Delta to automatically handle new columns:
```python
# Add new column without breaking existing reads
df_with_new_col = existing_df.withColumn("new_column", F.lit(None).cast("string"))

# Write with schema evolution enabled
df_with_new_col.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("lakehouse/silver/customers/")
```

## Time Travel
Query historical versions of a Delta table:
```python
# Read by version number
historical_df = spark.read \
    .format("delta") \
    .option("versionAsOf", 3) \
    .load("lakehouse/gold/fact_sales/")

# Read by timestamp
historical_df = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-15 00:00:00") \
    .load("lakehouse/gold/fact_sales/")

# Show table history
delta_table.history().show(10, truncate=False)
```

## Merge / Upsert Pattern
The standard upsert pattern used throughout this platform:
```python
from delta.tables import DeltaTable
import pyspark.sql.functions as F

def upsert_to_delta(df, path: str, merge_key: list[str]) -> None:
    if DeltaTable.isDeltaTable(spark, path):
        delta_table = DeltaTable.forPath(spark, path)
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
```

## Delta Table Properties
Set important table properties:
```python
spark.sql("""
    ALTER TABLE delta.`lakehouse/gold/fact_sales`
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.deletedFileRetentionDuration' = 'interval 7 days'
    )
""")
```
