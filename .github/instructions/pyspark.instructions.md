---
applyTo: "**/*.py"
---
# PySpark Coding Instructions

## DataFrame API
- Always use the DataFrame API — avoid raw SQL strings in Python code
- Import `pyspark.sql.functions as F` and use `F.col()`, `F.lit()`, `F.when()`, etc.
- Never use `df["column"]` syntax — always use `F.col("column")`
- Never use `df.column` attribute access — always use `F.col("column")`

## Transformation Chaining
- Chain transformations using method chaining for readability:
```python
result = (
    df
    .filter(F.col("quantity") > 0)
    .withColumn("total_price", F.col("quantity") * F.col("unit_price"))
    .dropDuplicates(["transaction_id"])
    .withColumn("_processed_at", F.current_timestamp())
)
```

## Performance
- Broadcast small lookup DataFrames (< 10MB): `F.broadcast(lookup_df)`
- Repartition before writing: `df.repartition(4)` for small tables, `df.repartition(16)` for large
- Use `cache()` only for DataFrames used multiple times in the same notebook
- Avoid `collect()` except for small result sets (< 1000 rows)

## Type Safety
- Always enforce schemas using `spark.createDataFrame(data, schema=get_schema("table_name"))`
- Cast columns explicitly: `F.col("quantity").cast(IntegerType())`
- Handle nulls explicitly with `F.coalesce()` or `.fillna()`

## PII Handling
- Hash PII columns in Silver layer using SHA-256:
```python
F.sha2(F.col("email"), 256).alias("email_hash")
```
- Drop original PII columns after hashing
- Never log PII values

## Delta Lake
- Always use `upsert_to_delta()` from `delta_utils.py`
- Never use `.mode("overwrite")` for production tables
- Use `.mode("append")` only for event/log tables

## Imports
- Group imports: stdlib → pyspark → project modules
- Never use `from module import *`
