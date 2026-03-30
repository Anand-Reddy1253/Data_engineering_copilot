---
name: pipeline-debugger
description: Debug pipeline failures in the Contoso Fabric Platform. Classifies errors, identifies root cause, and provides targeted fixes.
---

# Pipeline Debugger Skill

## Step 1: Collect Context
Gather information about the failure:
1. Which pipeline and step failed?
2. What is the full error message and stack trace?
3. What are the input and output paths for the failed step?
4. When did the last successful run occur?
5. Were there any schema or data changes before the failure?

## Step 2: Classify Error

### Schema Mismatch
**Symptoms:** `AnalysisException: cannot resolve column`, `incompatible data type`
**Cause:** Source schema changed or notebook column name doesn't match schema registry
**Fix:**
```python
# Check current schema
spark.read.format("delta").load("lakehouse/bronze/pos_transactions/").printSchema()
# Compare with schema_registry.py
from schema_registry import get_schema
get_schema("bronze_pos_transactions").simpleString()
```

### Null Violation
**Symptoms:** `NOT NULL constraint violated`, rows missing after Silver transform
**Cause:** Business key column has null values in source data
**Fix:**
```python
# Find null rows
df.filter(F.col("transaction_id").isNull()).show(20)
# Add null filter in Silver notebook
cleaned_df = df.filter(F.col("transaction_id").isNotNull())
```

### Timeout Error
**Symptoms:** `TimeoutException`, `StageNotComplete after N seconds`
**Cause:** Large data volume, skewed partitions, or insufficient resources
**Fix:**
```python
# Check partition distribution
df.groupBy(F.spark_partition_id()).count().show()
# Repartition to distribute load
df = df.repartition(16, "sale_date")
```

### Dependency Failure
**Symptoms:** Step fails because a dependency step hasn't completed
**Cause:** Missing `depends_on` in pipeline YAML or upstream failure
**Fix:** Check pipeline YAML `depends_on` fields and run upstream steps manually

### Permission Error
**Symptoms:** `AccessDeniedException`, `Permission denied`
**Cause:** Wrong path, missing directory, or file permissions
**Fix:**
```bash
ls -la lakehouse/bronze/
mkdir -p lakehouse/bronze/pos_transactions/
```

## Step 3: Apply Fix
Based on classification, apply the recommended fix and re-run only the failed step.

## Step 4: Prevent Recurrence
- For schema changes: update schema registry and add schema evolution option
- For null violations: add null checks to Bronze and Silver expectations
- For timeouts: add repartition and increase `timeout_minutes` in pipeline YAML
- For permissions: add directory creation to `setup_local_env.sh`
