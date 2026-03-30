# Contoso Fabric Platform — Copilot Instructions

## Project Overview
This is the Contoso Fabric Platform, a production-grade Data Engineering project simulating a Microsoft Fabric
environment using the medallion architecture (Bronze → Silver → Gold) with PySpark and Delta Lake.

## Technology Stack
- Python 3.11+
- PySpark 3.5+
- Delta Lake 3.x
- Great Expectations for data quality
- pytest + chispa for testing

## Python Coding Standards
- Use Python 3.11+ syntax and type hints on ALL functions
- Use Google-style docstrings on all functions and classes
- Never use `import *` — always use explicit imports
- Column names must be in snake_case
- Table names must be in snake_case
- All PySpark code uses the DataFrame API via `pyspark.sql.functions as F`
- Use `F.col("column_name")` not `df["column_name"]`
- Chain DataFrame transformations for readability
- Broadcast small lookup DataFrames using `F.broadcast()`
- Repartition DataFrames before writing to Delta

## Medallion Architecture Rules

### Bronze Layer
- Raw ingestion ONLY — no business transformations
- Add `_ingested_at` column: `F.current_timestamp()`
- Add `_source_file` column: the source file path as a string literal
- Write to: `lakehouse/bronze/{entity}/`
- Schema: permissive — accept whatever comes from source

### Silver Layer
- Deduplicate on business key
- Cast all columns to correct types per schema registry
- Handle nulls: fill defaults or filter rows with null business keys
- Hash PII columns (email, phone, address) using SHA-256: `F.sha2(F.col("email"), 256)`
- Write to: `lakehouse/silver/{entity}/`
- Use Delta merge/upsert on business key — never full overwrite

### Gold Layer
- Business aggregations and star schema tables only
- Join dimensions, aggregate metrics
- Write to: `lakehouse/gold/{entity}/`
- Use Delta merge/upsert — never full overwrite

## Delta Lake Standards
- ALL writes use `upsert_to_delta()` from `notebooks/_shared/delta_utils.py`
- Never use `df.write.format("delta").mode("overwrite")`
- Always merge/upsert using business key

## Credentials & Security
- NEVER hardcode credentials, passwords, API keys, or tokens
- Always use environment variables: `os.environ.get("VAR_NAME")`
- PII columns (email, phone, address, name) must be hashed in Silver layer

## Notebook Standards
Every notebook MUST start with this metadata comment block:
```python
# METADATA
# owner: data-engineering-team
# layer: bronze|silver|gold
# source: <source table/path>
# target: <target table/path>
# description: <brief description>
# created: YYYY-MM-DD
```

## Schema Registry
- Always import `schema_registry.py` and use `get_schema(table_name)` for schema enforcement
- Schema registry is the single source of truth for all table schemas

## Logging
- Always import `logging_config.py` and use the configured logger
- Log at key milestones: start, row counts, merge stats, end
- Never log PII data
