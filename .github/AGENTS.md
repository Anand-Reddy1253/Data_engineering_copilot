# AGENTS.md — Contoso Fabric Platform

## What Is This Project?

Contoso Fabric Platform is a production-grade Data Engineering project simulating a Microsoft Fabric
environment locally. It uses the **medallion architecture** (Bronze → Silver → Gold) with PySpark,
Delta Lake, and Great Expectations. The platform processes retail data for Contoso, a fictional retail
chain.

## Data Sources
1. **POS Transactions** — store_id, product_id, customer_id, quantity, unit_price, sale_date, payment_method
2. **Inventory Movements** — warehouse_id, product_id, movement_type (IN/OUT), quantity, movement_date
3. **Customers** — customer_id, name, email, phone, address, city, country, signup_date
4. **Web Clickstream** — session_id, customer_id, page_url, event_type, timestamp, device_type

## Gold Outputs
- Star schema: `dim_customer`, `dim_product`, `dim_store`, `dim_date`, `fact_sales`, `fact_inventory_snapshot`
- `agg_customer_360` — customer lifetime value, purchase frequency, preferred categories
- Data quality scorecards per pipeline run

## Key Directories

| Directory | Purpose |
|-----------|---------|
| `notebooks/bronze/` | Raw ingestion notebooks — read source, add metadata columns, write Delta |
| `notebooks/silver/` | Cleaning notebooks — dedup, type cast, PII hash, merge to Delta |
| `notebooks/gold/` | Aggregation notebooks — star schema and analytics tables |
| `notebooks/_shared/` | Shared modules: SparkSession, Delta utils, schema registry, logging |
| `pipelines/` | Pipeline orchestration YAML files |
| `data_quality/` | Great Expectations suites and checkpoints |
| `tests/` | pytest unit and integration tests |
| `warehouse/schemas/` | SQL DDL for DuckDB warehouse |
| `connections/` | Source connection configs (no credentials) |
| `docs/` | Architecture, data dictionary, feature guides |
| `.github/` | Copilot customisation: instructions, agents, prompts, skills, hooks |

## How to Build, Test, and Run

```bash
make setup          # Create venv, install dependencies, scaffold lakehouse dirs
make seed           # Generate sample data (1K rows) in lakehouse/bronze/raw/
make lint           # Run ruff + mypy
make test           # Run unit tests
make dq             # Run Great Expectations data quality checks
make run-bronze     # Run all Bronze notebooks
make run-silver     # Run all Silver notebooks
make run-gold       # Run all Gold notebooks
make run-all        # Run full Bronze → Silver → Gold pipeline
make clean          # Remove __pycache__, .venv, lakehouse data
```

## Coding Agent Rules

When the GitHub Copilot Coding Agent works in this repository, it MUST:

1. **Always lint before committing** — run `make lint` and fix all issues
2. **Always test before opening a PR** — run `make test` and ensure all tests pass
3. **Update data dictionary** when adding new tables or changing schemas
4. **Register schemas** in `schema_registry.py` for any new tables
5. **Follow medallion rules** — Bronze ingests only, Silver cleans and hashes PII, Gold aggregates
6. **Never hardcode credentials** — use environment variables only
7. **Use `upsert_to_delta()`** from `delta_utils.py` — never use `.mode("overwrite")`
8. **Add DQ expectations** for any new Gold tables
9. **Write tests** for all new transformation logic
10. **Add notebook metadata comment block** at the top of every notebook

## Available Custom Agents

- **data-engineer** — Primary: writes notebooks, Delta ops, schemas, pipelines
- **dq-auditor** — Read-only auditor: checks schema compliance and DQ coverage
- **pipeline-architect** — Designs and validates pipeline DAGs
- **test-generator** — Creates pytest tests and iterates on failures
- **security-reviewer** — Read-only: checks for secrets, PII in logs, vulnerabilities
- **docs-writer** — Generates data dictionary entries and architecture docs
