---
applyTo: "**/*.sql"
---
# SQL Coding Instructions

## Query Structure
- Always use CTEs instead of nested subqueries
- Every CTE must have a descriptive name explaining its purpose
- Final SELECT should reference CTE names only

```sql
WITH
source_data AS (
    SELECT ...
    FROM raw_table
),
filtered AS (
    SELECT ...
    FROM source_data
    WHERE condition
),
aggregated AS (
    SELECT ...
    FROM filtered
    GROUP BY ...
)
SELECT * FROM aggregated;
```

## Column References
- Always qualify column references with table aliases
- Use short, meaningful aliases (t for transactions, c for customers, etc.)
- Never use SELECT * in production queries — always list columns explicitly

## SCD Type 2 Columns
All slowly-changing dimension tables must include:
- `_valid_from TIMESTAMP NOT NULL` — when this version became active
- `_valid_to TIMESTAMP` — when this version was superseded (NULL = current)
- `_is_current BOOLEAN NOT NULL DEFAULT TRUE` — convenience flag

## Keys
- Surrogate keys: `{table}_sk` — e.g., `customer_sk`, `product_sk`
- Business keys: `{entity}_bk` — e.g., `customer_bk`, `product_bk`
- Surrogate keys are BIGINT IDENTITY or SEQUENCE-generated — never reuse
- Business keys are the natural keys from source systems

## Formatting
- Keywords in UPPERCASE: SELECT, FROM, WHERE, GROUP BY, ORDER BY
- Indent continuation lines by 4 spaces
- One column per line in SELECT lists
- Comments: `-- Single line` or `/* Multi-line */`

## Naming
- Table names: snake_case (e.g., `dim_customer`, `fact_sales`)
- Column names: snake_case (e.g., `customer_id`, `sale_date`)
- Index names: `idx_{table}_{column(s)}`
- Constraint names: `pk_{table}`, `fk_{table}_{ref_table}`, `uq_{table}_{column}`
