---
name: data-quality-framework
description: Create and manage Great Expectations data quality suites for the Contoso Fabric Platform. Covers rule templates, expectation JSON generation, checkpoint registration, and running checks.
---

# Data Quality Framework Skill

## Overview
This platform uses Great Expectations (GE) for data quality checks. Each table has an
expectation suite (JSON) and all suites are run via a shared checkpoint.

## Step 1: Select Rule Templates

### Completeness — not-null checks
```json
{
    "expectation_type": "expect_column_values_to_not_be_null",
    "kwargs": { "column": "customer_id" },
    "meta": { "rule_type": "completeness" }
}
```

### Uniqueness — business key check
```json
{
    "expectation_type": "expect_column_values_to_be_unique",
    "kwargs": { "column": "transaction_id" },
    "meta": { "rule_type": "uniqueness" }
}
```

### Freshness — recent data check
```json
{
    "expectation_type": "expect_column_max_to_be_between",
    "kwargs": {
        "column": "sale_date",
        "min_value": "2024-01-01",
        "max_value": null
    },
    "meta": { "rule_type": "freshness" }
}
```

### Referential Integrity — values in set
```json
{
    "expectation_type": "expect_column_values_to_be_in_set",
    "kwargs": {
        "column": "payment_method",
        "value_set": ["CASH", "CARD", "DIGITAL"]
    },
    "meta": { "rule_type": "referential_integrity" }
}
```

### Range — numeric bounds
```json
{
    "expectation_type": "expect_column_values_to_be_between",
    "kwargs": {
        "column": "quantity",
        "min_value": 1,
        "max_value": 10000
    },
    "meta": { "rule_type": "range" }
}
```

### Row Count
```json
{
    "expectation_type": "expect_table_row_count_to_be_between",
    "kwargs": {
        "min_value": 1,
        "max_value": 10000000
    },
    "meta": { "rule_type": "row_count" }
}
```

## Step 2: Generate Expectation JSON
Save to `data_quality/expectations/{suite_name}.json`:
```json
{
    "expectation_suite_name": "gold_fact_sales",
    "data_asset_type": "Dataset",
    "meta": { "great_expectations_version": "0.18.0" },
    "expectations": [
        { ... }
    ]
}
```

## Step 3: Register Checkpoint
Add the suite to `data_quality/checkpoints/default_checkpoint.yaml`:
```yaml
expectation_suite_names:
  - bronze_pos_transactions
  - silver_clean_transactions
  - gold_fact_sales    # Add new suite here
```

## Step 4: Run Checks
```bash
make dq
# Or directly:
python -m great_expectations checkpoint run default_checkpoint
```
