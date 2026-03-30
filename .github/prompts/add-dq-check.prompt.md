---
agent: dq-auditor
description: Add a Great Expectations data quality check for a table. Generates expectation JSON, registers in checkpoint, and adds DQ step to pipeline YAML.
tools:
  - editFiles
  - search
---

# Add DQ Check Prompt

Add a new data quality check for a table in the Contoso Fabric Platform.

## Inputs Required
Please provide:
1. **Table path** — Delta table path (e.g., `lakehouse/gold/fact_sales/`)
2. **Suite name** — name for the expectation suite (e.g., `gold_fact_sales`)
3. **Check types** — which checks to include:
   - `completeness` — not_null checks on specified columns
   - `uniqueness` — unique value checks on business keys
   - `range` — value range checks on numeric columns
   - `row_count` — minimum and maximum row count
   - `freshness` — check that data was updated recently
4. **Thresholds** — values for range and row count checks
5. **Key columns** — columns to check for not-null and uniqueness

## Steps to Execute

1. **Generate expectation JSON** based on check types and save to `data_quality/expectations/{suite_name}.json`

2. **Register in checkpoint** by adding the suite to `data_quality/checkpoints/default_checkpoint.yaml`

3. **Add DQ step to pipeline** — find the relevant pipeline YAML and add a `dq_checkpoint` step after the table is populated

## Expectation JSON Template
```json
{
  "expectation_suite_name": "{{SUITE_NAME}}",
  "data_asset_type": "Dataset",
  "meta": {
    "great_expectations_version": "0.18.0"
  },
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": { "column": "{{KEY_COLUMN}}" },
      "meta": {}
    }
  ]
}
```
