---
name: fabric-notebook-scaffold
description: Scaffold new PySpark notebooks for the Contoso Fabric Platform. Selects the correct template based on the target layer, fills placeholders, registers the schema, and creates test stubs.
---

# Fabric Notebook Scaffold Skill

## When to Use This Skill
Use this skill whenever a new notebook needs to be created for any layer of the medallion
architecture. This skill ensures consistency across all notebooks.

## Steps

### Step 1: Determine Layer
Identify the target layer based on the notebook's purpose:
- **Bronze** — reads raw source files, adds metadata columns, no transforms
- **Silver** — reads Bronze Delta, cleans and transforms data
- **Gold** — reads Silver Delta, builds aggregates and star schema

### Step 2: Select Template
Choose the appropriate template:
- Bronze → `templates/bronze_ingestion.py.tmpl`
- Silver → `templates/silver_transform.py.tmpl`
- Gold → `templates/gold_aggregate.py.tmpl`

### Step 3: Fill Placeholders
Replace these placeholders in the template:
| Placeholder | Description | Example |
|------------|-------------|---------|
| `{{ENTITY}}` | Entity/table name in snake_case | `pos_transactions` |
| `{{ENTITY_UPPER}}` | Entity name in UPPER_CASE | `POS_TRANSACTIONS` |
| `{{SOURCE_PATH}}` | Input data path | `lakehouse/bronze/raw/pos_transactions/` |
| `{{TARGET_PATH}}` | Output Delta table path | `lakehouse/bronze/pos_transactions/` |
| `{{DESCRIPTION}}` | One-line description | `Ingest POS transactions from raw CSV` |
| `{{MERGE_KEY}}` | Business key column(s) | `transaction_id` |
| `{{LAYER}}` | Layer name | `bronze` |

### Step 4: Register Schema
Add the new table's StructType schema to `notebooks/_shared/schema_registry.py` in the
`SCHEMAS` dictionary.

### Step 5: Create Test Stub
Create `tests/unit/test_{entity}.py` with:
- Import statements
- Placeholder test functions for happy path, nulls, duplicates, empty DataFrame
- TODO comments for the test-generator agent to fill in

### Step 6: Validate
Run: `python -m py_compile notebooks/{layer}/{notebook_file}.py`
Fix any syntax errors before reporting completion.

### Step 7: Report
Summarise:
- Files created
- Schema registered (yes/no)
- Test stub created (yes/no)
- Validation result
- Next steps (e.g., "hand off to test-generator to complete tests")
