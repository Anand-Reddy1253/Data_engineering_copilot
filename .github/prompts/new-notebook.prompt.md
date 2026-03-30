---
agent: data-engineer
description: Scaffold a new PySpark notebook for the given layer and entity using the fabric-notebook-scaffold skill.
tools:
  - editFiles
  - runTerminal
  - search
---

# New Notebook Prompt

Scaffold a new notebook for the Contoso Fabric Platform.

## Inputs Required
Please provide:
1. **Layer** — bronze, silver, or gold
2. **Entity** — the table/entity name (e.g., pos_transactions, customers)
3. **Source path** — where the notebook reads data from
4. **Target path** — where the notebook writes data to
5. **Description** — brief description of what this notebook does

## Steps to Execute

1. **Select template** based on layer:
   - Bronze → `.github/skills/fabric-notebook-scaffold/templates/bronze_ingestion.py.tmpl`
   - Silver → `.github/skills/fabric-notebook-scaffold/templates/silver_transform.py.tmpl`
   - Gold → `.github/skills/fabric-notebook-scaffold/templates/gold_aggregate.py.tmpl`

2. **Fill placeholders** in the template:
   - `{{ENTITY}}` → entity name
   - `{{SOURCE_PATH}}` → source path
   - `{{TARGET_PATH}}` → target path
   - `{{DESCRIPTION}}` → description

3. **Register schema** in `notebooks/_shared/schema_registry.py` if not already present

4. **Create test stub** in `tests/unit/test_{entity}.py` with TODO markers for the test-generator agent

5. **Create DQ expectation** in `data_quality/expectations/{layer}_{entity}.json`

6. **Validate** the notebook is syntactically valid: `python -m py_compile notebooks/{layer}/{notebook_name}.py`

7. **Report** what was created and any next steps

## Output
Create the notebook at `notebooks/{layer}/{notebook_name}.py` where `notebook_name` follows the pattern:
- Bronze: `ingest_{entity}.py`
- Silver: `clean_{entity}.py`
- Gold: `build_{entity}.py`
