---
agent: docs-writer
description: Generate a data dictionary entry for a table. Reads schema from registry or Delta table metadata.
tools:
  - editFiles
  - search
  - runTerminal
---

# Document Table Prompt

Generate a data dictionary entry for a table in the Contoso Fabric Platform.

## Inputs Required
Please provide:
1. **Table name** — e.g., `fact_sales`, `dim_customer`
2. **Layer** — bronze, silver, or gold

## Steps to Execute

1. **Read schema** from `notebooks/_shared/schema_registry.py`:
```python
from schema_registry import get_schema
schema = get_schema("fact_sales")
```

2. **Read lineage** by searching the notebooks that produce this table

3. **Generate data dictionary entry** in the format below

4. **Append to** `docs/data_dictionary.md` in the appropriate section

5. **Update lineage diagram** in `docs/architecture.md` if needed

## Output Format
```markdown
### `{layer}.{table_name}`
**Layer:** {Bronze | Silver | Gold}
**Source:** {source tables}
**Owner:** data-engineering-team
**Notebook:** `notebooks/{layer}/{notebook_file}.py`
**Description:** {business description}

| Column | Type | Nullable | Description | Source |
|--------|------|----------|-------------|--------|
| column_name | STRING | No | Description | source_column |

**Business Rules:**
- Rule 1
- Rule 2

**Lineage:**
\`\`\`mermaid
flowchart LR
    src[Source] --> tgt[{table_name}]
\`\`\`
```
