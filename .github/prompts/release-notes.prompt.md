---
agent: docs-writer
description: Generate release notes from git log since the last tag. Groups changes by category and formats as Markdown.
tools:
  - runTerminal
  - editFiles
  - search
---

# Release Notes Prompt

Generate release notes for the latest version of the Contoso Fabric Platform.

## Steps to Execute

1. **Get last tag**:
```bash
git describe --tags --abbrev=0
```

2. **Get commits since last tag**:
```bash
git log {last_tag}..HEAD --oneline --no-merges
```

3. **Categorise commits** by type:
   - **New Tables** — commits mentioning new Delta tables, schemas, or notebooks
   - **Pipeline Changes** — commits to `pipelines/` directory
   - **Bug Fixes** — commits starting with `fix:` or mentioning bug/error/issue
   - **Data Quality** — commits to `data_quality/` or mentioning expectations/checkpoints
   - **Documentation** — commits to `docs/` or updating README/data dictionary
   - **Infrastructure** — commits to `pyproject.toml`, `requirements.txt`, workflows

4. **Format as Markdown**:
```markdown
# Release Notes — v{version}
**Released:** {date}

## New Tables
- Added `gold.fact_sales` — consolidated sales facts from POS and inventory

## Pipeline Changes
- Updated `daily_full_refresh.yaml` to include new DQ checkpoints

## Bug Fixes
- Fixed null handling in `clean_transactions.py` for missing product_ids

## Data Quality
- Added completeness expectations for `gold.agg_customer_360`

## Documentation
- Updated data dictionary with `fact_inventory_snapshot` schema
```

5. **Save** to `docs/releases/v{version}.md`
6. **Append summary** to top of `CHANGELOG.md`
