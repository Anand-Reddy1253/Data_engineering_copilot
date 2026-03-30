---
agent: dq-auditor
description: Read-only review of a notebook for schema compliance, medallion rules, PII handling, idempotency, and test coverage. Outputs structured findings.
tools:
  - search
---

# Review Notebook Prompt

Perform a comprehensive review of a notebook in the Contoso Fabric Platform.

## Inputs Required
Please provide:
1. **Notebook path** — path to the notebook to review

## Review Checklist

### Schema Compliance
- [ ] Table schema registered in `schema_registry.py`
- [ ] All columns explicitly cast to registered types
- [ ] No extra columns added without schema update

### Medallion Architecture Compliance
- [ ] Bronze: only raw ingestion, no business transforms
- [ ] Silver: deduplication, type casting, null handling, PII hashing
- [ ] Gold: business logic only, reads from Silver

### PII Handling
- [ ] PII columns (email, phone, address) are hashed using SHA-256
- [ ] No raw PII in logger statements
- [ ] PII columns not passed to Gold layer

### Idempotency
- [ ] Uses `upsert_to_delta()` not `.mode("overwrite")`
- [ ] Re-running produces the same result
- [ ] No side effects outside the target Delta table

### Test Coverage
- [ ] Unit tests exist in `tests/unit/test_{entity}.py`
- [ ] Tests cover happy path and edge cases

## Output Format
```
## Notebook Review: {notebook_path}
**Reviewed:** {date}
**Layer:** {layer}

### Findings
[CRITICAL] ...
[WARNING] ...
[INFO] ...

### Summary
- Critical: N issues
- Warnings: N issues
- Info: N suggestions
- Recommendation: APPROVE / REQUEST CHANGES
```
