---
agent: test-generator
description: Generate pytest tests for a source file. Covers happy path, edge cases, nulls, duplicates, and empty DataFrames. Runs tests and fixes failures.
tools:
  - editFiles
  - runTerminal
  - search
---

# Write Tests Prompt

Generate pytest tests for a given source file in the Contoso Fabric Platform.

## Inputs Required
Please provide:
1. **Source file** — path to the file to test (e.g., `notebooks/_shared/delta_utils.py`)
2. **Test type** — unit or integration
3. **Specific functions** — list of functions to test (or "all")

## Steps to Execute

1. **Read source file** to understand the functions and their signatures

2. **Generate tests** for each function covering:
   - Happy path with valid data
   - Null values in key columns
   - Duplicate records on business key
   - Empty DataFrame input
   - Schema edge cases

3. **Save test file** to `tests/unit/test_{module}.py` or `tests/integration/test_{module}.py`

4. **Run tests**:
```bash
pytest tests/unit/test_{module}.py -v
```

5. **Fix failures** — if tests fail, diagnose and fix either the test or the source code

6. **Iterate** up to 3 times on failures, then report remaining issues

## Output
- Test file with all scenarios covered
- Test run summary (N passed, N failed)
- Any remaining issues that need manual attention
