---
applyTo: "tests/**/*.py"
---
# Testing Instructions

## Framework
- Use `pytest` as the test framework
- Use `chispa` for PySpark DataFrame comparisons:
```python
from chispa.dataframe_comparer import assert_df_equality
assert_df_equality(actual_df, expected_df, ignore_row_order=True)
```

## Test Data
- Create test DataFrames inline in tests — never read from CSV fixtures
- Use `spark.createDataFrame()` with explicit schema from schema registry
- Keep test datasets small (5-20 rows) but cover key scenarios

## Test Coverage Requirements
Every transformation function must be tested for:
- **Happy path** — valid data, expected output
- **Null handling** — nulls in key columns, nullable fields
- **Duplicates** — duplicate records on business key
- **Empty DataFrame** — zero-row input
- **Schema mismatch** — wrong column types (should raise or handle gracefully)
- **Edge cases** — boundary values, special characters, max/min values

## Test Markers
- Mark integration tests with `@pytest.mark.integration`
- Mark slow tests with `@pytest.mark.slow`
- Mark tests requiring Spark with `@pytest.mark.spark`

## Fixtures
- Use the session-scoped `spark` fixture from `conftest.py`
- Use the `tmp_delta_path` fixture for temporary Delta table paths
- Use `make_df()` helper for quick DataFrame creation from dicts

## Test Naming
- Test file: `test_{module_being_tested}.py`
- Test function: `test_{function_name}_{scenario}()`
- Example: `test_upsert_to_delta_inserts_new_records()`
- Example: `test_clean_transactions_removes_duplicates()`

## Assertions
- Use `assert_df_equality()` for DataFrame comparisons (ignores row order by default)
- Use `assert df.count() == expected` for row count checks
- Use `assert set(df.columns) == expected_columns` for schema checks
- Always verify output schema, not just data

## Do Not
- Do not use `df.toPandas()` in tests — use chispa comparisons
- Do not read files in unit tests — create data inline
- Do not use `time.sleep()` — use proper synchronisation
