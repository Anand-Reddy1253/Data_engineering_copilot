---
applyTo: "pipelines/**/*.yaml"
---
# Pipeline YAML Instructions

## Required Top-Level Fields
Every pipeline YAML file MUST contain these fields:
- `name` — unique pipeline identifier, snake_case
- `description` — human-readable description of what the pipeline does
- `schedule` — cron expression (e.g., `0 6 * * *` for daily at 6am UTC)
- `owner` — team or person responsible (e.g., `data-engineering-team`)
- `steps` — ordered list of pipeline steps

## Step Structure
Each step in the `steps` list MUST have:
- `name` — unique step name within the pipeline, snake_case
- `type` — step type: `notebook`, `sql`, `dq_checkpoint`, `notification`
- `source` — input path or table (for notebook/sql steps)
- `target` — output path or table (for notebook/sql steps)
- `depends_on` — list of step names this step requires (empty list `[]` for first steps)

## Retry Policy
Every step should include a `retry_policy`:
```yaml
retry_policy:
  max_attempts: 3
  backoff_seconds: 60
  retry_on: ["SparkException", "DeltaOptimisticConcurrencyException"]
```

## Timeout
Every step must have `timeout_minutes: 30` (or appropriate value).

## Notifications
Include a `notifications` section:
```yaml
notifications:
  on_failure:
    - type: email
      recipients: ["data-team@contoso.com"]
    - type: teams_webhook
      url_env: TEAMS_WEBHOOK_URL
  on_success: []
```

## Example Pipeline Step
```yaml
- name: bronze_ingest_pos_transactions
  type: notebook
  notebook: notebooks/bronze/ingest_pos_transactions.py
  source: lakehouse/bronze/raw/pos_transactions/
  target: lakehouse/bronze/pos_transactions/
  depends_on: []
  retry_policy:
    max_attempts: 3
    backoff_seconds: 60
  timeout_minutes: 30
```
