---
agent: pipeline-architect
description: Generate a new pipeline YAML file for the Contoso Fabric Platform. Validates against JSON schema.
tools:
  - editFiles
  - runTerminal
  - search
---

# Generate Pipeline Prompt

Create a new pipeline orchestration file for the Contoso Fabric Platform.

## Inputs Required
Please provide:
1. **Pipeline name** — snake_case identifier (e.g., `daily_full_refresh`)
2. **Schedule** — cron expression (e.g., `0 6 * * *`)
3. **Notebook list** — ordered list of notebooks to run (with layer prefix)
4. **Failure handling** — what to do when a step fails (retry, skip, abort, notify)
5. **Owner** — team responsible for this pipeline

## Steps to Execute

1. **Design DAG** — determine which steps can run in parallel and which have dependencies

2. **Generate YAML** following the template in `.github/instructions/pipeline-yaml.instructions.md`

3. **Add DQ checkpoints** between layers (after Bronze, after Silver, after Gold)

4. **Add notifications** for failure conditions

5. **Validate against schema**:
```bash
python -c "
import yaml, json, jsonschema
with open('pipelines/schemas/pipeline_schema.json') as f:
    schema = json.load(f)
with open('pipelines/{pipeline_name}.yaml') as f:
    pipeline = yaml.safe_load(f)
jsonschema.validate(pipeline, schema)
print('Pipeline YAML is valid!')
"
```

6. **Save** to `pipelines/{pipeline_name}.yaml`

## Output
A valid pipeline YAML file with all steps, dependencies, retry policies, and notifications configured.
