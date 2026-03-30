# GitHub Copilot Features Guide — Contoso Fabric Platform

This guide walks through every GitHub Copilot feature used in this project.

## 1. Custom Instructions (`.github/copilot-instructions.md`)

**What they are:** A Markdown file that sets project-wide coding rules for Copilot.
**Where they live:** `.github/copilot-instructions.md`
**How they work:** Copilot reads this file and applies the rules to all suggestions in this repository.

This project's instructions enforce:
- Python 3.11+, PySpark 3.5+, Delta Lake 3.x
- Type hints and Google-style docstrings
- Medallion architecture rules (Bronze → Silver → Gold)
- No hardcoded credentials
- All writes via `upsert_to_delta()`

## 2. AGENTS.md (`.github/AGENTS.md`)

**What it is:** A repository-level overview file always included in the agent's context.
**Where it lives:** `.github/AGENTS.md`
**How it works:** When any Copilot agent runs in this repo, it reads AGENTS.md first to understand
the project structure, commands, and rules.

This project's AGENTS.md includes:
- Business context (Contoso retail)
- Key directory map
- `make setup`, `make seed`, `make test` commands
- Rules for the Coding Agent

## 3. File-Scoped Instructions (`.github/instructions/`)

**What they are:** Markdown files with an `applyTo` frontmatter field that Copilot uses to apply
targeted instructions only when working on matching files.
**Where they live:** `.github/instructions/*.instructions.md`
**How they activate:** When Copilot opens a file matching the `applyTo` glob pattern.

| File | applyTo | Purpose |
|------|---------|---------|
| `pyspark.instructions.md` | `**/*.py` | DataFrame API, F.col(), chaining |
| `sql.instructions.md` | `**/*.sql` | CTEs, SCD2 columns, surrogate keys |
| `pipeline-yaml.instructions.md` | `pipelines/**/*.yaml` | Required fields, retry policy |
| `testing.instructions.md` | `tests/**/*.py` | chispa, inline data, test markers |

## 4. Prompt Files (`.github/prompts/`)

**What they are:** Reusable prompt templates that agents execute with `/command` syntax.
**Where they live:** `.github/prompts/*.prompt.md`
**How to invoke:** In the chat: `/new-notebook`, `/add-dq-check`, `/generate-pipeline`, etc.

| Prompt | Command | What it does |
|--------|---------|-------------|
| `new-notebook.prompt.md` | `/new-notebook` | Scaffolds a new Bronze/Silver/Gold notebook |
| `add-dq-check.prompt.md` | `/add-dq-check` | Adds Great Expectations DQ suite |
| `generate-pipeline.prompt.md` | `/generate-pipeline` | Creates pipeline YAML with validation |
| `write-tests.prompt.md` | `/write-tests` | Generates pytest tests for a module |
| `review-notebook.prompt.md` | `/review-notebook` | Audits a notebook for compliance |
| `document-table.prompt.md` | `/document-table` | Generates data dictionary entry |
| `release-notes.prompt.md` | `/release-notes` | Creates release notes from git log |

## 5. Custom Agents (`.github/agents/`)

**What they are:** Specialised AI agents with defined personas, tools, and handoff rules.
**Where they live:** `.github/agents/*.agent.md`
**How to select:** In Copilot chat, click the agent selector or use `@agent-name`.

| Agent | Role | Tools |
|-------|------|-------|
| `data-engineer` | Primary: writes notebooks and pipelines | editFiles, runTerminal, search |
| `dq-auditor` | Read-only DQ compliance auditor | search only |
| `pipeline-architect` | Designs pipeline DAGs | editFiles, runTerminal, search |
| `test-generator` | Creates and runs pytest tests | editFiles, runTerminal, search |
| `security-reviewer` | Read-only security scanner | search only |
| `docs-writer` | Writes data dictionary and architecture docs | editFiles, search, runTerminal |

## 6. Sub-Agents and Handoffs

The `data-engineer` agent delegates to other agents via handoffs:
1. After writing a notebook → hands off to **test-generator** to create tests
2. After testing → hands off to **dq-auditor** to validate medallion compliance
3. After validation → hands off to **docs-writer** to update data dictionary

Handoffs are defined in the agent YAML frontmatter:
```yaml
handoffs:
  - test-generator
  - dq-auditor
  - docs-writer
```

## 7. Skills (`.github/skills/`)

**What they are:** Structured multi-step procedures that agents follow for complex tasks.
**Where they live:** `.github/skills/{skill-name}/SKILL.md`
**How they're discovered:** Copilot scans `.github/skills/` automatically.

| Skill | When to use |
|-------|------------|
| `fabric-notebook-scaffold` | Creating new notebooks from templates |
| `delta-table-ops` | OPTIMIZE, VACUUM, time travel operations |
| `data-quality-framework` | Creating GE expectation suites |
| `pipeline-debugger` | Diagnosing and fixing pipeline failures |

## 8. Hooks (`.github/hooks/`)

**What they are:** Lifecycle event handlers that run before/after tool use.
**Where they live:** `.github/hooks/*.hooks.json`
**Lifecycle events:**
- `sessionStart` / `sessionEnd` — log session boundaries
- `preToolUse` — can deny tool use (returns `{"decision": "deny"}`)
- `postToolUse` — runs after every file edit
- `agentStop` — runs before agent commits work
- `errorOccurred` — logs errors

This project's hooks:
- `quality-gate.hooks.json` — runs ruff after edits, runs `make lint && make test` before commit
- `security-scan.hooks.json` — blocks commits with hardcoded secrets, scans for PII in logs

## 9. MCP Servers (`.github/mcp/mcp.json`)

**What they are:** Model Context Protocol servers that give agents access to live data.
**Where they live:** `.github/mcp/mcp.json`
**How they work:** Agents query MCP servers to get real-time context.

| Server | Type | Purpose |
|--------|------|---------|
| `github` | URL | Access issues, PRs, workflows |
| `filesystem` | stdio (npx) | Read/write local files |
| `duckdb-warehouse` | stdio (python) | Query DuckDB for schema inspection |

## 10. Coding Agent (assign to @copilot)

To create a PR automatically from an issue:
1. Open a GitHub Issue describing what to build
2. In the issue, comment: `@copilot please implement this`
3. Copilot creates a branch, writes code, and opens a PR
4. Review and merge the PR

This project is pre-configured for the Coding Agent via `AGENTS.md`.

## 11. Copilot CLI

Run agents from the terminal:
```bash
gh copilot suggest "How do I upsert to a Delta table?"
gh copilot explain "what does the silver layer do?"
```

## 12. Built-in Generation Commands

| Command | What it creates |
|---------|----------------|
| `/init` | Initialises Copilot for the repo (AGENTS.md + instructions) |
| `/create-prompt` | Creates a new prompt file interactively |
| `/create-agent` | Creates a new agent profile |
| `/create-skill` | Creates a new skill directory |
| `/create-hook` | Creates a new hook configuration |

## 13. Agentic Code Review

Mention `@copilot` on a PR to trigger an automated code review:
```
@copilot please review this PR for medallion architecture compliance and PII handling
```

The `dq-auditor` agent persona is optimised for PR reviews.

## 14. Copilot Memory

Copilot learns repository-level patterns over time:
- Preferred patterns (upsert_to_delta, F.col() usage)
- Common schemas and table names
- Test data creation patterns
- Pipeline YAML structure

Memory is stored per-repository and improves suggestions over time.
