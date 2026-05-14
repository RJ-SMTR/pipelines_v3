# CLAUDE.md

## Project Overview

Monorepo managing **Prefect 3** data pipelines for Rio de Janeiro (SMTR). Combines Prefect flows (`pipelines/`), dbt models (`queries/`), and **uv workspaces** for dependency management. Targets BigQuery on GCP.

## Quick Reference

```bash
# Setup
uv sync --all-packages && source .venv/bin/activate

# Run a pipeline locally
uv run --package <pipeline_name> -- prefect flow-run execute

# Lint & format
ruff check --fix . && ruff format .
pre-commit run --all-files

# New pipeline (interactive)
uv run --package cookiecutter templates --output-dir=pipelines

# dbt (from queries/)
dbt deps && dbt build --selector <name>
dbt compile --select <model>   # debug SQL
dbt parse                       # check for errors
sqlfmt <file>.sql               # ALWAYS after editing SQL
```

## Architecture

```
pipelines/
  common/              # Shared: capture/treatment base flows, utils, constants
  <type>__<name>/      # Individual pipelines (capture__, treatment__, integration__)
    flow.py, constants.py, prefect.yaml, Dockerfile, pyproject.toml
  templates/           # Cookiecutter templates for new pipelines

queries/               # dbt project — see queries/CLAUDE.md
```

### Pipeline Patterns

- **Capture** (`capture__*`): Ingests data via `create_capture_flows_default_tasks()`
- **Treatment** (`treatment__*`): Runs dbt via `create_materialization_flows_default_tasks()`
- **Integration** (`integration__*`): Custom workflows combining both

### Naming

- Directory: `<flow_type>__<name>` (e.g., `capture__jae_auxiliar`)
- Deployment: `rj-<type>--<name>--<env>` (e.g., `rj-capture--jae_auxiliar--prod`)

## Key Patterns

- **Environment**: Auto-detected from deployment name (`--prod` / `--dev`), defaults to `dev` locally
- **Credentials**: `iplanrio.pipelines_utils.env.inject_bd_credentials()`
- **Secrets**: `pipelines.common.utils.secret.get_env_secret()`
- **Logging**: Use `print()` with `@flow(log_prints=True)` — no `log()` calls
- **Constants**: Plain variables, no `Enum` — no `.value` access
- **Imports**: Always at top of file, never inside functions
- **No `__init__.py`** in individual pipelines — only in `common/`
- **Docstrings**: Only at file level in `flow.py`, never on the flow function

## Environment

- **Python 3.13** | **Prefect 3.4.9** | **dbt-bigquery 1.10.1**
- **Formatter**: Ruff (Python), sqlfmt (SQL)
- **Branches**: `staging/*` → staging deploy, `master` → prod deploy
- **Work pools**: `smtr-pool` (general)

## Code Style

- Ruff handles linting + formatting (replaces black/isort/flake8)
- **IMPORTANT**: Always run `sqlfmt` after editing any dbt SQL model

## References

- [Prefect 3 Docs](https://docs.prefect.io/v3/) | [dbt Docs](https://docs.getdbt.com/) | [uv Docs](https://github.com/astral-sh/uv)
- `dbt.md` — Detailed dbt architecture and execution strategies

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **pipelines_v3** (1564 symbols, 2398 relationships, 45 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## When Debugging

1. `gitnexus_query({query: "<error or symptom>"})` — find execution flows related to the issue
2. `gitnexus_context({name: "<suspect function>"})` — see all callers, callees, and process participation
3. `READ gitnexus://repo/pipelines_v3/process/{processName}` — trace the full execution flow step by step
4. For regressions: `gitnexus_detect_changes({scope: "compare", base_ref: "main"})` — see what your branch changed

## When Refactoring

- **Renaming**: MUST use `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` first. Review the preview — graph edits are safe, text_search edits need manual review. Then run with `dry_run: false`.
- **Extracting/Splitting**: MUST run `gitnexus_context({name: "target"})` to see all incoming/outgoing refs, then `gitnexus_impact({target: "target", direction: "upstream"})` to find all external callers before moving code.
- After any refactor: run `gitnexus_detect_changes({scope: "all"})` to verify only expected files changed.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Tools Quick Reference

| Tool | When to use | Command |
|------|-------------|---------|
| `query` | Find code by concept | `gitnexus_query({query: "auth validation"})` |
| `context` | 360-degree view of one symbol | `gitnexus_context({name: "validateUser"})` |
| `impact` | Blast radius before editing | `gitnexus_impact({target: "X", direction: "upstream"})` |
| `detect_changes` | Pre-commit scope check | `gitnexus_detect_changes({scope: "staged"})` |
| `rename` | Safe multi-file rename | `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` |
| `cypher` | Custom graph queries | `gitnexus_cypher({query: "MATCH ..."})` |

## Impact Risk Levels

| Depth | Meaning | Action |
|-------|---------|--------|
| d=1 | WILL BREAK — direct callers/importers | MUST update these |
| d=2 | LIKELY AFFECTED — indirect deps | Should test |
| d=3 | MAY NEED TESTING — transitive | Test if critical path |

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/pipelines_v3/context` | Codebase overview, check index freshness |
| `gitnexus://repo/pipelines_v3/clusters` | All functional areas |
| `gitnexus://repo/pipelines_v3/processes` | All execution flows |
| `gitnexus://repo/pipelines_v3/process/{name}` | Step-by-step execution trace |

## Self-Check Before Finishing

Before completing any code modification task, verify:
1. `gitnexus_impact` was run for all modified symbols
2. No HIGH/CRITICAL risk warnings were ignored
3. `gitnexus_detect_changes()` confirms changes match expected scope
4. All d=1 (WILL BREAK) dependents were updated

## Keeping the Index Fresh

After committing code changes, the GitNexus index becomes stale. Re-run analyze to update it:

```bash
npx gitnexus analyze
```

If the index previously included embeddings, preserve them by adding `--embeddings`:

```bash
npx gitnexus analyze --embeddings
```

To check whether embeddings exist, inspect `.gitnexus/meta.json` — the `stats.embeddings` field shows the count (0 means no embeddings). **Running analyze without `--embeddings` will delete any previously generated embeddings.**

> Claude Code users: A PostToolUse hook handles this automatically after `git commit` and `git merge`.

## CLI

- Re-index: `npx gitnexus analyze`
- Check freshness: `npx gitnexus status`
- Generate docs: `npx gitnexus wiki`

<!-- gitnexus:end -->
