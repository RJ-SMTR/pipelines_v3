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
- **Work pools**: `default-pool` (general), `datario-pool` (VPN/internal DB)

## Code Style

- Ruff handles linting + formatting (replaces black/isort/flake8)
- **IMPORTANT**: Always run `sqlfmt` after editing any dbt SQL model

## References

- [Prefect 3 Docs](https://docs.prefect.io/v3/) | [dbt Docs](https://docs.getdbt.com/) | [uv Docs](https://github.com/astral-sh/uv)
- `dbt.md` — Detailed dbt architecture and execution strategies
