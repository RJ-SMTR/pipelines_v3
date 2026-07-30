# AGENTS.md

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

# dbt (from queries/ — ALWAYS pass --profiles-dir ./dev when running locally)
dbt deps
dbt build --selector <name> --profiles-dir ./dev
dbt compile --select <model> --profiles-dir ./dev  # debug SQL
dbt parse --profiles-dir ./dev                     # check for errors
sqlfmt <file>.sql                                  # ALWAYS after editing SQL
```

## Architecture

```
pipelines/
  common/              # Shared: capture/treatment base flows, utils, constants
  <type>__<name>/      # Individual pipelines
    flow.py, constants.py, prefect.yaml, Dockerfile, pyproject.toml
  templates/           # Cookiecutter templates for new pipelines

queries/               # dbt project — see queries/AGENTS.md
```

### Pipeline Types

- **Capture** (`capture__*`): Ingests data via `create_capture_flows_default_tasks()`
- **Treatment** (`treatment__*`): Runs dbt via `create_materialization_flows_default_tasks()`
- **Integration** (`integration__*`): Custom workflows combining both
- **Control** (`control__*`): Operational flows (Redis keys, freshness checks, capture verification)
- **Maintenance** (`maintenance__*`): Infra housekeeping (DB retention, janitor)
- **Quality check** (`quality_check__*`): Data quality validations
- **Test** (`test__*`): Ad-hoc connectivity/infra test flows

### Naming

- Directory: `<flow_type>__<name>` (e.g., `capture__jae_auxiliar`)
- Deployment: `rj-<type>--<name>--<env>` (e.g., `rj-capture--jae_auxiliar--prod`)

## Key Patterns

- **Flow decorator**: Import `flow` from `pipelines.common.utils.prefect`, NOT from `prefect` — it applies `DEFAULT_FLOW_TIMEOUT` (3600s); override with `timeout_seconds=`
- **Environment**: Auto-detected from deployment name (`get_run_env` in `pipelines/common/tasks.py`): `--prod` → `prod`, anything else (including `--staging`) → `dev`; defaults to `dev` locally
- **Credentials**: `pipelines.common.utils.env.inject_bd_credentials()` (called by the base tasks in `common/`)
- **Secrets**: `pipelines.common.utils.secret.get_env_secret()` — reads from the Kubernetes Secret mounted via `secretName` in `prefect.yaml` (`prefect-jobs-secrets` prod / `prefect-jobs-secrets-staging` staging)
- **DBT selectors**: Use `deepcopy()` when reusing/combining `DBTSelector` objects from another pipeline's `constants.py`
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
