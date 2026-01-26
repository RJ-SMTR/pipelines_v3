# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a monorepo managing multiple **Prefect 3** data pipelines for the Rio de Janeiro municipal government. The project combines:
- **Prefect flows** (in `pipelines/`) for orchestration and data capture/transformation
- **DBT** (in `queries/`) for data modeling and transformation in BigQuery
- **uv workspaces** for Python dependency management across multiple pipeline modules

The repository architecture ensures reproducibility, isolation, and standardization across all pipelines.

## Development Environment

### Setup and Installation

```bash
# Using uv (recommended)
uv sync --all-packages

# Activate the virtual environment
source .venv/bin/activate

# Run a single pipeline locally for testing
python test.py
```

### Running Pipelines Locally

```bash
# Test a specific flow
uv run --package <pipeline_name> -- prefect flow-run execute

# Example: test the capture__jae_auxiliar flow
uv run --package capture__jae_auxiliar -- prefect flow-run execute
```

### Python and Code Quality

- **Python version**: 3.13 (see `pyproject.toml`)
- **Linting and formatting**: Ruff
  - `ruff check --fix .` - Run linter with automatic fixes
  - `ruff format .` - Format code
  - Pre-commit hooks automatically enforce these rules on git commits

### Key Dependencies

- **Prefect 3.4.9**: Workflow orchestration and scheduling
- **dbt-bigquery 1.10.1**: Data transformation and modeling
- **iplanrio**: Internal library for common utilities and credential injection
- **pandas, pendulum, pydantic**: Data processing and utilities
- **redis-pal**: Redis client utilities

## Codebase Architecture

### Directory Structure

```
pipelines_v3/
├── pipelines/                    # Prefect flows
│   ├── common/                   # Shared utilities and base patterns
│   │   ├── capture/              # Base capture flow patterns
│   │   ├── treatment/            # DBT materialization patterns
│   │   ├── tasks.py              # Common Prefect tasks
│   │   ├── utils/                # Helper utilities
│   │   └── constants.py          # Shared constants
│   ├── <flow_type>__<name>/      # Individual pipeline modules
│   │   ├── flow.py               # Prefect flow definition
│   │   ├── prefect.yaml          # Deployment configuration
│   │   ├── Dockerfile            # Pipeline-specific image
│   │   ├── pyproject.toml        # Pipeline dependencies
│   │   ├── constants.py           # Pipeline constants
│   │   └── utils/                 # Pipeline-specific utilities
│   └── __init__.py               # Workspace root
│
├── queries/                      # DBT project for data modeling
│   ├── models/                   # DBT models organized by domain
│   │   ├── transito/             # Traffic/transportation domain
│   │   ├── cadastro/             # Registry domain
│   │   ├── bilhetagem/           # Ticketing domain
│   │   └── <domain>/staging/     # Staging transformations
│   ├── dbt_project.yml           # DBT configuration
│   ├── selectors.yml             # DBT model selectors for filtering runs
│   ├── macros/                   # Reusable DBT macros
│   ├── tests/                    # dbt tests
│   ├── packages.yml              # dbt dependencies
│   └── dbt_packages/             # Installed dbt packages
│
├── pyproject.toml                # Root workspace configuration
├── Dockerfile                    # Base image for all pipelines
├── dbt.md                        # Detailed DBT architecture
└── README.md                     # Project overview

```

### Pipeline Naming Convention

- **Directory**: `<flow_type>__<name>` (lowercase, underscores only)
  - `flow_type`: `capture`, `treatment`, `integration`, `test`, etc.
  - Example: `capture__jae_auxiliar`, `treatment__cliente_cpf`

- **Deployment**: `rj-<flow_type>--<name>--<env>`
  - Example: `rj-capture--jae_auxiliar--prod`, `rj-treatment--cadastro--staging`

### Pipeline Execution Patterns

#### 1. **Capture Pattern** (`capture__*`)
Data ingestion/extraction flows. Uses `common.capture.default_capture`:
```python
# Flow delegates to shared base pattern
create_capture_flows_default_tasks(
    env=env,
    sources=sources,           # Data source definitions
    source_table_ids=source_table_ids,
    timestamp=timestamp,
    create_extractor_task=create_extractor_task,  # Custom extractor
    recapture=recapture,
    recapture_days=recapture_days,
)
```

#### 2. **Treatment Pattern** (`treatment__*`)
DBT model materialization flows. Uses `common.treatment.default_treatment`:
```python
# Flow delegates to shared base pattern for DBT execution
create_materialization_flows_default_tasks(
    env=env,
    selectors=[SELECTOR],  # DBT selector from constants.py
    datetime_start=datetime_start,
    datetime_end=datetime_end,
    flags=flags,
    additional_vars=additional_vars,
)
```

#### 3. **Integration Pattern** (`integration__*`)
Custom data workflows combining capture and treatment logic.

### Flow Parameters and Environment Resolution

Most flows accept these parameters:
- `env`: "prod" or "dev" (auto-detected from deployment name if None)
- `timestamp`: Scheduled run time (auto-detected if None)
- For DBT flows: `datetime_start`, `datetime_end`, `flags`, `additional_vars`

Environment resolution (`get_run_env` in `common/tasks.py`):
- Deployment name ending in `--prod` → env="prod"
- Deployment name ending with other suffix → env="dev"
- Running locally with no env specified → defaults to "dev"

### Credential Injection

The `iplanrio` library handles credential injection:
```python
from iplanrio.pipelines_utils.env import inject_bd_credentials
inject_bd_credentials(environment="prod" | "staging")  # Auto-loads from secrets
```

In Kubernetes, secrets are mounted via `secretName` in `prefect.yaml`:
- `prefect-jobs-secrets`: Production environment
- `prefect-jobs-secrets-staging`: Staging/development environment

## DBT and Data Modeling

See `dbt.md` for comprehensive DBT documentation. Key points:

### DBT Project Structure

```
queries/
├── dbt_project.yml           # Configuration + vars for dates, IDs, table references
├── models/
│   └── <domain>/
│       ├── staging/          # Raw → structured (stg_*)
│       └── *.sql             # Transformation models
├── selectors.yml             # Named selectors for `dbt build --selector <name>`
├── macros/                   # Custom SQL functions
└── tests/                    # Data quality tests (dbt test)
```

### Common DBT Variables

Set in `dbt_project.yml` vars and overridable at runtime:
- `date_range_start`, `date_range_end`: Date filtering
- `datetime_start`, `datetime_end`: DateTime filtering
- `flow_name`: Current flow identifier
- Domain-specific: `brt_terminais`, `sppo_registros_staging`, etc.

### Running DBT Commands

From the root:
```bash
cd queries

# List available models and selectors
dbt ls

# Build specific models
dbt build --select tag:daily
dbt build --selector <selector_name>

# Test data quality
dbt test

# Generate and view documentation
dbt docs generate
dbt docs serve

# Dry run to see what would execute
dbt parse

# Check source freshness
dbt source freshness
```

### DBT in Pipelines

**Dedicated execution pipeline** (`rj_iplanrio__run_dbt`):
- Scheduled daily execution of tagged models (tag:daily, tag:weekly)
- Clones `https://github.com/prefeitura-rio/queries-rj-iplanrio.git`
- Manages artifacts in GCS for incremental builds
- Sends Discord notifications on success/failure

**On-demand execution** in specific pipelines:
```python
from iplanrio.pipelines_utils.dbt import execute_dbt_task
execute_dbt_task(select="raw_model_name", target="prod")
```

## Docker and Deployment

### Base Docker Image

The root `Dockerfile` provides the base image used by all pipelines:
- Python 3.13
- Oracle InstantClient (for legacy database connectivity)
- MSSQL drivers
- dbt and dependencies
- All Python packages via `uv sync`

### Pipeline-Specific Images

Each pipeline has its own `Dockerfile` extending the base:
```dockerfile
FROM ghcr.io/rj-smtr/pipelines_v3:latest
# Add pipeline-specific dependencies if needed
```

### Deployment Configuration

Each pipeline requires `prefect.yaml` defining:
- **build** step: Compile Docker image with git commit hash
- **push** step: Push to GitHub Container Registry (`ghcr.io/rj-smtr/pipelines_v3`)
- **pull** step: Set working directory in container
- **deployments**: Define deployment(s) with:
  - Staging and prod variants
  - Work pool and job variables (including `secretName`)
  - Schedules (cron format, timezone: America/Sao_Paulo)
  - Entrypoint: `pipelines/<pipeline>/flow.py:<flow_function>`

Example schedule in `prefect.yaml`:
```yaml
schedules:
  - cron: "0 * * * *"        # Every hour
    timezone: "America/Sao_Paulo"
```

### Work Pools

Two main pools for execution:
- **default-pool**: General purpose flows with no special network requirements
- **datario-pool**: Flows accessing internal databases or requiring VPN

## CI/CD

GitHub Actions workflows in `.github/workflows/`:

### Deploy Prefect Flows (`deploy-prefect-flows-prod.yaml`, `deploy-prefect-flows-staging.yaml`)
- Triggered on: Push to `master` (prod) or `staging/*` (staging), or manual trigger
- Watches: Changes in `pipelines/**`
- Steps:
  1. Checkout code
  2. Login to GitHub Container Registry
  3. Install dependencies with `uv`
  4. Run `.github/scripts/deploy_prefect_flows.py` to deploy all flows
  5. Fail fast on any deployment error

### Build Base Image (`build-and-push-root-dockerfile.yaml`)
- Triggered on: Changes to root `Dockerfile` on `master`, or manual trigger
- Publishes to: `ghcr.io/${{ github.repository }}:latest`

## Common Tasks

### Creating a New Pipeline

Use the template generator:
```bash
uv run --package cookiecutter templates --output-dir=pipelines
```

Prompts for `secretaria` and `pipeline` name, generates:
- `pipelines/<flow_type>__<name>/` directory
- `flow.py`, `prefect.yaml`, `Dockerfile`, `pyproject.toml`
- Pre-configured for naming conventions and patterns

### Adding Pipeline Dependencies

Edit `pipelines/<pipeline>/pyproject.toml`:
```toml
[project]
name = "<flow_type>__<name>"
dependencies = [
    "your-package>=1.0.0",
]
```

Root `pyproject.toml` automatically includes all pipeline modules via `uv.workspace.members = ["pipelines/*"]`.

### Running Tests

The repository uses pytest (if configured) or custom test flows:
```bash
# Check a single pipeline flow
uv run --package <pipeline> -- prefect flow-run execute
```

### Formatting and Linting

```bash
# Fix code style
ruff check --fix .
ruff format .

# Format DBT SQL models
sqlfmt <model_file>.sql

# Pre-commit checks all staged files
pre-commit run --all-files
```

**IMPORTANT**: Whenever modifying DBT models in `queries/models/`, always run `sqlfmt` to format the SQL.

### Debugging DBT Issues

```bash
cd queries

# See full query being generated (useful for SQL debugging)
dbt compile --select <model>

# Run with verbose output
dbt build --select <model> -d

# Check for parse errors
dbt parse --select <model>

# Inspect model dependencies
dbt docs generate && dbt docs serve

# Format SQL after modifications
sqlfmt <model_file>.sql
```

## Key Concepts and Patterns

### Prefect 3 Basics

- **Flows**: Functions decorated with `@flow` defining the workflow DAG
- **Tasks**: Functions decorated with `@task` representing atomic operations
- **Deployments**: Configured in `prefect.yaml` to schedule and manage flow execution
- **Parameters**: Flow inputs, auto-mapped to CLI args when using `prefect flow-run execute`
- **Work Pools**: Kubernetes job execution targets

### Environment Separation

- **prod**: Master branch, `prefect-jobs-secrets`, production databases
- **dev/staging**: Staging branches, `prefect-jobs-secrets-staging`, staging databases
- **Local**: Running flows on developer machine, auto-detected

### Data Flow in This Repository

1. **Capture flows** extract data and load to BigQuery staging tables
2. **Treatment flows** transform staged data using dbt and materialize final tables
3. **Each domain** (transito, cadastro, etc.) has its own schema and models

## File Changes to Be Aware Of

Current modified files (check git status):
- DBT models in `queries/models/transito_interno/` and `queries/models/transito_interno/staging/`
- DBT model in `queries/models/cadastro/` staging area

Always ensure modifications follow existing patterns and pass pre-commit hooks before committing.

## References

- [Prefect 3 Documentation](https://docs.prefect.io/v3/)
- [dbt Documentation](https://docs.getdbt.com/)
- [uv Documentation](https://github.com/astral-sh/uv)
- See `dbt.md` for detailed DBT architecture and execution strategies
- See `README.md` for full project structure and CI/CD details
