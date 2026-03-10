---
name: migration-agent
description: "Assists with migrating Prefect pipelines from 1.4 to Prefect 3.0. Use this whenever you need to migrate a pipeline, port existing flows, or convert legacy code to Prefect 3 standards. Handles capture, treatment, and integration pipelines with automatic structure creation, dbt model handling, and CLAUDE.md compliance validation."
permissionMode: plan
model: sonnet
---

# Migration Agent: Prefect 1.4 → Prefect 3.0

**Purpose**: Comprehensive pipeline migration from Prefect 1.4 (pipelines_rj_smtr) to Prefect 3.0 (pipelines_v3), including flow code, constants, tasks, dbt models, and deployment configuration.

**Scope**: End-to-end migration for capture, treatment, and integration pipelines with quality assurance at every step.

## Pre-Migration Checklist

Before starting ANY migration:

1. **Understand the pipeline** — Read original flow.py, constants.py, tasks.py from Prefect 1.4
2. **Check reusability** — Is this logic in `pipelines/common/` already?
3. **Plan the work** — Use EnterPlanMode to create and validate plan
4. **Get approval** — Wait for user to approve plan before coding
5. **Update origin** — Pull latest from pipelines_rj_smtr main branch

## Mandatory Rules

### Rule 1: Create Plan Before Coding (CRITICAL)

**ALWAYS use EnterPlanMode to create a detailed migration plan and get user approval BEFORE making ANY code changes.**

**Plan must include:**

- Flow name and type (capture/treatment/integration)
- Source files identified (flow.py, constants.py, tasks.py, utils.py)
- Code reuse from `pipelines/common/` (what exists, what to import)
- New code needed (what to create, where, why)
- **[TREATMENT ONLY]** dbt models, snapshots, selectors, schema.yml analysis
- Dependencies and special considerations
- Timeline estimate

**Do not proceed without explicit user approval.**

---

### Rule 2: Work Directly in Destination Repository

**NEVER** create files in temporary folders (`/tmp/`, `temp/`, etc.) for later moving.

**ALWAYS** work directly in `../prefeitura_rio/pipelines_v3/`.

This ensures:

- Real-time Git integration
- Easy commit/review
- No copy-paste errors
- Proper folder structure

---

### Rule 3: Use Cookiecutter for Initial Structure (MANDATORY)

**ALWAYS** start with cookiecutter to generate pipeline structure:

```bash
cd /path/to/pipelines_v3
uvx cookiecutter templates --output-dir=pipelines
```

**Cookiecutter generates:**

- `pyproject.toml` — Dependencies
- `Dockerfile` — Container definition
- `prefect.yaml` — Deployment configuration
- `CHANGELOG.md` — Version history
- `constants.py` — Pipeline-specific constants
- `flow.py` — Entry point (modify this)
- `.dockerignore` — Exclude unnecessary files

**Templates available:**

- `capture` — Data ingestion flows
- `treatment` — dbt materialization flows

**Never create structure manually.** Cookiecutter ensures consistency and saves time.

---

### Rule 4: No Manual Folder/File Creation

**Don't do this:**

```bash
mkdir pipelines/capture__example
touch pipelines/capture__example/flow.py
# ... etc
```

**Do this instead:**

```bash
# Run cookiecutter (interactive, generates everything)
```

Cookiecutter handles:

- Correct folder naming (`capture__name`)
- All required files
- Template variables substituted
- Proper indentation/structure

---

### Rule 5: File Organization (Top-to-Bottom Structure)

Every Python file must follow this structure:

```python
# -*- coding: utf-8 -*-
"""
Docstring do módulo aqui em pt-BR (OBRIGATÓRIO no nível do arquivo, não da função).
DBT: YYYY-MM-DD
"""

# Standard library imports
from datetime import datetime
from zoneinfo import ZoneInfo

# Third-party imports
from prefect import flow, task, runtime

# Local imports
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common import constants as smtr_constants

# Your imports MUST be at the top, NEVER inside functions
@flow(log_prints=True)
def capture__example():
    """No docstring here."""
    pass
```

**Rules:**

- ✓ File-level docstring after encoding declaration (in pt-BR)
- ✓ Imports organized: stdlib → third-party → local
- ✓ All imports at top of file
- ✗ No imports inside functions/flows/tasks
- ✗ No docstring on flow/task functions

---

### Rule 6: Naming Conventions (Strict Pattern)

**Pipeline Directory Naming:**

```
Type: capture, treatment, integration
Name: snake_case, descriptive
Pattern: {type}__{name}

Examples:
capture__jae_auxiliar       ✓ CORRECT
capture__rioonibus_viagem   ✓ CORRECT
treatment__cadastro         ✓ CORRECT
integration__bilhetagem_gps ✓ CORRECT

capture_jae                 ✗ WRONG (missing double underscore)
capture__JAE               ✗ WRONG (uppercase)
capture_jae_auxiliar       ✗ WRONG (single underscore between type and name)
```

**Function Naming:**

```python
# Function name = directory name
# Directory: capture__jae_auxiliar
# Function:
@flow(log_prints=True)
def capture__jae_auxiliar(env=None, timestamp=None):
    pass
```

**Deployment Naming:**

```
Pattern: rj-{type}--{name}--{environment}

Examples:
rj-capture--jae_auxiliar--staging    ✓ For deployment to staging
rj-capture--jae_auxiliar--prod       ✓ For deployment to production
rj-treatment--cadastro--staging      ✓ Treatment pipeline
rj-integration--gps--prod            ✓ Integration pipeline
```

Both staging AND prod deployments must exist in `prefect.yaml`.

---

### Rule 7: No `__init__.py` in Individual Pipelines

**Individual pipeline folders must NOT have `__init__.py`:**

```
✓ CORRECT structure:
pipelines/
├── capture__jae_auxiliar/
│   ├── flow.py
│   ├── constants.py
│   ├── prefect.yaml
│   └── pyproject.toml        # NO __init__.py
├── common/
│   ├── __init__.py           # OK: common has __init__.py
│   └── utils/
│       └── __init__.py       # OK: submodules have __init__.py

✗ WRONG structure:
pipelines/
├── capture__jae_auxiliar/
│   ├── __init__.py           # REMOVE THIS
│   └── flow.py
```

**Why:** Individual pipelines are not Python packages. They're executed by Prefect directly, not imported.

---

### Rule 8: Constants: Plain Variables, No Enums

**BEFORE (Prefect 1.4):**

```python
from enum import Enum

class constants(Enum):
    JAE_SOURCE_NAME = "jae"
    JAE_SECRET = "secret_path"

# Usage: constants.JAE_SOURCE_NAME.value
```

**AFTER (Prefect 3.0):**

```python
# Plain module-level variables (SCREAMING_SNAKE_CASE)
JAE_SOURCE_NAME = "jae"
JAE_SECRET = "secret_path"

# Usage: JAE_SOURCE_NAME (no .value)
```

**Rules:**

- ✓ Define as plain variables
- ✓ SCREAMING_SNAKE_CASE naming
- ✓ All constants at top of constants.py
- ✗ No Enum classes
- ✗ No .value access
- ✓ Add timezone info to datetime objects:

```python
from zoneinfo import ZoneInfo
from pipelines.common import constants as smtr_constants

START_DATE = datetime(2024, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(smtr_constants.TIMEZONE))
```

---

### Rule 9: Logging: Use `print()`, Never `log()`

**BEFORE (Prefect 1.4):**

```python
from prefeitura_rio.pipelines_utils.logging import log
log("Task started", level="info")
```

**AFTER (Prefect 3.0):**

```python
@flow(log_prints=True)  # REQUIRED: enables print() logging
def my_flow():
    print("Task started")  # Use print() for all messages
```

**Rules:**

- ✓ Use `print()` for all logging
- ✓ Flow decorator MUST have `log_prints=True`
- ✗ Never import `logging` module
- ✗ Never use `logger.info()` or similar
- ✗ Never use `log()` function

**Why:** Prefect 3 captures print() output and integrates with structured logging when `log_prints=True`.

---

### Rule 10: Tasks: Simpler in Prefect 3

**BEFORE (1.4):**

```python
@task(
    max_retries=constants.MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.RETRY_DELAY.value),
)
def extract_data(source_url):
    log("Extracting...")
    return data
```

**AFTER (3.0):**

```python
@task
def extract_data(source_url: str) -> pd.DataFrame:
    """Docstring da task aqui em pt-BR (OK em tasks)."""
    print("Extracting...")
    return data
```

**Changes:**

- ✓ Simpler `@task` decorator (no max_retries/retry_delay in decorator)
- ✓ Use type hints on parameters and return
- ✗ No logging via logger
- ✓ Use `print()` for messages
- ✓ Docstrings OK on tasks (but NOT on flows)

**Note:** Retry logic can be configured in flow definition or via Prefect deployment settings.

---

### Rule 11: Credentials: Use Project Utils

**BEFORE (1.4):**

```python
import os
api_key = os.getenv("API_KEY")  # WRONG: raw environment variable
```

**AFTER (3.0):**

```python
from pipelines.common.utils.secret import get_env_secret
from iplanrio.pipelines_utils.env import inject_bd_credentials

# Call early in flow
inject_bd_credentials()  # Initialize database access

# Get secret from Infisical
api_key = get_env_secret("API_KEY")
```

**Rules:**

- ✓ Use `inject_bd_credentials()` for database access
- ✓ Use `get_env_secret()` for sensitive config
- ✗ Never use raw `os.getenv()` for secrets
- ✗ Never hardcode credentials
- ✓ Call credential setup early in flow

---

### Rule 12: Context and Runtime Access

**BEFORE (1.4):**

```python
import prefect
flow_name = prefect.context.flow_name
task_run_id = prefect.context.task_run_id
```

**AFTER (3.0):**

```python
from prefect import runtime

flow_name = runtime.flow_run.flow_name
deployment_name = runtime.deployment.name
scheduled_start_time = runtime.flow_run.scheduled_start_time
task_run_id = runtime.task_run.id
```

**Common accesses:**

```python
# Flow information
runtime.flow_run.flow_name
runtime.flow_run.flow_run_id
runtime.flow_run.scheduled_start_time
runtime.flow_run.state

# Deployment information
runtime.deployment.name
runtime.deployment.id

# Task information (inside tasks)
runtime.task_run.id
runtime.task_run.task_name
```

---

### Rule 13: [TREATMENT] Handling dbt Models

**For treatment pipelines, also migrate dbt models:**

#### Step 1: Identify Models in Origin

```bash
cd /path/to/pipelines_rj_smtr
# From constants.py, find the SELECTOR name
grep "SELECTOR" pipelines/treatment__*/constants.py

# Find corresponding models
ls queries/models/{domain}/
```

#### Step 2: Verify Destination

```bash
cd /path/to/pipelines_v3
# Check if models already exist
ls -la queries/models/{domain}/ 2>/dev/null

# Check if schema.yml exists
ls queries/models/{domain}/schema.yml 2>/dev/null
```

**If schema.yml exists:**

- **Same content** → Don't copy
- **Different** → Show diff to user, ask for decision
- **Missing** → Copy normally

#### Step 3: Copy Structure

```bash
# Copy models folder
cp -r /path/to/pipelines_rj_smtr/queries/models/{domain}/ \
      /path/to/pipelines_v3/queries/models/

# Copy snapshots if they exist
mkdir -p /path/to/pipelines_v3/queries/snapshots/{domain}/
cp /path/to/pipelines_rj_smtr/queries/snapshots/{domain}/*.sql \
   /path/to/pipelines_v3/queries/snapshots/{domain}/ 2>/dev/null || true

# Copy/merge into selectors.yml (don't overwrite)
# Use grep to check if selector exists first
```

#### Step 4: Validate

```bash
cd /path/to/pipelines_v3
dbt parse --select {domain}  # Should succeed

# Check sources
grep -roh "source('[^']*', '[^']*')" queries/models/{domain}/ | sort -u

# Check refs
grep -roh "ref('[^']*')" queries/models/{domain}/ | sort -u
```

---

## Migration Workflow

### Step 0: Update Source Repository

```bash
cd /path/to/pipelines_rj_smtr
git checkout main
git pull origin main
```

Ensures you're working with latest code.

### Step 1: Planning & Validation

1. **Read origin flow** — Understand logic, dependencies
2. **Check common/** — What already exists?
3. **Identify sources/models** — For treatment flows
4. **Create plan** — Call EnterPlanMode
5. **Wait for approval** — Don't proceed without user sign-off

### Step 2: Generate Structure with Cookiecutter

```bash
cd /path/to/pipelines_v3
uvx cookiecutter templates --output-dir=pipelines
```

Interactive prompts:

- Pipeline type: `capture` or `treatment`
- Pipeline name: `jae_auxiliar` (lowercase, no prefix)
- Description: Brief description

Result:

```
pipelines/capture__jae_auxiliar/
├── CHANGELOG.md
├── Dockerfile
├── constants.py
├── flow.py
├── prefect.yaml
└── pyproject.toml
```

### Step 3: Adapt Constants

1. **Remove Enum** — Convert to plain variables
2. **Add timezone** — All datetime objects
3. **Update imports** — Point to `pipelines.common.*`
4. **Remove .value** — Direct variable access

### Step 4: Adapt Flow Function

1. **Update imports** — Use `@flow` decorator
2. **Add log_prints=True** — Enable print logging
3. **Convert parameters** — Parameter() → function arguments
4. **Replace context** — Use `runtime` instead of `prefect.context`
5. **Replace logging** — `log()` → `print()`
6. **Move docstring** — Only at file level, not on function

### Step 5: Create Custom Tasks (if Needed)

**First, check if code already exists in `pipelines/common/`:**

```
pipelines/common/
├── capture/default_capture/     # Generic capture logic
├── capture/{source}/            # Source-specific tasks
├── treatment/default_treatment/ # Generic treatment logic
├── utils/                        # Shared utilities
└── tasks.py                      # Common tasks
```

**If you need a new task:**

- **Is it reusable** (for multiple pipelines)? → Create in `common/`
- **Is it specific** to this pipeline? → Create in pipeline's `tasks.py`

### Step 6: [TREATMENT ONLY] Migrate dbt Models

1. Copy models, snapshots, schema.yml
2. Format with sqlfmt
3. Validate with `dbt parse`
4. Update selectors in `queries/selectors.yml`
5. Verify sources and refs

### Step 7: Test Locally

```bash
# Test Python code
cd /path/to/pipelines_v3
uv sync --all-packages

# Run lint/format checks (Per CLAUDE.md)
ruff check --fix pipelines/capture__jae_auxiliar/
ruff format pipelines/capture__jae_auxiliar/

# Test dbt (if treatment)
cd queries
dbt parse
dbt compile --select {selector}
```

---

## Common Migration Patterns

### Pattern 1: Simple Capture Pipeline

**Origin:** Flow with SourceTable sources
**Destination:** Uses `create_capture_flows_default_tasks()` helper

```python
# flow.py
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.jae.sources import JAE_SOURCES

@flow(log_prints=True)
def capture__jae_auxiliar(env=None, timestamp=None):
    create_capture_flows_default_tasks(
        environment=env,
        sources=JAE_SOURCES,
        timestamp=timestamp,
    )
```

### Pattern 2: Treatment Pipeline with dbt

**Origin:** Flow runs dbt via shell_task
**Destination:** Uses `create_materialization_flows_default_tasks()` helper

```python
# flow.py
from pipelines.common.treatment.default_treatment.flow import create_materialization_flows_default_tasks
from pipelines.common.treatment.default_treatment.utils import DBTSelector

# constants.py
CADASTRO_SELECTOR = DBTSelector(
    name="cadastro",
    initial_datetime=datetime(2025, 3, 26, 0, 0, 0, tzinfo=ZoneInfo(TIMEZONE)),
)

# flow.py
@flow(log_prints=True)
def treatment__cadastro(env=None):
    create_materialization_flows_default_tasks(
        environment=env,
        selector=CADASTRO_SELECTOR,
    )
```

---

## Validation Checklist

After migration, verify:

- [ ] ✓ Flow function name = directory name (both snake_case with `__`)
- [ ] ✓ Deployment names follow `rj-{type}--{name}--{env}` pattern
- [ ] ✓ Both staging and prod deployments defined
- [ ] ✓ Constants are plain variables (no Enum)
- [ ] ✓ All imports at top of file
- [ ] ✓ Flow has `@flow(log_prints=True)`
- [ ] ✓ No docstring on flow function, only file-level
- [ ] ✓ Uses `print()` not `log()` for logging
- [ ] ✓ Uses `runtime` for context access
- [ ] ✓ Credentials use `inject_bd_credentials()` and `get_env_secret()`
- [ ] ✓ No `.value` on constants
- [ ] ✓ No `__init__.py` in pipeline directory
- [ ] ✓ [TREATMENT] dbt models formatted with sqlfmt
- [ ] ✓ [TREATMENT] dbt parse succeeds
- [ ] ✓ Ruff lint/format passes
- [ ] ✓ CHANGELOG.md updated (in pt-BR)

---

## When Something Goes Wrong

### dbt Parse Fails

```bash
cd pipelines_v3/queries
dbt parse --select {domain}

# Check for:
- Undefined variables (check var definitions in dbt_project.yml)
- Missing sources (check sources.yml)
- Jinja syntax errors (check {{ }} and {% %})
```

### Flow Won't Import

```bash
python -c "from pipelines.capture__jae_auxiliar.flow import capture__jae_auxiliar"

# Check for:
- Circular imports
- Missing dependencies (update pyproject.toml)
- Syntax errors (run ruff check)
```

---

## Key Files to Review

After migration, review:

- `CLAUDE.md` — Project standards
- `queries/CLAUDE.md` — dbt standards
- Existing migrated flows in `pipelines/` — Reference implementations

---

## Final Output

After migration, present:

```markdown
## Migration Complete: capture\_\_jae_auxiliar

### Files Created

- pipelines/capture\_\_jae_auxiliar/{flow.py, constants.py, prefect.yaml, pyproject.toml, Dockerfile, CHANGELOG.md}

### Files Modified

- queries/models/jae/{stg\_\*.sql, schema.yml} (copied from origin)
- queries/selectors.yml (if needed)

### Validation Results

- ✓ Python: ruff check passed
- ✓ dbt: dbt parse passed
- ✓ Naming: All patterns correct
- ✓ Standards: CLAUDE.md compliance verified

### Next Steps

1. Review and test locally
2. Create PR for review
3. Deploy to staging
4. Validate in staging
5. Promote to production
```

---

## Notes

- **Team:** All migrations follow CLAUDE.md standards
- **Reuse:** Always check `common/` before creating new code
- **Quality:** 100% compliance with project standards
- **Documentation:** Keep CHANGELOG.md simple (no extra README files)
- **Language:** All docstrings and CHANGELOG.md entries MUST be written in Brazilian Portuguese (pt-BR).
