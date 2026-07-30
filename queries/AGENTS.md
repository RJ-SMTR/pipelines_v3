# queries/ — dbt Project

## Structure

```
queries/
  dbt_project.yml        # Config + vars (date_range_start/end, datetime_start/end, flow_name)
  selectors.yml          # Named selectors for `dbt build --selector <name>`
  packages.yml           # dbt package dependencies
  dev/
    profiles-example.yml # Template — copy to profiles.yml (gitignored)
    run.example.py         # Template — copy to run.py for local dbt runs (gitignored)
    utils.py               # Helpers to run dbt from Python
  models/
    <domain>/            # e.g., transito, cadastro, bilhetagem
      staging/           # Raw → structured (stg_*, aux_*)
      *.sql              # Transformation models
      schema.yml         # Column docs + tests
  macros/                # Reusable SQL functions
  snapshots/<domain>/    # SCD Type 2 snapshots
  seeds/                 # Static CSV data
  tests/                 # Data quality tests
  manual_fixes/          # dbt Fusion migration kit (see fusion_migration.md)
```

## Commands

**ALWAYS pass `--profiles-dir ./dev` when running dbt locally.**

```bash
cd queries
cp dev/profiles-example.yml dev/profiles.yml   # first-time setup
cp dev/run.example.py dev/run.py               # optional: local dbt runner

dbt deps                                                    # Install packages
dbt build --selector <selector_name> --profiles-dir ./dev  # Build models by selector
dbt build --select tag:daily --profiles-dir ./dev          # Build by tag
dbt compile --select <model> --profiles-dir ./dev          # See generated SQL (debug)
dbt parse --profiles-dir ./dev                             # Check for parse errors
dbt test --profiles-dir ./dev                              # Run data quality tests
dbt source freshness --profiles-dir ./dev                  # Check source freshness
```

## Key Variables (dbt_project.yml)

- `date_range_start`, `date_range_end` — Date filtering
- `datetime_start`, `datetime_end` — DateTime filtering
- `flow_name` — Current flow identifier
- Domain-specific vars: `brt_terminais`, `sppo_registros_staging`, etc.

## Formatting Rule

**ALWAYS run `sqlfmt <file>.sql` after editing any SQL model.** This is mandatory before committing.

## Test Naming

Named tests in `schema.yml` must use the full package prefix in the `name:` field:

```yaml
- dbt_expectations.expect_row_values_to_have_recent_data:
    name: dbt_expectations.expect_row_values_to_have_recent_data__<column>__<table>
```

## DBTSelector (Python side)

Treatment pipelines use `DBTSelector` from `pipelines.common.treatment.default_treatment.utils`:

```python
from pipelines.common.treatment.default_treatment.utils import DBTSelector

SELECTOR = DBTSelector(
    name="<selector_name>",            # Must match selectors.yml
    initial_datetime=datetime(..., tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
    flow_folder_name="treatment__<name>",
)
```

When reusing/combining a `DBTSelector` from another pipeline's `constants.py`, wrap it in `deepcopy()` to avoid mutating the shared object.
