# queries/ — dbt Project

## Structure

```
queries/
  dbt_project.yml        # Config + vars (date_range_start/end, datetime_start/end, flow_name)
  selectors.yml          # Named selectors for `dbt build --selector <name>`
  packages.yml           # dbt package dependencies
  models/
    <domain>/            # e.g., transito, cadastro, bilhetagem
      staging/           # Raw → structured (stg_*, aux_*)
      *.sql              # Transformation models
      schema.yml         # Column docs + tests
  macros/                # Reusable SQL functions
  snapshots/<domain>/    # SCD Type 2 snapshots
  seeds/                 # Static CSV data
  tests/                 # Data quality tests
```

## Commands

```bash
cd queries

dbt deps                              # Install packages
dbt build --selector <selector_name>  # Build models by selector
dbt build --select tag:daily          # Build by tag
dbt compile --select <model>          # See generated SQL (debug)
dbt parse                             # Check for parse errors
dbt test                              # Run data quality tests
dbt source freshness                  # Check source freshness
```

## Key Variables (dbt_project.yml)

- `date_range_start`, `date_range_end` — Date filtering
- `datetime_start`, `datetime_end` — DateTime filtering
- `flow_name` — Current flow identifier
- Domain-specific vars: `brt_terminais`, `sppo_registros_staging`, etc.

## Formatting Rule

**ALWAYS run `sqlfmt <file>.sql` after editing any SQL model.** This is mandatory before committing.

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
