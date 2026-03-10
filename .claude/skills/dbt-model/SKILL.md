# dbt Model Skill

Workflow for creating or modifying dbt models in `queries/models/`.

## Creating a New Model

1. **Identify the domain** — which folder under `queries/models/` (e.g., `transito`, `cadastro`, `bilhetagem`)
2. **Check existing patterns** in that domain for naming and style conventions
3. **Create the SQL file** following the naming convention:
   - `stg_<source>__<table>.sql` for staging models
   - `<domain>__<description>.sql` for transformation models
4. **Add to schema.yml** — create or update `queries/models/<domain>/schema.yml` with:
   - Model name, description
   - Column names, descriptions, and tests
5. **Add to selectors.yml** if needed — `queries/selectors.yml`
6. **Run sqlfmt** on the new file:
   ```bash
   sqlfmt queries/models/<domain>/<file>.sql
   ```
7. **Validate**:
   ```bash
   cd queries
   dbt parse --select <model_name>
   dbt compile --select <model_name>
   ```

## Modifying an Existing Model

1. Read the current model and its schema.yml entry
2. Make changes
3. Run `sqlfmt` on the modified file
4. Validate with `dbt parse` and `dbt compile`
5. Check downstream dependencies:
   ```bash
   dbt ls --select <model_name>+
   ```

## Key Reminders

- **ALWAYS** run `sqlfmt` after any SQL edit
- Use `{{ var("datetime_start") }}` and `{{ var("datetime_end") }}` for date filtering
- Use `ref("model_name")` for model references, `source("source", "table")` for raw tables
- Check `queries/macros/` for reusable SQL functions before writing custom logic
