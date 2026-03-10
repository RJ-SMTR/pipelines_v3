Validate dbt models for parse errors and compilation issues.

1. Run dbt parse to check for syntax/config errors:
   ```bash
   cd /home/botelho/prefeitura_rio/pipelines_v3/queries
   dbt parse
   ```

2. If a specific model or selector is provided as argument: $ARGUMENTS
   ```bash
   dbt compile --select $ARGUMENTS
   ```

3. Check for formatting issues on modified SQL files:
   ```bash
   git diff --name-only -- '*.sql' | xargs -r sqlfmt --check
   ```

4. Report: parse status, any errors found, models that need sqlfmt.
