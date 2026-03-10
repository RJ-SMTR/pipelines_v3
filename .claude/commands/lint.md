Run all linting and formatting checks on the codebase.

1. Run Python linting and formatting:
   ```bash
   cd /home/botelho/prefeitura_rio/pipelines_v3
   ruff check --fix .
   ruff format .
   ```

2. If any SQL files were modified (check with `git diff --name-only`), run sqlfmt on them:
   ```bash
   sqlfmt <changed_sql_files>
   ```

3. Report results: files fixed, remaining issues, or all clean.
