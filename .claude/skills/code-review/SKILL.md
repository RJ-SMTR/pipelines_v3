# Code Review Skill

Review changed files against project conventions and best practices.

## Workflow

1. Get the list of changed files:
   ```bash
   git diff --name-only HEAD~1..HEAD
   # or for staged changes:
   git diff --name-only --cached
   ```

2. For each changed file, check against this checklist:

### Python Files
- [ ] Imports at top of file, never inside functions
- [ ] No `Enum` for constants — use plain variables
- [ ] No `.value` access on constants
- [ ] `print()` for logging (with `log_prints=True` on flow), not `log()`
- [ ] No `__init__.py` in individual pipeline directories
- [ ] Docstring only at file level in `flow.py`, not on the flow function
- [ ] Uses `pipelines.common.*` imports where shared code exists
- [ ] Credentials via `iplanrio` or `get_env_secret()`, never hardcoded
- [ ] Timezone-aware datetimes (`tzinfo=ZoneInfo(...)`)

### dbt SQL Files
- [ ] Formatted with `sqlfmt`
- [ ] Uses `{{ var(...) }}` for configurable values
- [ ] Proper `ref()` and `source()` usage
- [ ] schema.yml exists with column descriptions and tests

### prefect.yaml
- [ ] Both staging and prod deployments defined
- [ ] Correct `secretName` per environment
- [ ] Schedule has `timezone: "America/Sao_Paulo"`
- [ ] Entrypoint matches flow function name

### General
- [ ] No temporary files, debug prints, or TODOs left behind
- [ ] CHANGELOG.md updated if applicable
- [ ] No secrets or credentials in code

3. Report findings grouped by severity: blocking issues, warnings, suggestions.
