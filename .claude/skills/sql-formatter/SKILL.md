---
name: sql-formatter
description: Format SQL files with sqlfmt (shandy-sqlfmt) according to project standards. Use this whenever editing dbt models, fixing SQL syntax, or preparing SQL for commit. ALWAYS format SQL before committing — this is mandatory per CLAUDE.md. Supports Jinja templates, BigQuery functions, and all dbt-specific syntax.
---

# SQL Formatter Skill

Enforces consistent SQL formatting across all dbt models using `sqlfmt` with Jinja support.

## Core Purpose

Per **CLAUDE.md**: **ALWAYS run `sqlfmt` after editing any dbt SQL model.** This is not optional.

This skill ensures:

- Consistent formatting across the codebase
- Readable, idiomatic SQL
- Proper Jinja template handling
- BigQuery compatibility
- Team standardization

## When to Use

- **After editing any `.sql` file** — Format before saving
- **Before committing SQL changes** — Validate formatting in PR
- **Fixing SQL syntax issues** — Formatter helps identify structure problems
- **Reviewing dbt models** — Ensure consistent style across domains
- **Bulk formatting** — Format entire domain or model set

## What It Does

### Input

- Path to one or more `.sql` files in `queries/models/`
- Can be a single file: `queries/models/bilhetagem/stg_bilhetagem.sql`
- Can be a directory: `queries/models/bilhetagem/` (formats all `.sql` files recursively)

### Processing

1. **Syntax validation** — Check for SQL errors before formatting
2. **Formatting pass** — Apply sqlfmt rules:
   - Uppercase SQL keywords (SELECT, FROM, WHERE, JOIN, etc.)
   - Consistent spacing and indentation (2-space indent)
   - Line breaks for readability (one clause per line)
   - Jinja template preservation
3. **Jinja handling** — Preserve and format `{{ var }}`, `{% if %}`, macros, etc.
4. **BigQuery compliance** — Keep BigQuery-specific functions intact

### Output

- Formatted `.sql` file (in-place modification)
- Formatting report showing changes made
- Any syntax errors encountered

## Usage

### Automatic (via Hook)

Every time you edit a `.sql` file in `queries/models/`, the PostToolUse hook triggers:

```
Edit .sql file → sqlfmt auto-formats → ✓ Done
```

No manual action needed — formatting happens automatically.

### Manual Invocation

```bash
# Format a single file
/sql-formatter queries/models/bilhetagem/stg_bilhetagem.sql

# Format all models in a domain
/sql-formatter queries/models/bilhetagem

# Format all models across queries
/sql-formatter queries/models
```

## Error Handling

If sqlfmt detects SQL syntax errors:

```
ERROR: Failed to format queries/models/billing/fct_invoices.sql
  Line 12: Unexpected token 'FROM'

Possible issues:
- Missing comma in SELECT clause
- Unmatched parentheses in JOIN condition
- Invalid Jinja syntax

Fix the syntax error and try again.
```

## Integration with Hooks

This skill has a **PostToolUse hook** that runs automatically:

```json
{
  "tool": "Edit",
  "glob": "queries/**/*.sql",
  "run": "cd queries && sqlfmt {file_path}",
  "description": "Auto-format SQL models with sqlfmt (MANDATORY per CLAUDE.md)"
}
```

This means:

- Every `.sql` edit in `queries/` triggers sqlfmt automatically
- No need to manually invoke the skill in most cases
- Ensures compliance with formatting standards

## Why This Matters

- **Team standardization** — Everyone's SQL looks the same
- **Readability** — Formatted SQL is easier to review and debug
- **Consistency** — Prevents formatting noise in diffs
- **Pre-commit check** — Catch formatting issues before merging
- **Refactoring** — Easier to spot issues in well-formatted code

## Configuration

sqlfmt is configured in `pyproject.toml`:

```toml
[tool.ruff]
line-length = 100

[tool.sqlfmt]
# Inherits line-length from ruff config
```

Current settings:

- **Line length**: 100 characters (from Ruff config)
- **Jinja support**: Enabled (`shandy-sqlfmt[jinjafmt]`)
- **SQL dialect**: BigQuery (default)

## Quick Reference

| Task              | Command                                             |
| ----------------- | --------------------------------------------------- |
| Format one file   | `/sql-formatter queries/models/domain/model.sql`    |
| Format a domain   | `/sql-formatter queries/models/bilhetagem`          |
| Format all models | `/sql-formatter queries/models`                     |
| (Auto on edit)    | Just edit any `.sql` file — hook runs automatically |

## Notes

- sqlfmt runs with `jinjafmt` support built-in
- All Jinja syntax is preserved and properly formatted
- BigQuery functions are fully supported
- dbt macros, variables, and refs are handled correctly
- Formatting is idempotent (running twice gives the same result)
