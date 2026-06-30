---
name: dbt-adapter-portability
description: Port dbt_vitao macros across Postgres, Snowflake, Dremio, and future adapters using dbt dispatch and adapter-specific SQL contracts.
---

# dbt adapter portability skill

Use this skill when making a macro work on more than one database engine.

## Process

1. Define the logical contract of the macro in adapter-neutral language.
2. Create a support matrix:
   - supported
   - partially supported
   - unsupported
   - not yet tested
3. Implement through dbt dispatch.
4. Add adapter-specific SQL files.
5. Add adapter-specific examples or tests.
6. Document limitations clearly.

## Adapter target structure

```text
macros/
  public/
    json.sql
    formatting.sql
  adapters/
    default/
    json.sql
    postgres/
    json.sql
    snowflake/
    json.sql
    dremio/
    json.sql
```

## Snowflake checklist

- Use `VARIANT` where JSON is native.
- Use `TRY_CAST` or `TRY_TO_*` patterns for safe conversions.
- Use `LATERAL FLATTEN` for arrays when runtime flattening is needed.
- Confirm identifier behavior with quoted and unquoted names.

## Dremio checklist

- Verify JSON function availability against the exact Dremio version.
- Avoid assuming Postgres operators like `->` and `->>` exist.
- Keep Iceberg and lakehouse table behavior in mind.
- Prefer safe extraction functions/macros for optional fields.

## Output format

Always report:

```text
Macro:
Current behavior:
Adapter risks:
Implementation plan:
Files to change:
Tests to add:
Unsupported behavior:
```
