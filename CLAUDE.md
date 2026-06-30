# CLAUDE.md — dbt_vitao

## Project identity

`dbt_vitao` is a utility dbt package. Its purpose is to reduce data pipeline development time by providing reusable macros for formatting, JSON shape discovery, recursive JSON flattening, table creation behavior, and database-specific utilities.

The current production repository is expected to live at:

- https://github.com/victorbertoldo/dbt_vitao

## Current implementation reality

Treat the package as early-stage.

Known current implementation patterns:

- The package currently contains Postgres-oriented macros under `macros/db/postgres/**`.
- Existing macro families include:
  - `utils`: automatic column formatting and column exclusion.
  - `parser`: recursive JSON unnesting / flattening.
  - `optimizers`: index creation helpers.
  - `tb_types`: custom Postgres table creation behavior.
- Do not claim Snowflake, Dremio, BigQuery, Spark, or Databricks support unless adapter-specific macros and tests exist.
- Any multi-platform work must use dbt adapter dispatch and explicit adapter-specific implementations.

## Architectural direction

The package should mature from a Postgres utility package into a cross-adapter data-engineering utility package.

Target architecture:

```text
Public macro API
  -> dispatch wrapper macro
    -> default implementation
    -> postgres__ implementation
    -> snowflake__ implementation
    -> dremio__ implementation
    -> other adapters later
  -> integration tests per adapter
  -> docs examples per adapter
```

Prefer stable public macro names such as:

- `v_format_columns(...)`
- `v_flatten_json(...)`
- `v_except_columns(...)`
- `v_safe_cast(...)`
- `v_create_optimized_table_as(...)`

Adapter-specific implementation names must follow dbt dispatch naming:

- `default__macro_name`
- `postgres__macro_name`
- `snowflake__macro_name`
- `dremio__macro_name`

## Non-negotiable dbt rules

1. Do not put warehouse-specific SQL directly in a public macro unless it is explicitly adapter-scoped.
2. Use `adapter.dispatch()` for cross-adapter behavior.
3. Add or update macro documentation in YAML for every public macro.
4. Add integration examples or tests for every macro behavior change.
5. Do not break existing macro names without adding backwards-compatible aliases.
6. Do not execute destructive SQL in generated code unless the user explicitly requested it.
7. Do not change dbt materialization internals unless the impact on dbt version compatibility is documented.
8. Prefer compile-time validation errors over silently producing invalid SQL.

## Adapter priority

Current priority order:

1. Postgres compatibility preservation.
2. Snowflake support, especially JSON/VARIANT flattening and safe casting.
3. Dremio support, especially JSON extraction, Iceberg-compatible CTAS/merge behavior, and lakehouse SQL limitations.
4. BigQuery / Databricks later.

## Package maturity goals

Before calling this package production-ready, ensure:

- Macro API is stable and documented.
- Adapter dispatch structure is consistent.
- Snowflake and Dremio implementations are tested separately.
- JSON flattening supports nested objects, arrays, reserved characters, and optional fields.
- Examples exist for source relation, ref relation, and inline query use cases.
- CI runs `dbt deps`, `dbt parse`, `dbt compile`, and adapter-specific smoke tests.
- Release notes and semantic versioning are maintained.

## Commands to prefer

Use these commands when available:

```bash
dbt deps
dbt parse
dbt compile
dbt test
```

For package development, prefer compiling a small fixture project rather than changing a production model directly.

## Review checklist before making changes

Before editing macros:

1. Identify the public macro API.
2. Identify adapter-specific SQL differences.
3. Check whether dispatch is already used.
4. Preserve backwards compatibility.
5. Add or update docs.
6. Add compile tests / fixture models.
7. Run parse and compile.

## Communication style for this repository

When reporting findings:

- Be direct about what is implemented versus only planned.
- Separate "current state", "risk", and "recommended implementation".
- Use data-platform terminology: adapter portability, macro API, dispatch, fixtures, compile tests, integration tests, package release readiness.
- Do not overstate maturity.
