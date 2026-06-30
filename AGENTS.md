# AGENTS.md — interoperable agent instructions for dbt_vitao

This file mirrors the project-level operating rules for AI coding agents that do not read `CLAUDE.md`.

## Project purpose

`dbt_vitao` is a dbt utility package for reusable data-engineering macros. The initial value proposition is faster pipeline development through automatic formatting and recursive JSON flattening.

## Critical constraints

- Treat current implementation as early-stage and mostly Postgres-specific.
- Do not invent cross-platform support.
- Use dbt adapter dispatch for multi-platform macro work.
- Keep public macro APIs stable or provide backwards-compatible aliases.
- Update docs and tests with every macro change.
- Run `dbt parse` and `dbt compile` before considering work complete.

## Preferred architecture

```text
macros/
  public_api.sql
  adapters/
    default/
    postgres/
    snowflake/
    dremio/
integration_tests/
  projects/
    postgres/
    snowflake/
    dremio/
docs/
  architecture/
  adapter-support/
```

## Completion criteria

A task is done only when the code compiles, documentation is updated, and the adapter impact is clearly described.
