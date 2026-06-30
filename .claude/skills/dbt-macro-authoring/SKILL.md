---
name: dbt-macro-authoring
description: Author and refactor dbt package macros with dispatch, documentation, compatibility aliases, and test fixtures.
---

# dbt macro authoring skill

Use this skill when creating, refactoring, or reviewing dbt macros in `dbt_vitao`.

## Required approach

1. Identify whether the macro is public or internal.
2. For public behavior, create a wrapper macro that calls `adapter.dispatch()`.
3. Put database-specific SQL in adapter-specific implementations.
4. Keep the default implementation conservative.
5. Add YAML docs for every public macro.
6. Add compile examples or tests.
7. Preserve backwards compatibility through aliases when possible.

## Preferred pattern

```sql
{% macro v_flatten_json(relation, column_name, max_depth=5) -%}
  {{ return(adapter.dispatch('v_flatten_json', 'dbt_vitao')(relation, column_name, max_depth)) }}
{%- endmacro %}

{% macro default__v_flatten_json(relation, column_name, max_depth=5) -%}
  {{ exceptions.raise_compiler_error('v_flatten_json is not implemented for adapter: ' ~ target.type) }}
{%- endmacro %}
```

## Documentation pattern

```yaml
version: 2

macros:
  - name: v_flatten_json
    description: Recursively flattens a JSON-like column into SQL select expressions.
    arguments:
      - name: relation
        type: relation
        description: dbt relation containing the JSON column.
      - name: column_name
        type: string
        description: JSON column to flatten.
      - name: max_depth
        type: integer
        description: Maximum recursion depth.
```

## Anti-patterns

- Public macro with Postgres-only SQL.
- Hidden `run_query()` sampling without documentation.
- Macro names that expose implementation details such as `cool__` for public APIs.
- No tests or examples.
- Silent success on unsupported adapters.
