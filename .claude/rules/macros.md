---
description: Rules for dbt macro development in dbt_vitao
paths:
  - "macros/**/*.sql"
  - "macros/**/*.yml"
---

# dbt macro rules

- Public macros should be thin wrappers that call `adapter.dispatch()`.
- Adapter SQL belongs in adapter-specific implementations.
- Preserve existing macro names with compatibility aliases when renaming.
- Add macro YAML documentation for every public macro.
- Every macro must have at least one compile-time example in an integration fixture.
- Avoid hidden `run_query()` behavior unless the macro is clearly documented as requiring execution context.
- Guard all `run_query()` access with `if execute`.
- Return deterministic SQL. Avoid relying on arbitrary `limit 1` sampling for production contracts unless the user explicitly accepts probabilistic schema inference.
