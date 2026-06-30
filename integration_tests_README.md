# integration_tests/README.md

Recommended future location: `integration_tests/README.md`.

Create small dbt projects that exercise the package macros.

Suggested structure:

```text
integration_tests/
  postgres_project/
  snowflake_project/
  dremio_project/
  seeds/
    json_edge_cases.csv
```

Minimum CI commands per fixture project:

```bash
dbt deps
dbt parse
dbt compile
dbt test
```

Start with compile tests even before full warehouse execution is automated.
