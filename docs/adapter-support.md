# Adapter Support Matrix

`dbt_vitao` version 0.1.0.

| Macro | PostgreSQL | Snowflake | Notes |
|---|:---:|:---:|---|
| `format_column` | Supported | Supported | Adapter-specific type mapping and string quoting |
| `format_relation` | Supported | Supported | Configurable string formatting |
| `flatten_json` (mode=columns) | Supported | Supported | Postgres requires `schema_override`; Snowflake supports auto-discovery |
| `flatten_json` (mode=rows) | Supported | Supported | Postgres uses `jsonb_each`; Snowflake uses `LATERAL FLATTEN` |
| `json_extract_scalar` | Supported | Supported | Postgres: `->>` operator; Snowflake: `:path::type` |
| `json_extract_object` | Supported | Supported | Postgres: `->` operator; Snowflake: `GET_PATH()` |
| `json_extract_variant` | Not available | Supported | Snowflake-native VARIANT; raises error on Postgres |
| `get_relation_columns` | Supported | Supported | Delegates to `adapter.get_columns_in_relation()` |
| `optimize_relation` (index) | Supported | Not available | Raises error if called on Snowflake |
| `optimize_relation` (cluster_by) | Not available | Supported | Raises error if called on Postgres |
| `optimize_relation` (search_optimization) | Not available | Supported | Raises error if called on Postgres |
| `safe_cast` | Supported | Supported | Postgres: `::type`; Snowflake: `TRY_CAST(... AS type)` |

## Not yet supported

The following adapters are not implemented and will raise a clear compiler error:

- Dremio
- BigQuery
- Databricks / Spark
- Redshift
- Trino / Starburst

Do not attempt to use `dbt_vitao` macros on these adapters without first adding
adapter-specific dispatch implementations.

## String formatting behavior

`format_column` and `format_relation` accept a `string_formatting` option
that controls how string/text columns are transformed.

```yaml
# dbt_project.yml (consuming project)
vars:
  dbt_vitao:
    string_formatting: trim_only
```

| Value | SQL produced |
|---|---|
| `none` | `column_name` |
| `trim_only` | `trim(column_name)` |
| `lower` | `lower(trim(column_name))` |
| `upper` | `upper(trim(column_name))` |
| `initcap` | `initcap(trim(column_name))` |

**Default is `trim_only`** for all adapters.

**Warning**: do not use `initcap` unless you understand the impact.
`initcap` will transform `"AbC-001-XyZ"` → `"Abc-001-Xyz"`, corrupt emails,
change URL casing, and modify case-sensitive business identifiers.

## JSON flattening modes

### mode='columns'

Produces a wide SELECT projection:

```sql
select
  src.*,
  src.payload:customer:name::string as customer_name,
  src.payload:customer:email::string as customer_email,
  ...
from my_table src
```

Best for: staging models, known schema, stable key structure.

### mode='rows'

Produces a lateral row explosion:

```sql
select
  f.key,
  f.path,
  f.value,
  typeof(f.value) as value_type
from my_table src
, lateral flatten(input => src.payload, recursive => true, outer => true) f
```

Best for: schema discovery, arrays, key-value profiling, audit models.

## Snowflake JSON limitations

1. Auto-discovery (`schema_override=none`) samples only top-level keys.
   Sparse or polymorphic JSON may produce incomplete column sets.
2. Production models should use `schema_override` for deterministic output.
3. Arbitrary array indexes are not projected in columns mode.
   Use `mode='rows'` to explode arrays.
4. `VARIANT`, `OBJECT`, and `ARRAY` types are returned as-is by `format_column`.
   They are not cast to string automatically.

## PostgreSQL JSON limitations

1. `flatten_json` with `mode='columns'` requires `schema_override`.
   PostgreSQL does not support compile-time key discovery from this package.
2. `json_extract_variant` raises a compiler error on PostgreSQL.
3. `jsonb_each` in rows mode returns only top-level keys. Recursive flattening
   of nested objects requires multiple joins or recursive CTEs (not automated here).
