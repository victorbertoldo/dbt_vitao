# dbt_vitao

A utility dbt package providing adapter-aware macros for column formatting,
JSON flattening, and storage optimization.

Supports **PostgreSQL** and **Snowflake**. Raises clear compiler errors on unsupported adapters.

---

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/victorbertoldo/dbt_vitao"
    revision: 0.1.0
```

Then:

```bash
dbt deps
```

---

## Package variables

Configure in your `dbt_project.yml`:

```yaml
vars:
  dbt_vitao:
    string_formatting: trim_only  # none | trim_only | lower | upper | initcap
```

**Default is `trim_only`.**

> **Warning**: `initcap` will corrupt IDs, emails, URLs, codes, and
> case-sensitive business values. Use it only if every string column in your
> relation is safe to title-case.

---

## Public API

### `format_column`

Format a single column expression based on its data type.

```jinja
{{ dbt_vitao.format_column('email', 'text', 'formatted_email', {'string_formatting': 'lower'}) }}
-- produces: lower(trim("email")) as formatted_email
```

### `format_relation`

Generate a formatted column list for an entire relation.

```sql
-- models/staging/stg_customers.sql
select
{{ dbt_vitao.format_relation(source('raw', 'customers'), prefix='stg_') }}
from {{ source('raw', 'customers') }}
```

Options are passed through to `format_column`:

```jinja
{{ dbt_vitao.format_relation(ref('raw_customers'), prefix='', options={'string_formatting': 'lower'}) }}
```

### `flatten_json`

#### mode='columns' — wide projection

```sql
-- models/staging/stg_events.sql
{{ dbt_vitao.flatten_json(
    relation=source('raw', 'events'),
    json_column='payload',
    mode='columns',
    schema_override=[
        {'path': 'customer.name',  'cast_type': 'text',    'alias': 'customer_name'},
        {'path': 'customer.email', 'cast_type': 'text',    'alias': 'customer_email'},
        {'path': 'flags.active',   'cast_type': 'boolean', 'alias': 'is_active'}
    ],
    include_source_columns=true
) }}
```

Snowflake uses colon-separated paths:

```jinja
schema_override=[
    {'path': 'customer:name',  'cast_type': 'string', 'alias': 'customer_name'},
    {'path': 'flags:active',   'cast_type': 'boolean', 'alias': 'is_active'}
]
```

#### mode='rows' — lateral explosion

```sql
-- models/staging/stg_events_exploded.sql
{{ dbt_vitao.flatten_json(
    relation=source('raw', 'events'),
    json_column='payload',
    mode='rows',
    recursive=true,
    outer=true,
    include_source_columns=false
) }}
```

### `json_extract_scalar`

```jinja
{{ dbt_vitao.json_extract_scalar('payload', 'customer.name', 'text') }}
-- postgres:   (payload -> 'customer' ->> 'name')::text
-- snowflake:  payload:customer:name::string
```

### `json_extract_object`

```jinja
{{ dbt_vitao.json_extract_object('payload', 'customer.address') }}
-- postgres:   payload -> 'customer' -> 'address'
-- snowflake:  get_path(payload, 'customer:address')
```

### `json_extract_variant`

Snowflake only. Returns a raw VARIANT value.

```jinja
{{ dbt_vitao.json_extract_variant('payload', 'customer:orders') }}
-- snowflake:  payload:customer:orders
```

### `optimize_relation`

Use in a model post-hook.

```yaml
# dbt_project.yml
models:
  my_project:
    my_model:
      post-hook:
        - "{{ dbt_vitao.optimize_relation(this, strategy='index', columns=['user_id'], options={'index_type': 'btree'}) }}"
```

Snowflake:

```yaml
post-hook:
  - "{{ dbt_vitao.optimize_relation(this, strategy='search_optimization', columns=['payload:user:uuid'], options={'mode': 'variant'}) }}"
```

> Optimization is always **opt-in**. Never applied automatically.

### `safe_cast`

```jinja
{{ dbt_vitao.safe_cast('my_column', 'integer') }}
-- postgres:   (my_column)::integer
-- snowflake:  try_cast(my_column as integer)
```

---

## Adapter support matrix

See [docs/adapter-support.md](docs/adapter-support.md) for the full matrix.

Quick reference:

| Macro | PostgreSQL | Snowflake |
|---|:---:|:---:|
| `format_column` | yes | yes |
| `format_relation` | yes | yes |
| `flatten_json` columns | yes (requires schema_override) | yes |
| `flatten_json` rows | yes | yes |
| `json_extract_scalar` | yes | yes |
| `json_extract_object` | yes | yes |
| `json_extract_variant` | no | yes |
| `get_relation_columns` | yes | yes |
| `optimize_relation` index | yes | no |
| `optimize_relation` cluster_by | no | yes |
| `optimize_relation` search_optimization | no | yes |
| `safe_cast` | yes | yes |

---

## Legacy compatibility

The following macros from previous versions remain available:

| Legacy name | Replacement |
|---|---|
| `cool__formatter` | `dbt_vitao.format_relation()` |
| `unnest__json` | `dbt_vitao.flatten_json()` |
| `cool__indexer` | `dbt_vitao.optimize_relation()` |

`cool__formatter` now delegates to `format_relation` with `string_formatting='initcap'`
for backward compatibility. **JSON columns are no longer auto-expanded** — use
`flatten_json()` explicitly.

---

## dbt compatibility

```
require-dbt-version: [">=1.2.0", "<2.0.0"]
```
