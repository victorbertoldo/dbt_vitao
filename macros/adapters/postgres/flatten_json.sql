{#
  `sample_size` and `null_key_cast` are accepted for signature compatibility with
  the dispatch wrapper but unused here: PostgreSQL has no compile-time key
  discovery, so mode='columns' requires an explicit `schema_override` and there is
  no inferred type to fall back on.
#}
{% macro postgres__flatten_json(
    relation,
    json_column,
    mode,
    max_depth,
    sample_size,
    schema_override,
    prefix,
    include_source_columns,
    include_json_column,
    recursive,
    outer,
    strip_quotes,
    null_key_cast
) %}

  {%- if mode == 'rows' -%}
    {{ dbt_vitao._postgres__flatten_json_rows(relation, json_column, include_source_columns, outer) }}

  {%- elif mode == 'columns' -%}
    {{ dbt_vitao._postgres__flatten_json_columns(relation, json_column, schema_override, prefix, include_source_columns, include_json_column, strip_quotes) }}

  {%- else -%}
    {{ exceptions.raise_compiler_error(
      "dbt_vitao.flatten_json: unsupported mode '" ~ mode ~ "'. Valid values: 'columns', 'rows'."
    ) }}
  {%- endif -%}

{% endmacro %}


{%- macro _postgres__flatten_json_rows(relation, json_column, include_source_columns, outer) -%}

  select
    {%- if include_source_columns %}
    src.*,
    {%- endif %}
    e.key,
    e.value,
    jsonb_typeof(e.value) as value_type
  from {{ relation }} src
  {%- if outer %}
  left join lateral jsonb_each(src."{{ json_column }}") as e(key, value) on true
  {%- else %}
  , lateral jsonb_each(src."{{ json_column }}") as e(key, value)
  {%- endif %}

{%- endmacro -%}


{%- macro _postgres__flatten_json_columns(relation, json_column, schema_override, prefix, include_source_columns, include_json_column, strip_quotes) -%}

  {%- if schema_override is none -%}
    {{ exceptions.raise_compiler_error(
      "dbt_vitao.flatten_json (postgres, mode='columns'): schema_override is required for PostgreSQL column projection mode. "
      ~ "PostgreSQL does not support compile-time key discovery. "
      ~ "Pass a list of path strings or dicts: "
      ~ "schema_override=['user.name', 'user.email'] "
      ~ "or schema_override=[{'path': 'user.name', 'cast_type': 'text', 'alias': 'user_name'}]"
    ) }}
  {%- endif -%}

  {%- set col_prefix = prefix ~ '_' if prefix else '' -%}
  {%- set projections = [] -%}

  {%- for entry in schema_override -%}
    {%- if entry is string -%}
      {%- set path = entry -%}
      {%- set cast_type = 'text' -%}
      {%- set alias = col_prefix ~ dbt_vitao.normalize_alias(entry) -%}
    {%- else -%}
      {%- set path = entry.path -%}
      {%- set cast_type = entry.get('cast_type', 'text') -%}
      {%- set alias = col_prefix ~ entry.get('alias', dbt_vitao.normalize_alias(path)) -%}
    {%- endif -%}
    {%- set expr = dbt_vitao.json_extract_scalar('"' ~ json_column ~ '"', path, cast_type) | trim -%}
    {%- if strip_quotes and cast_type in ('text', 'string', 'varchar') -%}
      {%- set expr = "trim(" ~ expr ~ ", '\"')" -%}
    {%- endif -%}
    {%- do projections.append(expr ~ ' as ' ~ alias) -%}
  {%- endfor -%}

  select
    {%- if include_source_columns %}
      {%- if include_json_column %}
    src.*,
      {%- else -%}
        {#- Postgres has no `SELECT * EXCLUDE (...)`, so explicitly list every source
           column except json_column via relation introspection. Only works when
           `relation` is an actual Relation object (ref/source), not a raw subquery
           string -- fine here since schema_override already requires explicit paths. #}
        {%- for col in adapter.get_columns_in_relation(relation) if col.name | lower != json_column | lower %}
    src."{{ col.name }}",
        {%- endfor %}
      {%- endif %}
    {%- endif %}
    {{ projections | join(',\n    ') }}
  from {{ relation }} src

{%- endmacro -%}
