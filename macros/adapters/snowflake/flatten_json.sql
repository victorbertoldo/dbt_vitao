{% macro snowflake__flatten_json(
    relation,
    json_column,
    mode,
    max_depth,
    sample_size,
    schema_override,
    prefix,
    include_source_columns,
    recursive,
    outer
) %}

  {%- if mode == 'rows' -%}
    {{ dbt_vitao._snowflake__flatten_json_rows(relation, json_column, include_source_columns, recursive, outer) }}

  {%- elif mode == 'columns' -%}
    {{ dbt_vitao._snowflake__flatten_json_columns(relation, json_column, schema_override, prefix, include_source_columns, sample_size, max_depth) }}

  {%- else -%}
    {{ exceptions.raise_compiler_error(
      "dbt_vitao.flatten_json: unsupported mode '" ~ mode ~ "'. Valid values: 'columns', 'rows'."
    ) }}
  {%- endif -%}

{% endmacro %}


{%- macro _snowflake__flatten_json_rows(relation, json_column, include_source_columns, recursive, outer) -%}

  {%- set input_expr = 'src.' ~ json_column -%}

  select
    {%- if include_source_columns %}
    src.*,
    {%- endif %}
    f.key,
    f.path,
    f.index,
    f.value,
    typeof(f.value) as value_type
  from {{ relation }} src
  , lateral flatten(
      input => {{ input_expr }},
      recursive => {{ recursive | lower }},
      outer => {{ outer | lower }}
  ) f

{%- endmacro -%}


{%- macro _snowflake__flatten_json_columns(relation, json_column, schema_override, prefix, include_source_columns, sample_size, max_depth) -%}

  {%- set col_prefix = prefix ~ '_' if prefix else '' -%}

  {%- if schema_override is not none -%}
    {%- set projections = [] -%}
    {%- for entry in schema_override -%}
      {%- if entry is string -%}
        {%- set path = entry -%}
        {%- set cast_type = 'string' -%}
        {%- set alias = col_prefix ~ dbt_vitao.normalize_alias(entry | replace(':', '_')) -%}
      {%- else -%}
        {%- set path = entry.path -%}
        {%- set cast_type = entry.get('cast_type', 'string') -%}
        {%- set alias = col_prefix ~ entry.get('alias', dbt_vitao.normalize_alias(path | replace(':', '_'))) -%}
      {%- endif -%}
      {%- set sf_path = path | replace('.', ':') -%}
      {%- do projections.append('src.' ~ json_column ~ ':' ~ sf_path ~ '::' ~ cast_type ~ ' as ' ~ alias) -%}
    {%- endfor -%}

  select
    {%- if include_source_columns %}
    src.*,
    {%- endif %}
    {{ projections | join(',\n    ') }}
  from {{ relation }} src

  {%- else -%}

    {%- if not execute -%}
      select null as _dbt_vitao_placeholder
    {%- else -%}

      {%- set discover_sql -%}
        select distinct
          f.value::string as key_name
        from {{ relation }} src
        , lateral flatten(
            input => object_keys(
              case
                when typeof(src.{{ json_column }}) in ('OBJECT', 'ARRAY', 'VARIANT')
                  then src.{{ json_column }}
                else try_parse_json(src.{{ json_column }}::string)
              end
            )
        ) f
        limit {{ sample_size }}
      {%- endset -%}

      {%- set key_results = run_query(discover_sql) -%}
      {%- set discovered_keys = key_results.columns[0].values() -%}

      {%- set projections = [] -%}
      {%- for key in discovered_keys -%}
        {%- set alias = col_prefix ~ dbt_vitao.normalize_alias(key) -%}
        {%- do projections.append('src.' ~ json_column ~ ':' ~ key ~ '::string as ' ~ alias) -%}
      {%- endfor -%}

  select
    {%- if include_source_columns %}
    src.*,
    {%- endif %}
    {%- if projections | length > 0 %}
    {{ projections | join(',\n    ') }}
    {%- else %}
    null as _dbt_vitao_no_keys_discovered
    {%- endif %}
  from {{ relation }} src

    {%- endif -%}

  {%- endif -%}

{%- endmacro -%}
