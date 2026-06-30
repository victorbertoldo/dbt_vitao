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


{#
  Maps Snowflake typeof() return values to SQL cast types.
  typeof() returns: FIXED, REAL, TEXT, BOOLEAN, OBJECT, ARRAY, NULL_VALUE, etc.
#}
{%- macro _snowflake__type_to_cast(sf_typeof) -%}
  {%- if sf_typeof == 'FIXED' -%}number
  {%- elif sf_typeof == 'REAL' -%}float
  {%- elif sf_typeof == 'TEXT' -%}string
  {%- elif sf_typeof == 'BOOLEAN' -%}boolean
  {%- else -%}variant
  {%- endif -%}
{%- endmacro -%}


{#
  Build a bracket-notation Snowflake path expression from a column name and a list of key segments.
  Bracket notation handles keys with spaces, hyphens, colons, and other special characters.
  Example: ('RAW_DATA', ['customer', 'address', 'city']) -> RAW_DATA['customer']['address']['city']
#}
{%- macro _snowflake__bracket_path(column_name, key_parts) -%}
  {%- set segments = [column_name] -%}
  {%- for part in key_parts -%}
    {%- do segments.append("['" ~ part ~ "']") -%}
  {%- endfor -%}
  {{ return(segments | join('')) }}
{%- endmacro -%}


{%- macro _snowflake__flatten_json_rows(relation, json_column, include_source_columns, recursive, outer) -%}

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
      input    => src.{{ json_column }},
      recursive => {{ recursive | lower }},
      outer     => {{ outer | lower }}
  ) f

{%- endmacro -%}


{%- macro _snowflake__flatten_json_columns(
    relation, json_column, schema_override,
    prefix, include_source_columns, sample_size, max_depth
) -%}

  {%- set col_prefix = prefix ~ '_' if prefix else '' -%}

  {# ------------------------------------------------------------------ #}
  {# PATH A: user provided schema_override — no discovery needed         #}
  {# ------------------------------------------------------------------ #}
  {%- if schema_override is not none -%}

    {%- set projections = [] -%}
    {%- for entry in schema_override -%}
      {%- if entry is string -%}
        {%- set path      = entry -%}
        {%- set cast_type = 'string' -%}
        {%- set alias     = col_prefix ~ dbt_vitao.normalize_alias(entry) -%}
      {%- else -%}
        {%- set path      = entry.path -%}
        {%- set cast_type = entry.get('cast_type', 'string') -%}
        {%- set alias     = col_prefix ~ entry.get('alias', dbt_vitao.normalize_alias(path)) -%}
      {%- endif -%}
      {%- set key_parts = (path | replace('.', ':')).split(':') -%}
      {%- set expr = dbt_vitao._snowflake__bracket_path(json_column, key_parts) | trim -%}
      {%- do projections.append('src.' ~ expr ~ '::' ~ cast_type ~ ' as ' ~ alias) -%}
    {%- endfor -%}

    select
      {%- if include_source_columns %}
      src.*,
      {%- endif %}
      {{ projections | join(',\n      ') }}
    from {{ relation }} src

  {# ------------------------------------------------------------------ #}
  {# PATH B: auto-discovery using OBJECT_KEYS + run_query               #}
  {# ------------------------------------------------------------------ #}
  {%- else -%}

    {%- if not execute -%}
      select null as _dbt_vitao_placeholder

    {%- else -%}

      {# --- Level 1: discover all top-level keys and their types --- #}
      {%- set l1_sql -%}
        select distinct
          f.value::string                                     as key_name,
          typeof(src.{{ json_column }}[f.value::string])     as value_type
        from (
          select {{ json_column }}
          from {{ relation }}
          where {{ json_column }} is not null
            and typeof({{ json_column }}) = 'OBJECT'
          limit {{ sample_size }}
        ) src,
        lateral flatten(
          input => object_keys(src.{{ json_column }}),
          outer => false
        ) f
        order by key_name
      {%- endset -%}

      {%- set l1_rows = run_query(l1_sql).rows -%}

      {# --- Level 2: for every OBJECT-valued L1 key, discover its children (if max_depth > 1) --- #}
      {%- set l2_data = {} -%}
      {%- if max_depth > 1 -%}
        {%- set l2_sql -%}
          select distinct
            l1.value::string                                                      as parent_key,
            l2.value::string                                                      as child_key,
            typeof(src.{{ json_column }}[l1.value::string][l2.value::string])    as value_type
          from (
            select {{ json_column }}
            from {{ relation }}
            where {{ json_column }} is not null
              and typeof({{ json_column }}) = 'OBJECT'
            limit {{ sample_size }}
          ) src,
          lateral flatten(
            input => object_keys(src.{{ json_column }}),
            outer => false
          ) l1,
          lateral flatten(
            input => object_keys(src.{{ json_column }}[l1.value::string]),
            outer => false
          ) l2
          where typeof(src.{{ json_column }}[l1.value::string]) = 'OBJECT'
          order by parent_key, child_key
        {%- endset -%}

        {%- for row in run_query(l2_sql).rows -%}
          {%- set pk = row[0] -%}
          {%- if pk not in l2_data -%}
            {%- do l2_data.update({pk: []}) -%}
          {%- endif -%}
          {%- do l2_data[pk].append({
            'key':  row[1],
            'cast': dbt_vitao._snowflake__type_to_cast(row[2])
          }) -%}
        {%- endfor -%}
      {%- endif -%}

      {# --- Build the projection list --- #}
      {%- set projections = [] -%}
      {%- for row in l1_rows -%}
        {%- set key   = row[0] -%}
        {%- set vtype = row[1] -%}

        {%- if vtype == 'OBJECT' and max_depth > 1 and key in l2_data -%}
          {# Expand the nested object into individual child columns #}
          {%- for child in l2_data[key] -%}
            {%- set child_alias = col_prefix ~ dbt_vitao.normalize_alias(key ~ '_' ~ child.key) -%}
            {%- do projections.append(
              "src." ~ json_column ~ "['" ~ key ~ "']['" ~ child.key ~ "']::" ~ child.cast ~ " as " ~ child_alias
            ) -%}
          {%- endfor -%}

        {%- else -%}
          {# Scalar, array, or OBJECT at max depth: project as single column #}
          {%- set cast  = dbt_vitao._snowflake__type_to_cast(vtype) -%}
          {%- set alias = col_prefix ~ dbt_vitao.normalize_alias(key) -%}
          {%- do projections.append(
            "src." ~ json_column ~ "['" ~ key ~ "']::" ~ cast ~ " as " ~ alias
          ) -%}
        {%- endif -%}

      {%- endfor -%}

      select
        {%- if include_source_columns %}
        src.*,
        {%- endif %}
        {%- if projections | length > 0 %}
        {{ projections | join(',\n        ') }}
        {%- else %}
        null as _dbt_vitao_no_keys_discovered
        {%- endif %}
      from {{ relation }} src

    {%- endif -%}

  {%- endif -%}

{%- endmacro -%}
