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
    outer,
    strip_quotes
) %}

  {%- if mode == 'rows' -%}
    {{ dbt_vitao._snowflake__flatten_json_rows(relation, json_column, include_source_columns, recursive, outer) }}

  {%- elif mode == 'columns' -%}
    {{ dbt_vitao._snowflake__flatten_json_columns(relation, json_column, schema_override, prefix, include_source_columns, sample_size, max_depth, strip_quotes) }}

  {%- else -%}
    {{ exceptions.raise_compiler_error(
      "dbt_vitao.flatten_json: unsupported mode '" ~ mode ~ "'. Valid values: 'columns', 'rows'."
    ) }}
  {%- endif -%}

{% endmacro %}


{#
  Maps Snowflake typeof() return values to SQL cast types.
  typeof() has been observed to return both short and long forms for the same
  underlying type (e.g. INTEGER and FIXED for numbers, VARCHAR and TEXT for
  strings) depending on account/version -- match all known synonyms rather than
  a single literal, since an unmatched type silently falls back to `variant`
  (uncast), which then displays with native JSON quoting for strings.
#}
{%- macro _snowflake__type_to_cast(sf_typeof) -%}
  {%- if sf_typeof in ('FIXED', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'DECIMAL', 'NUMBER', 'NUMERIC') -%}number
  {%- elif sf_typeof in ('REAL', 'FLOAT', 'DOUBLE') -%}float
  {%- elif sf_typeof in ('TEXT', 'VARCHAR', 'CHAR', 'STRING') -%}string
  {%- elif sf_typeof == 'BOOLEAN' -%}boolean
  {%- else -%}variant
  {%- endif -%}
{%- endmacro -%}


{#
  Casts a Snowflake path expression to the target type. When strip_quotes is true and the
  target is 'string', wraps the cast in trim(..., '"') to remove stray leading/trailing quote
  characters that appear when source values are double-encoded (e.g. a string whose literal
  content is "ESTACIONAMENTO", quotes included, rather than the unquoted value).
#}
{%- macro _snowflake__cast_expr(path_expr, cast_type, strip_quotes) -%}
  {%- set casted = path_expr ~ '::' ~ cast_type -%}
  {%- if strip_quotes and cast_type == 'string' -%}
    {{ return("trim(" ~ casted ~ ", '\"')") }}
  {%- else -%}
    {{ return(casted) }}
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
    prefix, include_source_columns, sample_size, max_depth, strip_quotes
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
      {%- set casted = dbt_vitao._snowflake__cast_expr('src.' ~ expr, cast_type, strip_quotes) -%}
      {%- do projections.append(casted ~ ' as ' ~ alias) -%}
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

      {# Same type-inconsistency issue as level 2 below: a sampled key can disagree on
         type across rows (e.g. NULL for some, TEXT/OBJECT for others), so `distinct
         key_name, value_type` can yield more than one row per key. Dedupe here too,
         preferring the first non-NULL_VALUE type seen. #}
      {%- set l1_seen = {} -%}
      {%- for row in run_query(l1_sql).rows -%}
        {%- set key   = row[0] -%}
        {%- set vtype = row[1] -%}
        {%- set existing = l1_seen.get(key) -%}
        {%- if existing is none or existing == 'NULL_VALUE' -%}
          {%- do l1_seen.update({key: vtype}) -%}
        {%- endif -%}
      {%- endfor -%}
      {%- set l1_rows = [] -%}
      {%- for key, vtype in l1_seen.items() -%}
        {%- do l1_rows.append([key, vtype]) -%}
      {%- endfor -%}

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

        {# Sampled rows can disagree on a child key's type (e.g. NULL for some rows,
           OBJECT/TEXT for others), so `distinct parent_key, child_key, value_type`
           above can yield more than one row per child key. Dedupe here, preferring
           the first non-NULL_VALUE type seen, so each child key projects exactly
           one column. #}
        {%- set l2_seen = {} -%}
        {%- for row in run_query(l2_sql).rows -%}
          {%- set pk    = row[0] -%}
          {%- set ck    = row[1] -%}
          {%- set vtype = row[2] -%}
          {%- if pk not in l2_seen -%}
            {%- do l2_seen.update({pk: {}}) -%}
          {%- endif -%}
          {%- set existing = l2_seen[pk].get(ck) -%}
          {%- if existing is none or existing == 'NULL_VALUE' -%}
            {%- do l2_seen[pk].update({ck: vtype}) -%}
          {%- endif -%}
        {%- endfor -%}

        {%- for pk, children in l2_seen.items() -%}
          {%- set entries = [] -%}
          {%- for ck, vtype in children.items() -%}
            {%- do entries.append({
              'key':  ck,
              'cast': dbt_vitao._snowflake__type_to_cast(vtype)
            }) -%}
          {%- endfor -%}
          {%- do l2_data.update({pk: entries}) -%}
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
            {%- set child_path  = "src." ~ json_column ~ "['" ~ key ~ "']['" ~ child.key ~ "']" -%}
            {%- set casted = dbt_vitao._snowflake__cast_expr(child_path, child.cast, strip_quotes) -%}
            {%- do projections.append(casted ~ " as " ~ child_alias) -%}
          {%- endfor -%}

        {%- else -%}
          {# Scalar, array, or OBJECT at max depth: project as single column #}
          {%- set cast     = dbt_vitao._snowflake__type_to_cast(vtype) -%}
          {%- set alias    = col_prefix ~ dbt_vitao.normalize_alias(key) -%}
          {%- set key_path = "src." ~ json_column ~ "['" ~ key ~ "']" -%}
          {%- set casted   = dbt_vitao._snowflake__cast_expr(key_path, cast, strip_quotes) -%}
          {%- do projections.append(casted ~ " as " ~ alias) -%}
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
