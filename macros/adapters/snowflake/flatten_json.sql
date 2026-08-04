{% macro snowflake__flatten_json(
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
    strip_quotes
) %}

  {%- if mode == 'rows' -%}
    {{ dbt_vitao._snowflake__flatten_json_rows(relation, json_column, include_source_columns, recursive, outer) }}

  {%- elif mode == 'columns' -%}
    {{ dbt_vitao._snowflake__flatten_json_columns(relation, json_column, schema_override, prefix, include_source_columns, include_json_column, sample_size, max_depth, strip_quotes) }}

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
    {%- do segments.append("['" ~ dbt_vitao._snowflake__escape_key(part) ~ "']") -%}
  {%- endfor -%}
  {{ return(segments | join('')) }}
{%- endmacro -%}


{#
  Escapes a raw JSON key for safe interpolation inside a single-quoted Snowflake
  bracket-path segment (e.g. RAW_DATA['<key>']). Source keys are arbitrary text (survey
  questions, free-form labels) and can contain literal apostrophes, which would otherwise
  terminate the string literal early and produce invalid SQL.
#}
{%- macro _snowflake__escape_key(key) -%}
  {{ return(key | replace("'", "''")) }}
{%- endmacro -%}


{#
  Returns `alias` unchanged if not already present in `used_aliases`, otherwise appends
  `_2`, `_3`, ... until unique. Mutates `used_aliases` (a dict used as a set) to record
  whichever alias is returned. Jinja has no `while` loop, so this recurses on collision --
  fine in practice since real collisions are a handful of near-duplicate keys, not hundreds.
#}
{%- macro _snowflake__dedupe_alias(alias, used_aliases, n=1) -%}
  {%- set candidate = alias if n == 1 else (alias ~ '_' ~ n) -%}
  {%- if candidate in used_aliases -%}
    {{ return(dbt_vitao._snowflake__dedupe_alias(alias, used_aliases, n + 1)) }}
  {%- else -%}
    {%- do used_aliases.update({candidate: true}) -%}
    {{ return(candidate) }}
  {%- endif -%}
{%- endmacro -%}


{#
  Discovers the immediate child keys (and their Snowflake typeof()) of the object at
  `parent_path_expr` (an already-escaped bracket-path string, e.g. "['user']['address']",
  or '' for the root object). Sampled rows can disagree on a key's type (e.g. NULL for
  some rows, OBJECT/TEXT for others) -- dedupes to one entry per key, preferring the
  first non-NULL_VALUE type seen, so callers never see the same key twice.
#}
{%- macro _snowflake__discover_keys(relation, json_column, parent_path_expr, sample_size) -%}
  {%- set sql -%}
    select distinct
      f.value::string                                                as key_name,
      typeof(src.{{ json_column }}{{ parent_path_expr }}[f.value::string])    as value_type
    from (
      select {{ json_column }}
      from {{ relation }}
      where {{ json_column }} is not null
        and typeof({{ json_column }}) = 'OBJECT'
      limit {{ sample_size }}
    ) src,
    lateral flatten(
      input => object_keys(src.{{ json_column }}{{ parent_path_expr }}),
      outer => false
    ) f
    {%- if parent_path_expr != '' %}
    where typeof(src.{{ json_column }}{{ parent_path_expr }}) = 'OBJECT'
    {%- endif %}
    order by key_name
  {%- endset -%}

  {%- set seen = {} -%}
  {%- for row in run_query(sql).rows -%}
    {%- set key   = row[0] -%}
    {%- set vtype = row[1] -%}
    {%- set existing = seen.get(key) -%}
    {%- if existing is none or existing == 'NULL_VALUE' -%}
      {%- do seen.update({key: vtype}) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- set result = [] -%}
  {%- for key, vtype in seen.items() -%}
    {%- do result.append({'key': key, 'value_type': vtype}) -%}
  {%- endfor -%}
  {{ return(result) }}
{%- endmacro -%}


{#
  Recursively discovers and projects the keys of the object at `path_segments` (a list of
  raw, unescaped key names from the root; [] means the root object itself). Appends one
  projection string per resulting column onto `projections` (mutated in place). An
  OBJECT-valued key is expanded into its own children as long as `current_depth <
  max_depth`; otherwise (scalar, array, or an object at the depth limit) it is projected
  as a single column. This makes `max_depth` a genuine depth bound rather than the fixed
  two-level expansion earlier versions of this macro implemented.
#}
{%- macro _snowflake__expand_object(
    relation, json_column, sample_size, max_depth, strip_quotes, col_prefix,
    path_segments, alias_parts, current_depth, used_aliases, projections
) -%}
  {#- `_snowflake__bracket_path` with an empty column_name yields just the joined bracket
     segments (e.g. "['user']['address']"); reused here rather than accumulating the
     string by hand, since Jinja's `{% set %}` doesn't persist across `{% for %}`
     iterations the way `.append()` on a list does. #}
  {%- set parent_path_expr = dbt_vitao._snowflake__bracket_path('', path_segments) -%}

  {%- set keys = dbt_vitao._snowflake__discover_keys(relation, json_column, parent_path_expr, sample_size) -%}

  {%- for entry in keys -%}
    {%- set key   = entry.key -%}
    {%- set vtype = entry.value_type -%}
    {%- set new_path_segments = path_segments + [key] -%}
    {%- set new_alias_parts   = alias_parts + [key] -%}

    {%- if vtype == 'OBJECT' and current_depth < max_depth -%}
      {%- do dbt_vitao._snowflake__expand_object(
        relation, json_column, sample_size, max_depth, strip_quotes, col_prefix,
        new_path_segments, new_alias_parts, current_depth + 1, used_aliases, projections
      ) -%}

    {%- else -%}
      {%- set cast       = dbt_vitao._snowflake__type_to_cast(vtype) -%}
      {%- set alias_raw  = new_alias_parts | join('_') -%}
      {%- set alias      = dbt_vitao._snowflake__dedupe_alias(
        col_prefix ~ dbt_vitao.normalize_alias(alias_raw), used_aliases) -%}
      {%- set full_path_expr = dbt_vitao._snowflake__bracket_path('', new_path_segments) -%}
      {%- set key_path = "src." ~ json_column ~ full_path_expr -%}
      {%- set casted   = dbt_vitao._snowflake__cast_expr(key_path, cast, strip_quotes) -%}
      {%- do projections.append(casted ~ " as " ~ alias) -%}
    {%- endif -%}

  {%- endfor -%}
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
    prefix, include_source_columns, include_json_column, sample_size, max_depth, strip_quotes
) -%}

  {%- set col_prefix = prefix ~ '_' if prefix else '' -%}

  {#- Once json_column's fields are expanded into named columns, the original blob is
     usually just noise (e.g. an intermediate array-element column from a chained
     mode='rows' -> mode='columns' flatten). include_json_column=false drops it from the
     src.* passthrough via Snowflake's native `* EXCLUDE (...)`, while keeping every
     other source column (join keys, lineage columns, etc). #}
  {%- set source_cols_expr = 'src.*' if include_json_column else ('src.* exclude (' ~ json_column ~ ')') -%}

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
      {{ source_cols_expr }},
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

      {%- set projections = [] -%}
      {%- set used_aliases = {} -%}
      {%- do dbt_vitao._snowflake__expand_object(
        relation, json_column, sample_size, max_depth, strip_quotes, col_prefix,
        [], [], 1, used_aliases, projections
      ) -%}

      select
        {%- if include_source_columns %}
        {{ source_cols_expr }},
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
