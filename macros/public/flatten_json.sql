{% macro flatten_json(
    relation,
    json_column,
    mode='columns',
    max_depth=2,
    sample_size=1000,
    schema_override=none,
    prefix='',
    include_source_columns=true,
    include_json_column=true,
    recursive=true,
    outer=true,
    strip_quotes=false,
    null_key_cast='string'
) %}
  {{ return(adapter.dispatch('flatten_json', 'dbt_vitao')(
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
  )) }}
{% endmacro %}
