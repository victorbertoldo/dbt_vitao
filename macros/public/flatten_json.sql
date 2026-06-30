{% macro flatten_json(
    relation,
    json_column,
    mode='columns',
    max_depth=2,
    sample_size=1000,
    schema_override=none,
    prefix='',
    include_source_columns=true,
    recursive=true,
    outer=true
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
      recursive,
      outer
  )) }}
{% endmacro %}
