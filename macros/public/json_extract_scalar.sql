{% macro json_extract_scalar(expr, path, cast_type='string') %}
  {{ return(adapter.dispatch('json_extract_scalar', 'dbt_vitao')(expr, path, cast_type)) }}
{% endmacro %}
