{% macro json_extract_object(expr, path) %}
  {{ return(adapter.dispatch('json_extract_object', 'dbt_vitao')(expr, path)) }}
{% endmacro %}
