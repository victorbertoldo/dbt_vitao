{% macro json_extract_variant(expr, path) %}
  {{ return(adapter.dispatch('json_extract_variant', 'dbt_vitao')(expr, path)) }}
{% endmacro %}
