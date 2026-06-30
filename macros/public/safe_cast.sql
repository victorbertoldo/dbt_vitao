{% macro safe_cast(expr, cast_type) %}
  {{ return(adapter.dispatch('safe_cast', 'dbt_vitao')(expr, cast_type)) }}
{% endmacro %}
