{% macro get_relation_columns(relation) %}
  {{ return(adapter.dispatch('get_relation_columns', 'dbt_vitao')(relation)) }}
{% endmacro %}
