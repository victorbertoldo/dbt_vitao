{% macro default__get_relation_columns(relation) %}
  {{ return(adapter.get_columns_in_relation(relation)) }}
{% endmacro %}
