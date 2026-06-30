{% macro format_relation(relation, prefix='', options={}) %}
  {{ return(adapter.dispatch('format_relation', 'dbt_vitao')(relation, prefix, options)) }}
{% endmacro %}
