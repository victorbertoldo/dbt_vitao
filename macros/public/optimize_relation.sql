{% macro optimize_relation(relation, strategy='index', columns=[], options={}) %}
  {{ return(adapter.dispatch('optimize_relation', 'dbt_vitao')(relation, strategy, columns, options)) }}
{% endmacro %}
