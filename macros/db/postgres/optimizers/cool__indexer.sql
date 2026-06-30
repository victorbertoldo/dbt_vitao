{% macro cool__indexer(this, column, index_type='btree') %}
  {# Legacy macro — use dbt_vitao.optimize_relation() for new projects. #}
  {{ return(dbt_vitao.optimize_relation(this, strategy='index', columns=[column], options={'index_type': index_type})) }}
{% endmacro %}
