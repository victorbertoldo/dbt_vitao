{% macro default__optimize_relation(relation, strategy, columns, options) %}
  {{ exceptions.raise_compiler_error(
    "dbt_vitao.optimize_relation is not implemented for adapter: " ~ target.type
    ~ ". Supported adapters: postgres (strategy='index'), snowflake (strategy='cluster_by', 'search_optimization')."
  ) }}
{% endmacro %}
