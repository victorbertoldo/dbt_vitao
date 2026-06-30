{% macro default__format_relation(relation, prefix='', options={}) %}
  {{ exceptions.raise_compiler_error(
    "dbt_vitao.format_relation is not implemented for adapter: " ~ target.type
    ~ ". Supported adapters: postgres, snowflake."
  ) }}
{% endmacro %}
