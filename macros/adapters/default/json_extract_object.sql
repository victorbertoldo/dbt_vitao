{% macro default__json_extract_object(expr, path) %}
  {{ exceptions.raise_compiler_error(
    "dbt_vitao.json_extract_object is not implemented for adapter: " ~ target.type
    ~ ". Supported adapters: postgres, snowflake."
  ) }}
{% endmacro %}
