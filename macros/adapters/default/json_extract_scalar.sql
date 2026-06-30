{% macro default__json_extract_scalar(expr, path, cast_type) %}
  {{ exceptions.raise_compiler_error(
    "dbt_vitao.json_extract_scalar is not implemented for adapter: " ~ target.type
    ~ ". Supported adapters: postgres, snowflake."
  ) }}
{% endmacro %}
