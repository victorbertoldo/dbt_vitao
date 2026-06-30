{% macro default__safe_cast(expr, cast_type) %}
  {{ exceptions.raise_compiler_error(
    "dbt_vitao.safe_cast is not implemented for adapter: " ~ target.type
    ~ ". Supported adapters: postgres, snowflake."
  ) }}
{% endmacro %}
