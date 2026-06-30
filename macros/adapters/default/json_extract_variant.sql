{% macro default__json_extract_variant(expr, path) %}
  {{ exceptions.raise_compiler_error(
    "dbt_vitao.json_extract_variant is not implemented for adapter: " ~ target.type
    ~ ". Snowflake-native semi-structured extraction. Not available for: " ~ target.type
  ) }}
{% endmacro %}
