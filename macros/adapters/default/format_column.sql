{% macro default__format_column(column_name, data_type, alias_name=none, options={}) %}
  {{ exceptions.raise_compiler_error(
    "dbt_vitao.format_column is not implemented for adapter: " ~ target.type
    ~ ". Supported adapters: postgres, snowflake."
  ) }}
{% endmacro %}
