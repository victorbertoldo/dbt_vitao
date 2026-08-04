{% macro default__flatten_json(relation, json_column, mode, max_depth, sample_size, schema_override, prefix, include_source_columns, include_json_column, recursive, outer, strip_quotes) %}
  {{ exceptions.raise_compiler_error(
    "dbt_vitao.flatten_json is not implemented for adapter: " ~ target.type
    ~ ". Supported adapters: postgres, snowflake."
  ) }}
{% endmacro %}
