{% macro postgres__json_extract_variant(expr, path) %}
  {{ exceptions.raise_compiler_error(
    "dbt_vitao.json_extract_variant is not available on PostgreSQL. "
    ~ "PostgreSQL uses JSONB; use json_extract_object() for object extraction or json_extract_scalar() for scalar values."
  ) }}
{% endmacro %}
