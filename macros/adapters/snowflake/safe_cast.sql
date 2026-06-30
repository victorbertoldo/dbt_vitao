{% macro snowflake__safe_cast(expr, cast_type) %}
  try_cast({{ expr }} as {{ cast_type }})
{% endmacro %}
