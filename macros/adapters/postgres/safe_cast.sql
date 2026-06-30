{% macro postgres__safe_cast(expr, cast_type) %}
  ({{ expr }})::{{ cast_type }}
{% endmacro %}
