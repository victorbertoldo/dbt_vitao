{% macro snowflake__json_extract_object(expr, path) %}
  {%- set sf_path = path | replace('.', ':') -%}
  get_path({{ expr }}, '{{ sf_path }}')
{% endmacro %}
