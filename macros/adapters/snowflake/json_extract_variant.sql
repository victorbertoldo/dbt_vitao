{% macro snowflake__json_extract_variant(expr, path) %}
  {%- set sf_path = path | replace('.', ':') -%}
  {{ expr }}:{{ sf_path }}
{% endmacro %}
