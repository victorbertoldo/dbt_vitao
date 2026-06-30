{% macro snowflake__json_extract_scalar(expr, path, cast_type='string') %}
  {%- set sf_path = path | replace('.', ':') -%}
  {{ expr }}:{{ sf_path }}::{{ cast_type }}
{% endmacro %}
