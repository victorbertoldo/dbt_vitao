{% macro postgres__json_extract_object(expr, path) %}
  {%- set path_parts = path.split('.') -%}
  {%- if path_parts | length == 1 -%}
    {{ expr }} -> '{{ path }}'
  {%- else -%}
    {{ expr }}
    {%- for part in path_parts %} -> '{{ part }}'{% endfor %}
  {%- endif -%}
{% endmacro %}
