{% macro postgres__json_extract_scalar(expr, path, cast_type='string') %}
  {%- set pg_type = 'text' if cast_type == 'string' else cast_type -%}
  {%- set path_parts = path.split('.') -%}
  {%- if path_parts | length == 1 -%}
    ({{ expr }} ->> '{{ path }}')::{{ pg_type }}
  {%- else -%}
    ({{ expr }}
    {%- for part in path_parts[:-1] %} -> '{{ part }}'{% endfor %}
     ->> '{{ path_parts[-1] }}')::{{ pg_type }}
  {%- endif -%}
{% endmacro %}
