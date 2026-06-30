{% macro normalize_alias(name) %}
  {%- set result = name
    | lower
    | replace(' ', '_')
    | replace('.', '_')
    | replace(':', '_')
    | replace('@', '')
    | replace('-', '_')
    | replace('/', '_')
    | replace('"', '')
    | replace("'", '')
  -%}
  {{ return(result) }}
{% endmacro %}
