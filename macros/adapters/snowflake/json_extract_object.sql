{% macro snowflake__json_extract_object(expr, path) %}
  {# Returns a VARIANT sub-object. Uses bracket notation to handle special-character keys. #}
  {%- set key_parts = (path | replace('.', ':')).split(':') -%}
  {%- set segments = [expr] -%}
  {%- for part in key_parts -%}
    {%- do segments.append("['" ~ part ~ "']") -%}
  {%- endfor -%}
  {{ segments | join('') }}
{% endmacro %}
