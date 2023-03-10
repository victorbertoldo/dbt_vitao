{% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- set unlogged = config.get('unlogged', default=false) -%}
  {%- set columnar = config.get('columnar', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create {% if temporary -%}
    temporary
  {%- elif unlogged -%}
    unlogged
  {%- endif %} table {{ relation }}
  {%- if columnar %}
    using columnar
  {%- endif %}
  as (
    {{ sql }}
  );
{%- endmacro %}