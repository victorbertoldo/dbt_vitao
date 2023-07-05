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
  {% if config.get('contract', False) %}
    {{ get_assert_columns_equivalent(sql) }} 
    {{ get_columns_spec_ddl() }}  ;
    insert into {{ relation }} {{ get_column_names() }}
  {% else %}
  {%- if columnar %}
    using columnar
  {%- endif %}
      as
  {% endif %}
    (
    {{ sql }}
  );
{%- endmacro %}
