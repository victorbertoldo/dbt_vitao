{% macro postgres__format_relation(relation, prefix='', options={}) %}
  {%- set cols = adapter.get_columns_in_relation(relation) -%}
  {%- set results = [] -%}
  {%- for column in cols -%}
    {%- set alias = prefix ~ dbt_vitao.normalize_alias(column.name) -%}
    {%- set col_sql = dbt_vitao.format_column(column.name, column.data_type, alias, options) | trim -%}
    {%- do results.append(col_sql) -%}
  {%- endfor -%}
  {{ results | join(',\n') }}
{% endmacro %}
