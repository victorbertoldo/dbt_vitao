{% macro format_column(column_name, data_type, alias_name=none, options={}) %}
  {{ return(adapter.dispatch('format_column', 'dbt_vitao')(column_name, data_type, alias_name, options)) }}
{% endmacro %}
