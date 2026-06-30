{% macro snowflake__format_column(column_name, data_type, alias_name=none, options={}) %}
  {%- set alias = alias_name if alias_name is not none else dbt_vitao.normalize_alias(column_name) -%}
  {%- set family = dbt_vitao.type_family(data_type) -%}
  {%- set string_fmt = options.get('string_formatting', var('dbt_vitao', {}).get('string_formatting', 'trim_only')) -%}

  {%- if family == 'string' -%}
    {%- if string_fmt == 'none' -%}
      {{ column_name }} as {{ alias }}
    {%- elif string_fmt == 'trim_only' -%}
      trim({{ column_name }}) as {{ alias }}
    {%- elif string_fmt == 'lower' -%}
      lower(trim({{ column_name }})) as {{ alias }}
    {%- elif string_fmt == 'upper' -%}
      upper(trim({{ column_name }})) as {{ alias }}
    {%- elif string_fmt == 'initcap' -%}
      initcap(trim({{ column_name }})) as {{ alias }}
    {%- else -%}
      {{ exceptions.raise_compiler_error("dbt_vitao: unsupported string_formatting value: '" ~ string_fmt ~ "'. Valid values: none, trim_only, lower, upper, initcap.") }}
    {%- endif -%}
  {%- elif family in ['integer', 'numeric'] -%}
    coalesce({{ column_name }}, 0) as {{ alias }}
  {%- elif family == 'float' -%}
    coalesce({{ column_name }}, 0.0) as {{ alias }}
  {%- elif family == 'json' -%}
    {{ column_name }} as {{ alias }}
  {%- else -%}
    {{ column_name }} as {{ alias }}
  {%- endif -%}
{% endmacro %}
