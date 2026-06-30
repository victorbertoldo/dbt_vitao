{%- macro cool__formatter(tab, src=0, prefix='', schema_src='') -%}
  {#
    Legacy macro — use dbt_vitao.format_relation() for new projects.

    BEHAVIOR CHANGE from v0.0.3: JSON columns are no longer auto-expanded inline.
    Use dbt_vitao.flatten_json() to expand JSON columns explicitly.

    string_formatting defaults preserved as 'initcap' for backward compatibility
    with original cool__formatter behavior. New dbt_vitao.format_relation() calls
    default to 'trim_only'.
  #}
  {%- set opts = {'string_formatting': 'initcap'} -%}
  {%- if src == 0 -%}
    {{ return(dbt_vitao.format_relation(ref(tab), prefix=prefix, options=opts)) }}
  {%- elif src == 1 -%}
    {{ return(dbt_vitao.format_relation(source(schema_src, tab), prefix=prefix, options=opts)) }}
  {%- elif src == 2 -%}
    {{ return(dbt_vitao.format_relation(this, prefix=prefix, options=opts)) }}
  {%- else -%}
    {{ exceptions.raise_compiler_error("cool__formatter: invalid src value '" ~ src ~ "'. Use 0=ref, 1=source, 2=this.") }}
  {%- endif -%}
{%- endmacro -%}
