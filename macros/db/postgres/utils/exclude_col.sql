{%- macro exclude_col(schema_nm, tab, x_col, alias) -%}
    {%- set query -%}
        select 
            *
        from {{source(schema_nm,tab)}} 
        limit 1
    {%- endset -%}
    {%- set results = run_query(query) -%}
    {# Log results #}
    {% do log('Results: ' ~ results, info=true) %}

    {%- if execute -%}
        {%- set cols = results.exclude(x_col) -%}
        {%- for col in cols.column_names %}
            
            {%- if (alias | count)>0 -%}{{ alias~'.'}}{%- endif -%}{{ col }}{%- if not loop.last -%},{{'\n'}}{%- endif -%}
        {%- endfor %}
    {%- endif -%}
{%- endmacro -%}