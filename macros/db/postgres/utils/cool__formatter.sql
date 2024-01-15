{%- macro cool__formatter(tab,src=0,prefix='',schema_src='') -%}
    {#
        O parametro src possui o valor default de 0, sendo 0 para refs e 1 para sources
    #}
    {%- set fields=[] -%}
    {%- if src == 0 -%} {# Ref #}
        {%- set cols = adapter.get_columns_in_relation(ref(tab)) -%}
    {%- elif src == 1 -%} {# Source #}

        {%- do log('Passou por aqui, antes do get columns: "%s"' | format(tab), info=true) -%}
        {%- set cols = adapter.get_columns_in_relation(source(schema_src,tab)) -%}
        {%- do log('Passou por aqui, depois do get columns: "%s"' | format(cols), info=true) -%}
    {%- elif src == 2 -%} {# Query #}
        {%- set query -%}
            select * from {{ this }}
        {%- endset -%}
        {%- set results = run_query(query) -%}
        {%- if execute %}
            {%- set cols = results.column_names -%}
        {%- endif -%}
    {%- endif -%}

    {%- for column in cols -%}
        {{ '\n' }}
        {%- do log('Passou por aqui, iterando sobre as colunas: "%s"' | format(column), info=true) -%}
        {%- if column.data_type == 'string' or column.data_type == 'text' -%}
            initcap(trim("{{ column.name }}")) as {{ prefix ~ column.name | lower | replace('.',"_") }}{%- if not loop.last -%},{%- endif -%}
        {%- elif column.data_type == 'smallint' or column.data_type == 'int' or column.data_type == 'bigint' -%}
            coalesce("{{ column.name }}",0) as {{ prefix ~ column.name | lower | replace('.',"_") }}{%- if not loop.last -%},{%- endif -%}
        {%- elif column.data_type == 'double' or column.data_type == 'float' -%}
             coalesce("{{ column.name }}",0.00) as {{ prefix ~ column.name | lower | replace('.',"_") }}{%- if not loop.last -%},{%- endif -%}
        {%- elif column.data_type == 'jsonb' and 'trello' not in tab -%}

            {%- set query_json -%}
                select "{{ column.name }}"
                from {{ source(schema_src,tab) }}
                where jsonb_typeof("{{ column.name }}") = 'array' or jsonb_typeof("{{ column.name }}") = 'object'
                limit 1
            {%- endset -%}
                        
            {%- do log('Query JSON: "%s"' | format(query_json), info=true) -%}

            {%- set results_json = run_query(query_json) -%}

            {%- if execute -%}                      
                 {%- set is_array = results_json[0][0][0] -%}
                 {%- do log('JSONB Col. Verificacao é array: "%s"' | format(is_array), info=true) -%}
            {%- endif -%}  

            {%- if is_array == '[' -%}  

                {%- set query_json -%}
                    select "{{ column.name }}" ->0 as "{{ column.name }}"
                    from {{ source(schema_src,tab) }}
                    where jsonb_typeof("{{ column.name }}") = 'array'
                    limit 1
                {%- endset -%}


            {%- elif is_array == '{' -%}

                {%- set query_json -%}
                    select "{{ column.name }}" 
                    from {{ source(schema_src,tab) }}
                    where jsonb_typeof("{{ column.name }}") = 'object'
                    limit 1
                {%- endset -%}            

            {%- endif -%}  
            
            {%- set results_json = run_query(query_json) -%}

            {%- if execute -%}                   
                {%- do log('Results JSON Rows. Vals: "%s"' | format(results_json.rows[0][0]), info=true) -%}

                {%- if results_json.rows[0][0] is not none -%}
                    {%- set vals_jsonbcol = results_json.rows[0][0] -%}                
                    {%- do log('JSONB Col. Vals: "%s"' | format(vals_jsonbcol), info=true) -%}

                    {%- if vals_jsonbcol is not none -%}

                        {%- do log('JSONB Col. Iniciando o parse da coluna "%s"...' | format(column.name), info=true) -%}
                        {%- do log('Parsing result: "%s"' | format(vals_jsonbcol),info=true) -%}
                    
                        {%- set json2dict_jsonbcol = fromjson(vals_jsonbcol) -%}   

                        {%- do log('Pos parse usando fromdict: "%s"' | format(json2dict_jsonbcol), info=true) -%} 

                        {%- if is_array == '[' -%}                                              

                            {%- set jsondata_counts = unnest__json(json2dict_jsonbcol,none,column.name,true) | count -%}                                             
                            {{ unnest__json(json2dict_jsonbcol,none,column.name,true) }}{%- if jsondata_counts > 0 and not loop.last -%}, {% endif %}

                        {%- elif is_array == '{' -%}

                            {%- set jsondata_counts = unnest__json(json2dict_jsonbcol,none,column.name,none) | count -%} 
                            {{ unnest__json(json2dict_jsonbcol,none,column.name,none) }}{%- if jsondata_counts > 0 and not loop.last -%}, {% endif %}

                        {%- endif -%} 

                        {%- do log('JSONB Col. Finalizou o parse da coluna "%s"...' | format(column.name), info=true) -%}

                    {%- endif -%}
                {%- endif -%}
                {%- do log('Passou pelo parser do JSON', info=true)-%}
            {%- endif -%}


        {%- else -%}
           "{{ column.name }}" as {{ prefix ~ column.name | lower | replace('.',"_") }} {%- if not loop.last -%},{%- endif -%}
        {%- endif -%}
        {%- do fields.append(column.name) -%}
        {# {%- do log('Lista de campos: "%s"' | format(fields), info=true) -%}                                                       #}
    {%- endfor %}
{%- endmacro -%}
