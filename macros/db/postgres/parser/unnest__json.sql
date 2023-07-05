{%- macro unnest__json(dict,string,field,is_array) %}

        {%- for key, value in dict.items() -%}

            {%- if value is mapping -%}
                {%- if string -%}
                    {%- set parent_key -%}
                        {%- if is_array is not none %}
                            {{ string }} ->0 -> {{ key }}
                        {%- else -%}
                            {{ string }} -> {{ key }}
                        {%- endif %}
                    {%- endset -%}
                {%- else -%}
                    {%- set parent_key = key  -%}
                {%- endif -%}

                {{ dbt_vitao.unnest__json(value, parent_key, field,is_array) }}

            {%- else -%}

                {%- if string %}
                    {%- set str_list = string.split("->") -%}
                    {%- set list_count = str_list | count -%}
                    {%- if list_count > 1 %}
                        {%- if is_array is not none %}
                            "{{ field }}" ->0 -> '{{ str_list[0] | trim }}' -> '{{ str_list[1] | trim }}' ->> '{{ key }}' as {{ field | lower | replace(':','') | replace('@','') | replace('ns0','') | replace('ns1','') }}_{{ string.split('->')[0] | trim | lower | replace(':','') | replace('@','') | replace('ns0','') | replace('ns1','') }}_{{ key | lower | replace(':','') | replace('@','') | replace('ns0','') | replace('ns1','') }}
                        {%- else %}
                            "{{ field }}" -> '{{ str_list[0] | trim }}' -> '{{ str_list[1] | trim }}' ->> '{{ key }}' as {{ field | trim | lower | replace(':','') | replace('@','') | replace(' ','') | replace('ns0','') | replace('ns1','') }}_{{- (string.split('->')[0] ~ string.split('->')[1]) | trim | lower | replace(':','') | replace('@','') | replace(' ','') | replace('ns0','') | replace('ns1','') -}}_{{ key | trim | lower | replace(':','') | replace('@','') | replace(' ','') | replace('ns0','') | replace('ns1','') }} 
                        {%- endif %}                        
                    {%- else %}
                        {%- if is_array is not none %}
                            "{{ field }}" ->0 -> '{{ str_list[0] | trim }}' ->> '{{ key }}' as {{ field | lower | replace('"','') | replace(':','') | replace('@','') }}_{{ string.split('->')[0] | trim | replace(':','') | replace('@','') }}_{{ key | replace(':','') | replace('@','') }}
                        {%- else %}
                            "{{ field }}" -> '{{ str_list[0] | trim }}' ->> '{{ key }}' as {{ field | trim | lower | replace('"','') | replace(':','') | replace('@','') | replace(' ','') }}_{{ string.split('->')[0] | trim | lower | replace(':','') | replace('@','') | replace(' ','') }}_{{ key | trim | lower | replace(':','') | replace('@','') | replace(' ','') }} 
                        {%- endif %}  
                    {%- endif -%} 
                {%- else %}
                    {%- if is_array is not none %}
                        "{{ field }}" ->0 ->> '{{ key }}' as {{ field  | lower | replace('.',"_") | replace('@','') }}_{{ key  | lower | replace('.',"_") | replace('@','') | replace(':','') }}
                    {%- else %}
                        "{{ field }}" ->> '{{ key }}' as {{ field  | lower | replace('.',"_") | replace('@','') }}_{{ key  | lower | replace('.',"_") | replace('@','') | replace(':','') }}
                    {%- endif %}
                {%- endif -%}                

            {%- endif -%}
            {%- if not loop.last and field is not none -%},{%- endif -%} 

        {%- endfor -%}

{%- endmacro -%}