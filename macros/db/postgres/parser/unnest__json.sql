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

                {{ unnest__json(value, parent_key, field,is_array) }}

            {%- else -%}

                {%- if string %}
                    {%- set str_list = string.split("->") -%}
                    {%- set list_count = str_list | count -%}
                    {%- if list_count > 1 %}
                        {%- if is_array is not none %}
                            "{{ field }}" ->0 -> '{{ str_list[0] | trim }}' -> '{{ str_list[1] | trim }}' ->> '{{ key }}' as {{ field | lower }}_{{ string.split('->')[0] | trim | lower  }}_{{ key | lower }}
                        {%- else %}
                            "{{ field }}" -> '{{ str_list[0] | trim }}' -> '{{ str_list[1] | trim }}' ->> '{{ key }}' as {{ field | trim | lower }}_{{ (string.split('->')[0] ~ string.split('->')[1]) | trim | lower }}_{{ key | trim | lower }} 
                        {%- endif %}                        
                    {%- else %}
                        {%- if is_array is not none %}
                            "{{ field }}" ->0 -> '{{ str_list[0] | trim }}' ->> '{{ key }}' as {{ field | lower }}_{{ string.split('->')[0] | trim }}_{{ key }}
                        {%- else %}
                            "{{ field }}" -> '{{ str_list[0] | trim }}' ->> '{{ key }}' as {{ field | trim | lower }}_{{ string.split('->')[0] | trim | lower }}_{{ key | trim | lower }} 
                        {%- endif %}  
                    {%- endif -%} 
                {%- else %}
                    {%- if is_array is not none %}
                        "{{ field }}" ->0 ->> '{{ key }}' as {{ field  | lower }}_{{ key  | lower }}
                    {%- else %}
                        "{{ field }}" ->> '{{ key }}' as {{ field  | lower }}_{{ key  | lower }}
                    {%- endif %}
                {%- endif -%}                

            {%- endif -%}
            {%- if not loop.last and field is not none -%},{%- endif -%} 

        {%- endfor -%}

{%- endmacro -%}