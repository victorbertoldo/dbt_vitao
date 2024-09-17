
  {# for older versions of dbt #}
    
 {#  {% macro postgres__create_table_as(temporary, relation, sql) -%}
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
{%- endmacro %} #}


{# for newer versions of dbt #}
{# This was updated because of the changes in the dbt version 1.7.x #}
  {% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- set unlogged = config.get('unlogged', default=false) -%}
  {%- set columnar = config.get('columnar', default=false) -%}
  
  {{ log("Executing postgres__create_table_as macro", info=True) }}
  {{ log("Columnar config value: " ~ columnar, info=True) }}

  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}
    temporary
  {%- elif unlogged -%}
    unlogged
  {%- endif %} table {{ relation }}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
  {% endif -%}
  {% if contract_config.enforced and (not temporary) -%}
      {{ get_table_columns_and_constraints() }} 
    {%- if columnar == true %}
      {{ log("Creating columnar table for relation " ~ relation, info=True) }}
      using columnar
    {% else %}
      {{ log("Creating standard table for relation " ~ relation, info=True) }}
    {%- endif %} ;
    insert into {{ relation }} (
      {{ adapter.dispatch('get_column_names', 'dbt')() }}
    )
    {%- set sql = get_select_subquery(sql) %}
  {% else %}
    as
  {% endif %}
  (
    {{ sql }}
  );
{%- endmacro %}

  
  
