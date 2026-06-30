{% macro type_family(data_type) %}
  {%- set dt = data_type | lower | trim -%}
  {%- if dt in ['text', 'varchar', 'character varying', 'string', 'char', 'bpchar', 'nvarchar', 'nchar'] -%}
    {{ return('string') }}
  {%- elif dt in ['smallint', 'int2', 'int', 'int4', 'integer', 'bigint', 'int8'] -%}
    {{ return('integer') }}
  {%- elif dt in ['numeric', 'decimal', 'number'] -%}
    {{ return('numeric') }}
  {%- elif dt in ['float', 'float4', 'float8', 'double precision', 'double', 'real'] -%}
    {{ return('float') }}
  {%- elif dt in ['boolean', 'bool'] -%}
    {{ return('boolean') }}
  {%- elif dt in ['date'] -%}
    {{ return('date') }}
  {%- elif dt in ['timestamp', 'timestamp without time zone', 'timestamp with time zone', 'timestamptz', 'timestamp_ntz', 'timestamp_ltz', 'timestamp_tz', 'datetime'] -%}
    {{ return('timestamp') }}
  {%- elif dt in ['json', 'jsonb', 'variant', 'object', 'array'] -%}
    {{ return('json') }}
  {%- else -%}
    {{ return('unknown') }}
  {%- endif -%}
{% endmacro %}
