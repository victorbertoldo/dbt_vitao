-- Compile test for Snowflake format_column.
-- Snowflake identifiers are unquoted; types include VARIANT.
-- Run: dbt compile --models test_format_column_sf (against Snowflake target)

select
  {{ dbt_vitao.format_column('NAME',       'TEXT',    'fmt_name',      {'string_formatting': 'initcap'}) }},
  {{ dbt_vitao.format_column('EMAIL',      'STRING',  'fmt_email',     {'string_formatting': 'trim_only'}) }},
  {{ dbt_vitao.format_column('SCORE',      'NUMBER',  'fmt_score') }},
  {{ dbt_vitao.format_column('AMOUNT',     'FLOAT',   'fmt_amount') }},
  {{ dbt_vitao.format_column('IS_ACTIVE',  'BOOLEAN', 'fmt_is_active') }},
  {{ dbt_vitao.format_column('PAYLOAD',    'VARIANT', 'fmt_payload') }}
from {{ ref('json_edge_cases') }}
