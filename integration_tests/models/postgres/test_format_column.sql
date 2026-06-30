-- Compile test: verifies format_column generates correct SQL per type.
-- Run: dbt compile --models test_format_column
-- Does not require execution — inspect compiled SQL in target/compiled/.

select
  {{ dbt_vitao.format_column('name',         'text',    'fmt_name',         {'string_formatting': 'initcap'}) }},
  {{ dbt_vitao.format_column('email',        'varchar', 'fmt_email',        {'string_formatting': 'trim_only'}) }},
  {{ dbt_vitao.format_column('score',        'integer', 'fmt_score') }},
  {{ dbt_vitao.format_column('amount',       'numeric', 'fmt_amount') }},
  {{ dbt_vitao.format_column('is_active',    'boolean', 'fmt_is_active') }},
  {{ dbt_vitao.format_column('event_date',   'date',    'fmt_event_date') }},
  {{ dbt_vitao.format_column('event_ts',     'timestamp', 'fmt_event_ts') }},
  {{ dbt_vitao.format_column('payload',      'jsonb',   'fmt_payload') }},
  {{ dbt_vitao.format_column('unknown_col',  'uuid',    'fmt_unknown') }}
from {{ ref('json_edge_cases') }}
