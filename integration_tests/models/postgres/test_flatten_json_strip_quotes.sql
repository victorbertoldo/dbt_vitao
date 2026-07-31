-- Compile + execution test: flatten_json in columns mode with strip_quotes=true.
-- payload.category is a double-encoded string whose literal value is "ESTACIONAMENTO"
-- (quotes included) -- strip_quotes trims those stray leading/trailing double quotes
-- off every text-typed column.
-- Run: dbt run --models test_flatten_json_strip_quotes

{{ dbt_vitao.flatten_json(
    relation=ref('json_edge_cases'),
    json_column='payload',
    mode='columns',
    schema_override=[
        {'path': 'external_code', 'cast_type': 'text', 'alias': 'external_code'},
        {'path': 'category',      'cast_type': 'text', 'alias': 'category'}
    ],
    prefix='',
    include_source_columns=false,
    strip_quotes=true
) }}
