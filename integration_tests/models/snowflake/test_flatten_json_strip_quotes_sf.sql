-- Compile + execution test: Snowflake flatten_json columns mode with auto-discovery
-- and strip_quotes=true. json_edge_cases.PAYLOAD['category'] is a double-encoded
-- string whose literal value is "ESTACIONAMENTO" (quotes included) -- strip_quotes
-- trims those stray leading/trailing double quotes off every string-typed column.
-- Run: dbt run --models test_flatten_json_strip_quotes_sf (against Snowflake target)

{{ dbt_vitao.flatten_json(
    relation=ref('json_edge_cases'),
    json_column='PAYLOAD',
    mode='columns',
    max_depth=2,
    recursive=true,
    strip_quotes=true
) }}
