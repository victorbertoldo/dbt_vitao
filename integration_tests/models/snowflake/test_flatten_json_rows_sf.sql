-- Execution test: Snowflake flatten_json rows mode via LATERAL FLATTEN.
-- Run: dbt run --models test_flatten_json_rows_sf (against Snowflake target)

{{ dbt_vitao.flatten_json(
    relation=ref('json_edge_cases'),
    json_column='PAYLOAD',
    mode='rows',
    recursive=true,
    outer=true,
    include_source_columns=false
) }}
