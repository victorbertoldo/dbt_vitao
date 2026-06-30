-- Execution test: flatten_json in rows mode using jsonb_each lateral join.
-- Produces one row per top-level JSON key per source row.
-- Run: dbt run --models test_flatten_json_rows

{{ dbt_vitao.flatten_json(
    relation=ref('json_edge_cases'),
    json_column='payload',
    mode='rows',
    include_source_columns=false,
    outer=true
) }}
