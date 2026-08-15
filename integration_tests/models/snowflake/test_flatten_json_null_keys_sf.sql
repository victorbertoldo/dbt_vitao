-- Compile + execution test: Snowflake flatten_json auto-discovery (no schema_override)
-- covering the two schema-inference defects fixed in 0.5.0.
-- Run: dbt run --models test_flatten_json_null_keys_sf (against Snowflake target)
--
-- Fixture facts (integration_tests/seeds/json_edge_cases.csv):
--   * `flags:vip` is JSON null in every row that carries `flags` -> typeof() only ever
--     reports NULL_VALUE. Before 0.5.0 the key fell through to the `variant` fallback and
--     produced an uncast VARIANT column holding nothing but nulls. It must now come out as
--     a plain VARCHAR NULL column (null_key_cast default 'string').
--   * `flags:active` is boolean in row 1 and JSON null in row 4 -> the dedupe in
--     _snowflake__discover_keys must still prefer the observed BOOLEAN, so this one stays
--     BOOLEAN and is NOT affected by null_key_cast.
--   * `external_code` / `url` only exist on row 1, so they are only discovered when the
--     whole relation is profiled -- which is what sample_size=none asks for.
--
-- Assert after running:
--   select flags_vip, flags_active, external_code
--   from {{ this }};
--   -- flags_vip    -> TEXT (all null), never VARIANT
--   -- flags_active -> BOOLEAN
--   -- external_code present, i.e. the rare key was not dropped by sampling

{{ dbt_vitao.flatten_json(
    relation=ref('json_edge_cases'),
    json_column='PAYLOAD',
    mode='columns',
    max_depth=2,
    sample_size=none,
    include_source_columns=false
) }}
