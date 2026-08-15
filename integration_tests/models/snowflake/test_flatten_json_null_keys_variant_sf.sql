-- Compile + execution test: null_key_cast='variant' restores the pre-0.5.0 behaviour.
-- Run: dbt run --models test_flatten_json_null_keys_variant_sf (against Snowflake target)
--
-- Same fixture and same discovery settings as test_flatten_json_null_keys_sf, but opting
-- back into the legacy fallback. `flags_vip` must come out VARIANT here, proving the new
-- default is a choice and not a hard-coded behaviour change.
--
-- Assert after running:
--   select flags_vip from {{ this }};   -- VARIANT

{{ dbt_vitao.flatten_json(
    relation=ref('json_edge_cases'),
    json_column='PAYLOAD',
    mode='columns',
    max_depth=2,
    sample_size=none,
    include_source_columns=false,
    null_key_cast='variant'
) }}
