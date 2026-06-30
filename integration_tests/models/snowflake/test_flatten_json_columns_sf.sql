-- Compile + execution test: Snowflake flatten_json columns mode with schema_override.
-- Uses Snowflake colon-path notation internally.
-- Run: dbt run --models test_flatten_json_columns_sf (against Snowflake target)

{{ dbt_vitao.flatten_json(
    relation=ref('json_edge_cases'),
    json_column='PAYLOAD',
    mode='columns',
    schema_override=[
        {'path': 'id',                        'cast_type': 'integer', 'alias': 'payload_id'},
        {'path': 'customer:name',             'cast_type': 'string',  'alias': 'customer_name'},
        {'path': 'customer:email',            'cast_type': 'string',  'alias': 'customer_email'},
        {'path': 'customer:address:city',     'cast_type': 'string',  'alias': 'customer_city'},
        {'path': 'customer:address:country',  'cast_type': 'string',  'alias': 'customer_country'},
        {'path': 'external_code',             'cast_type': 'string',  'alias': 'external_code'},
        {'path': 'url',                       'cast_type': 'string',  'alias': 'url'},
        {'path': 'flags:active',              'cast_type': 'boolean', 'alias': 'flag_active'},
        {'path': 'flags:vip',                 'cast_type': 'boolean', 'alias': 'flag_vip'}
    ],
    prefix='',
    include_source_columns=true
) }}
