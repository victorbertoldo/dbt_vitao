{% macro postgres__optimize_relation(relation, strategy='index', columns=[], options={}) %}
  {%- if strategy == 'index' -%}
    {%- set index_type = options.get('index_type', 'btree') -%}
    {%- set valid_types = ['btree', 'hash', 'gin', 'gist', 'spgist', 'brin', 'concurrent'] -%}
    {%- if index_type not in valid_types -%}
      {{ exceptions.raise_compiler_error(
        "dbt_vitao.optimize_relation: unsupported index_type '" ~ index_type ~ "' for postgres. "
        ~ "Valid values: " ~ valid_types | join(', ')
      ) }}
    {%- endif -%}
    {%- if columns | length == 0 -%}
      {{ exceptions.raise_compiler_error("dbt_vitao.optimize_relation: at least one column is required for strategy='index'.") }}
    {%- endif -%}
    {%- for col in columns -%}
      {%- if index_type == 'btree' -%}
        create index if not exists "idx_{{ relation.name }}_on_{{ col }}_btree" on {{ relation }} ("{{ col }}")
      {%- elif index_type == 'hash' -%}
        create index if not exists "idx_{{ relation.name }}_on_{{ col }}_hash" on {{ relation }} using hash ("{{ col }}")
      {%- elif index_type == 'gin' -%}
        create index if not exists "idx_{{ relation.name }}_on_{{ col }}_gin" on {{ relation }} using gin ("{{ col }}")
      {%- elif index_type == 'gist' -%}
        create index if not exists "idx_{{ relation.name }}_on_{{ col }}_gist" on {{ relation }} using gist ("{{ col }}")
      {%- elif index_type == 'spgist' -%}
        create index if not exists "idx_{{ relation.name }}_on_{{ col }}_spgist" on {{ relation }} using spgist ("{{ col }}")
      {%- elif index_type == 'brin' -%}
        create index if not exists "idx_{{ relation.name }}_on_{{ col }}_brin" on {{ relation }} using brin ("{{ col }}")
      {%- elif index_type == 'concurrent' -%}
        create index concurrently if not exists "idx_{{ relation.name }}_on_{{ col }}_concurrent" on {{ relation }} ("{{ col }}")
      {%- endif -%}
      {%- if not loop.last -%};
      {%- endif -%}
    {%- endfor -%}
  {%- else -%}
    {{ exceptions.raise_compiler_error(
      "dbt_vitao.optimize_relation: strategy '" ~ strategy ~ "' is not supported on PostgreSQL. "
      ~ "Use strategy='index'."
    ) }}
  {%- endif -%}
{% endmacro %}
