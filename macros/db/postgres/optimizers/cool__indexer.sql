{% macro cool__indexer(this, column, index_type='btree') %}
    {# B-tree index: Best for (=) operations and range (<, <=, >, >=) queries on ordered data. #}
    {%- if index_type == 'btree' -%}
        create index if not exists "idx_{{ this.name }}_on_{{ column }}_{{ index_type}}" on {{ this }} ("{{ column }}")

    {# Hash index: Optimized for (=) comparisons, but not good for range queries. #}
    {%- elif index_type == 'hash' -%}
        create index if not exists "idx_{{ this.name }}_on_{{ column }}_{{ index_type}}" on {{ this }} using hash ("{{ column }}")

    {# GIN index: Best for indexing complex data types like arrays, JSONB, and full-text search. #}
    {%- elif index_type == 'gin' -%}
        create index if not exists "idx_{{ this.name }}_on_{{ column }}_{{ index_type}}" on {{ this }} using gin ("{{ column }}")

    {# GiST index: Suitable for spatial data, range searches, and nearest-neighbor searches. #}
    {%- elif index_type == 'gist' -%}
        create index if not exists "idx_{{ this.name }}_on_{{ column }}_{{ index_type}}" on {{ this }} using gist ("{{ column }}")

    {# SP-GiST index: Ideal for non-balanced data structures like quadtrees, K-D trees, and range queries. #}
    {%- elif index_type == 'spgist' -%}
        create index if not exists "idx_{{ this.name }}_on_{{ column }}_{{ index_type}}" on {{ this }} using spgist ("{{ column }}")

    {# BRIN index: Best for large tables with naturally ordered data (e.g., time-series data). #}
    {%- elif index_type == 'brin' -%}
        create index if not exists "idx_{{ this.name }}_on_{{ column }}_{{ index_type}}" on {{ this }} using brin ("{{ column }}")

    {# Concurrent index: Allows index creation without locking the table, useful for prod environments. #}
    {%- elif index_type == 'concurrent' -%}
        create index concurrently if not exists "idx_{{ this.name }}_on_{{ column }}_{{ index_type}}" on {{ this }} ("{{ column }}")

    {%- else -%}
        {{ exceptions.raise_compiler_error("Unsupported index type: " ~ index_type) }}
    {%- endif -%}
{% endmacro %}
