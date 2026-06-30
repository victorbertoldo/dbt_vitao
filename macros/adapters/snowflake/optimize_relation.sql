{% macro snowflake__optimize_relation(relation, strategy='search_optimization', columns=[], options={}) %}

  {%- if strategy == 'cluster_by' -%}
    {%- if columns | length == 0 -%}
      {{ exceptions.raise_compiler_error("dbt_vitao.optimize_relation: at least one column is required for strategy='cluster_by'.") }}
    {%- endif -%}
    alter table {{ relation }}
    cluster by ({{ columns | join(', ') }})

  {%- elif strategy == 'search_optimization' -%}
    {%- if columns | length == 0 -%}
      alter table {{ relation }}
      add search optimization
    {%- else -%}
      {%- set mode = options.get('mode', 'equality') -%}
      {%- set valid_modes = ['equality', 'substring', 'variant'] -%}
      {%- if mode not in valid_modes -%}
        {{ exceptions.raise_compiler_error(
          "dbt_vitao.optimize_relation: unsupported search optimization mode '" ~ mode ~ "'. "
          ~ "Valid values: " ~ valid_modes | join(', ')
        ) }}
      {%- endif -%}
      alter table {{ relation }}
      add search optimization on {{ mode }}({{ columns | join(', ') }})
    {%- endif -%}

  {%- elif strategy == 'index' -%}
    {{ exceptions.raise_compiler_error(
      "dbt_vitao.optimize_relation: strategy='index' is not supported on Snowflake. "
      ~ "Snowflake does not use traditional indexes. "
      ~ "Use strategy='cluster_by' or strategy='search_optimization'."
    ) }}

  {%- else -%}
    {{ exceptions.raise_compiler_error(
      "dbt_vitao.optimize_relation: strategy '" ~ strategy ~ "' is not supported on Snowflake. "
      ~ "Valid values: 'cluster_by', 'search_optimization'."
    ) }}
  {%- endif -%}

{% endmacro %}
