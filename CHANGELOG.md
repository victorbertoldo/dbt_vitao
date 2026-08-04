# Changelog

## [0.3.3] — 2026-08-04

### Fixed

- **`normalize_alias`**: a source key that normalizes to a bare SQL reserved
  word (e.g. a JSON key literally named `"null"`) produced an invalid
  unquoted column alias (`as null`). Reserved-word results now get a
  trailing underscore (`null_`).

## [0.3.2] — 2026-08-04

### Fixed

- **`flatten_json`** (Snowflake): source keys containing a literal apostrophe
  (e.g. a survey question referencing `invite('custom 2')`) broke out of the
  generated SQL string literal and produced invalid SQL. Key text is now
  escaped before being interpolated into `RAW_DATA['...']` bracket-path
  expressions, in both auto-discovery and `schema_override` modes.
- **`flatten_json`** (Snowflake, auto-discovery): two genuinely distinct
  source keys that normalize to the same alias (e.g. `"Nome"` and `"Nome:"`
  both becoming `nome`) produced a `duplicate column name` SQL error instead
  of building. Colliding aliases are now disambiguated with a numeric suffix
  (`nome`, `nome_2`, ...).

## [0.3.1] — 2026-08-04

### Fixed

- **`flatten_json`** (Snowflake, `mode='columns'`, auto-discovery): the same
  type-inconsistency dedup issue fixed for level-2 keys in 0.3.0 also affected
  level-1 (top-level) key discovery — a top-level key that is `NULL` for some
  sampled rows and typed for others could still produce duplicate columns.
  Level-1 discovery now dedupes the same way.
- **`normalize_alias`**: replaced the fixed list of character replacements with
  a catch-all sanitizer (letters/digits/underscore only, everything else
  collapsed to `_`), and added a leading-digit guard. Fixes invalid
  identifiers from free-text source keys such as survey questions
  (`"19. Principal insight..."` → started with a digit, `?` characters
  weren't stripped).

## [0.3.0] — 2026-08-04

### Fixed

- **`flatten_json`** (Snowflake, `mode='columns'`, auto-discovery): fixed duplicate
  column aliases when a nested object key has inconsistent types across sampled
  rows (e.g. `NULL` for some rows, `OBJECT`/`TEXT` for others). Level-2 key
  discovery now dedupes per child key, preferring the first non-`NULL_VALUE`
  type observed.
- **`normalize_alias`**: now strips parentheses and transliterates common Latin
  accented characters (á, ã, ç, õ, etc.), so source keys like
  `"Usuários Únicos"` or `"Formas (totem)"` produce valid unquoted Snowflake
  identifiers instead of syntax errors.

## [Unreleased] — 0.1.0

### Added

- **Public API**: stable macro names `format_column`, `format_relation`, `flatten_json`,
  `json_extract_scalar`, `json_extract_object`, `json_extract_variant`,
  `get_relation_columns`, `optimize_relation`, `safe_cast`.
- **Adapter dispatch**: all public macros use `adapter.dispatch('macro_name', 'dbt_vitao')`.
- **Snowflake support**: `snowflake__format_column`, `snowflake__format_relation`,
  `snowflake__flatten_json` (modes: `columns`, `rows`), `snowflake__json_extract_*`,
  `snowflake__optimize_relation` (strategies: `cluster_by`, `search_optimization`),
  `snowflake__safe_cast`.
- **PostgreSQL dispatch refactor**: existing PostgreSQL logic moved to `postgres__*` macros
  under `macros/adapters/postgres/`.
- **Default implementations**: clear compiler errors for unsupported adapters.
- **Configurable string formatting** via `vars.dbt_vitao.string_formatting`.
  Supported values: `none`, `trim_only`, `lower`, `upper`, `initcap`. Default: `trim_only`.
- **`flatten_json` modes**: `mode='columns'` for wide projection, `mode='rows'`
  for lateral row explosion.
- **Snowflake search optimization**: `optimize_relation(..., strategy='search_optimization')`
  supports `equality`, `substring`, and `variant` modes.
- **`type_family` utility**: maps raw type strings to logical families.
- **`normalize_alias` utility**: consistent alias normalization across adapters.
- **Integration test fixtures** under `integration_tests/`.
- **Adapter support matrix** in `docs/adapter-support.md`.
- **`dbt_vitao` dispatch block** added to `dbt_project.yml`.

### Changed

- **`cool__formatter`** is now a thin wrapper over `dbt_vitao.format_relation()`.
  - String formatting is explicitly set to `initcap` in the wrapper to preserve
    backward-compatible output.
  - **BREAKING**: JSON columns are no longer auto-expanded inline. They now appear
    as raw column references. Use `dbt_vitao.flatten_json()` to expand JSON columns.
- **`cool__indexer`** is now a thin wrapper over `dbt_vitao.optimize_relation()`.
  Output SQL is functionally identical.
- `dbt_project.yml` dispatch block now includes `dbt_vitao` namespace.

### Deprecated

- `cool__formatter` — use `dbt_vitao.format_relation()`.
- `unnest__json` — internal helper, not a public API. Use `dbt_vitao.flatten_json()`.
- `cool__indexer` — use `dbt_vitao.optimize_relation()`.
- `uindex` — use `dbt_vitao.optimize_relation(strategy='index')` with a unique index option.

---

## [0.0.3] — previous

- PostgreSQL index support via `cool__indexer` (btree, hash, gin, gist, spgist, brin, concurrent).
- Unique index via `uindex`.
- Custom `postgres__create_table_as` for columnar table support.

## [0.0.2] — previous

- `cool__formatter`: automatic column formatting and inline JSONB expansion.
- `unnest__json`: recursive JSONB path expression generator.
- `exclude_col`: column exclusion helper.
