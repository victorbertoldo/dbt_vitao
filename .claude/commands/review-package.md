# /review-package

Review the current dbt package for maturity.

Steps:

1. Inspect `dbt_project.yml`, `macros/**`, macro YAML docs, tests, and examples.
2. Identify the public macro API and internal helper macros.
3. Classify each macro as:
   - stable public API
   - adapter-specific implementation
   - internal helper
   - experimental
4. Check whether each macro is documented and tested.
5. Check adapter portability risks, especially Postgres-only syntax.
6. Produce a report with:
   - current state
   - maturity gaps
   - adapter gaps for Snowflake and Dremio
   - recommended roadmap
   - exact files to change first

Do not modify files unless explicitly asked.
