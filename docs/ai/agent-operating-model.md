# Agent Operating Model for dbt_vitao

## Agent routing

Use the following routing model:

| Task | Primary agent | Supporting skill |
|---|---|---|
| Review package maturity | dbt-package-architect | package-release-readiness |
| Add Snowflake implementation | adapter-portability-engineer | dbt-adapter-portability |
| Add Dremio implementation | adapter-portability-engineer | dbt-adapter-portability |
| Refactor a macro | dbt-package-architect | dbt-macro-authoring |
| Test JSON flattening | macro-test-engineer | json-flattening-contract |
| Prepare release notes | docs-release-agent | package-release-readiness |

## Standard workflow

1. Explore current state.
2. Plan changes.
3. Implement in the smallest safe slice.
4. Run parse/compile/tests.
5. Update docs.
6. Produce a short architectural summary.

## Required report format

```text
Current state:
Decision:
Files changed:
Validation performed:
Risks remaining:
Next step:
```

## Human approval gates

Require explicit human approval before:

- deleting files,
- changing public macro names,
- changing materialization internals,
- tagging a release,
- claiming support for a new adapter,
- publishing to a package registry.

## Context hygiene

Agents should not load every file by default. They should start with:

- `dbt_project.yml`,
- `README.md`,
- relevant `macros/**`,
- relevant YAML docs,
- tests/examples for the macro being changed.

Avoid context bloat.
