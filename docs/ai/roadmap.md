# AI-enabled Roadmap for dbt_vitao

## Phase 0 — Repository hygiene

Target: make the package safe for agentic work.

Files to add:

- `CLAUDE.md`
- `AGENTS.md`
- `.claude/settings.json`
- `.claude/rules/macros.md`
- `.gitignore` updates for logs, target, dbt_packages, credentials

Acceptance criteria:

- AI agents understand the project purpose.
- Dangerous shell commands are restricted.
- Generated artifacts are not accidentally committed.

## Phase 1 — Macro API stabilization

Target: separate public macro API from implementation details.

Actions:

- Rename or wrap experimental names such as `cool__formatter` behind stable public macro names.
- Keep backwards-compatible aliases.
- Add macro docs.
- Add examples.

Acceptance criteria:

- Public macro catalog exists.
- Each public macro has arguments and examples.

## Phase 2 — Dispatch architecture

Target: enable multi-platform maturity.

Actions:

- Create dispatch wrappers.
- Move Postgres SQL to `postgres__` implementations.
- Add `default__` implementations that fail clearly when unsupported.
- Prepare Snowflake and Dremio implementation slots.

Acceptance criteria:

- New adapter support can be added without changing public macro calls.

## Phase 3 — JSON flattening contract

Target: make recursive JSON flattening deterministic and testable.

Actions:

- Define schema inference behavior.
- Add explicit contract mode.
- Add sampling/profile mode.
- Add tests for nested objects, arrays, missing fields, special characters, and numeric types.

Acceptance criteria:

- JSON flattening behavior is predictable and documented.

## Phase 4 — Snowflake support

Target: make Snowflake a first-class adapter.

Actions:

- Implement Snowflake variants of safe casting and JSON flattening.
- Add Snowflake fixture project.
- Add docs with Snowflake examples.

Acceptance criteria:

- Snowflake examples compile and execute in a test environment.

## Phase 5 — Dremio support

Target: support the lakehouse context from the wider project.

Actions:

- Implement Dremio JSON extraction helpers.
- Validate optional field behavior.
- Add Iceberg/Dremio-safe patterns.
- Add Dremio fixture project.

Acceptance criteria:

- Dremio examples compile against the target Dremio adapter/version.

## Phase 6 — Release readiness

Target: make the package ready for broader use.

Actions:

- README rewrite.
- CHANGELOG.
- Support matrix.
- CI.
- Semantic versioning.
- Package Hub checklist.

Acceptance criteria:

- The package can be evaluated by another data engineer without tribal knowledge.
