# AI Architecture for dbt_vitao

## Objective

Create a lightweight AI operating system around `dbt_vitao` so AI agents can safely and consistently improve the dbt package.

The target is not just "prompts". The target is a versioned AI architecture composed of:

- persistent project instructions,
- scoped rules,
- reusable skills,
- specialized subagents,
- repeatable commands,
- safety settings/hooks,
- evaluation fixtures,
- release-readiness workflow.

## Recommended repository layout

```text
.
├── CLAUDE.md
├── AGENTS.md
├── .claude/
│   ├── settings.json
│   ├── rules/
│   │   └── macros.md
│   ├── commands/
│   │   └── review-package.md
│   ├── agents/
│   │   ├── dbt-package-architect.md
│   │   ├── adapter-portability-engineer.md
│   │   ├── macro-test-engineer.md
│   │   └── docs-release-agent.md
│   └── skills/
│       ├── dbt-macro-authoring/
│       │   └── SKILL.md
│       ├── dbt-adapter-portability/
│       │   └── SKILL.md
│       ├── json-flattening-contract/
│       │   └── SKILL.md
│       └── package-release-readiness/
│           └── SKILL.md
├── docs/
│   └── ai/
│       ├── architecture.md
│       ├── agent-operating-model.md
│       └── roadmap.md
└── integration_tests/
    └── README.md
```

## Design principles

### 1. Keep `CLAUDE.md` small and stable

Use `CLAUDE.md` for facts that should always be loaded: project purpose, architecture, commands, conventions, and non-negotiable rules.

Move long procedures into skills or commands.

### 2. Use skills for reusable workflows

Skills are appropriate when the agent needs a repeatable method, examples, edge cases, and task-specific rules.

For this project, the first four skills should be:

- dbt macro authoring,
- adapter portability,
- JSON flattening contract design,
- package release readiness.

### 3. Use subagents for context isolation

Subagents should be used when the task would otherwise flood the main context with code searches, docs, logs, or test output.

Good examples:

- package architecture review,
- adapter portability analysis,
- test fixture design,
- docs/release review.

### 4. Use rules for path-scoped behavior

Rules should apply only to the relevant files. For example, macro rules should activate for `macros/**/*.sql` and `macros/**/*.yml`.

### 5. Use commands for repeatable reviews

Commands are useful for standard manual workflows such as `/review-package`.

### 6. Treat AI output as code requiring tests

No macro change should be considered complete until it parses, compiles, and has at least one fixture or example.

## Package-specific AI architecture

`dbt_vitao` should be managed as a productized dbt package. The AI system should push all work toward this structure:

```text
macro contract
  -> public API
  -> dispatch implementation
  -> adapter-specific SQL
  -> docs
  -> examples
  -> compile tests
  -> release notes
```

## Main risk to control

The biggest AI risk in this project is overclaiming portability.

A macro that works in Postgres is not automatically compatible with Snowflake or Dremio. The AI system should force every adapter claim through dispatch, docs, and tests.
