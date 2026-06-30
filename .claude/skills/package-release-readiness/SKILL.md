---
name: package-release-readiness
description: Assess dbt_vitao readiness for internal release, semantic versioning, documentation quality, CI, and dbt Package Hub maturity.
---

# package release readiness skill

Use this skill before publishing or tagging a release.

## Checklist

1. `dbt_project.yml` version updated.
2. README explains purpose, installation, examples, and adapter support.
3. CHANGELOG has user-facing release notes.
4. Every public macro is documented in YAML.
5. Integration examples exist.
6. CI runs parse/compile/test.
7. Adapter support matrix is explicit.
8. Breaking changes are called out.
9. No credentials, logs, target folders, or dbt_packages are committed.
10. Package naming is consistent.

## Maturity levels

- Level 0: prototype macros.
- Level 1: documented single-adapter package.
- Level 2: tested single-adapter package.
- Level 3: cross-adapter package with dispatch and tests.
- Level 4: Package Hub-ready with CI, docs, examples, semantic releases.

## Report format

```text
Current maturity:
Release blockers:
Adapter support:
Documentation gaps:
Testing gaps:
Security/repo hygiene gaps:
Recommended next tag:
```
