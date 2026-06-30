---
name: json-flattening-contract
description: Design, review, and test recursive JSON flattening macros for dbt_vitao, including nested objects, arrays, optional fields, and adapter-specific JSON syntax.
---

# JSON flattening contract skill

Use this skill for any macro related to JSON introspection, flattening, parsing, or nested field extraction.

## Contract first

Before writing SQL, define:

- Input relation or source.
- JSON column name.
- Whether the column is object, array, text JSON, JSONB, VARIANT, or STRUCT-like.
- Maximum recursion depth.
- Expected column naming convention.
- Behavior for optional or missing fields.
- Behavior for arrays.
- Behavior for invalid JSON.

## Required edge cases

Test with JSON containing:

```json
{
  "id": 1,
  "user": {
    "name": "Ana",
    "contact.email": "ana@example.com"
  },
  "events": [
    {"type": "click", "value": 10},
    {"type": "view", "value": null}
  ],
  "@metadata": {
    "source": "api"
  }
}
```

Also test:

- Missing nested field.
- Empty object.
- Empty array.
- Array with mixed objects.
- Key with spaces, dots, colons, and `@`.
- Numeric-like strings and large integers.

## Naming convention

Default flattened column names should:

- Use lowercase.
- Replace spaces, dots, colons, slashes, and `@` with `_` or remove them consistently.
- Avoid duplicate aliases.
- Preserve enough path context to avoid collisions.

## Safety rule

Do not infer a production schema from one arbitrary sampled row without documenting the risk. Prefer either:

- explicit user-provided contract, or
- profiling across multiple rows, or
- generated recommendations reviewed by a human.
