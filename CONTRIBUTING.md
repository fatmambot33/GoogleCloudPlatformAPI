# Contributing

Contributions should keep GoogleCloudPlatformAPI simple, typed, safe, and
backward compatible.

## Workflow

1. Start from the current default branch.
2. Keep each pull request focused on one coherent change.
3. Add or update tests and documentation.
4. Record user-visible changes in the changelog.
5. Run the complete checks before requesting review.

## AI-facing capabilities

Register each operation once in the canonical capability registry. Do not
manually duplicate schemas in MCP, CLI, or agent adapters.

A capability proposal must include:

- stable name and semantic version;
- service and operation identifiers;
- typed, JSON-compatible input and output contracts;
- required Google permissions;
- read-only or mutation safety classification;
- row, byte, page, and timeout bounds;
- success, failure, truncation, serialization, and redaction tests.

Mutating capabilities additionally require authorization, dry-run behavior,
confirmation, idempotency, audit metadata, and dedicated safety evaluations.

## Checks

```bash
black --check .
pydocstyle GoogleCloudPlatformAPI
pyright GoogleCloudPlatformAPI
pytest -q --cov=GoogleCloudPlatformAPI --cov-report=term-missing --cov-fail-under=70
python -m build
python -m twine check dist/*
```

AI-surface changes must keep the deterministic readiness score at 100% and
preserve MCP-to-registry synchronization.
