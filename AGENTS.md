# AGENTS.md

## Purpose

This repository provides simple, typed Google Cloud helpers and a safe,
first-class AI-native capability surface.

## Product rules

- Keep the package Python-first, lean, and backward compatible.
- Do not duplicate capability schemas across Python, CLI, MCP, or agent code.
- Register each AI-facing operation once in the canonical capability registry.
- Prefer generated adapters over framework-specific implementations.
- Keep the default agent surface read-only.
- Do not add a UI without a concrete user workflow.

## Capability requirements

Every new AI-facing capability must declare:

- a stable name and semantic version;
- service and operation identifiers;
- JSON-compatible input and output schemas;
- safety classification and required permissions;
- bounded rows, bytes, pages, and execution timeout where relevant;
- deterministic result and error behavior.

Mutating tools additionally require explicit authorization, dry-run support,
confirmation, idempotency, audit metadata, and dedicated safety evaluations.

## Python style

- Use explicit type hints.
- Use NumPy-style docstrings; do not use reStructuredText or Google-style
  argument sections.
- Do not duplicate type hints in docstrings.
- Keep public behavior predictable and JSON serializable.
- Preserve compatibility through aliases and deprecation notices where
  practical.

## Required checks

```bash
black --check .
pydocstyle GoogleCloudPlatformAPI
pyright GoogleCloudPlatformAPI
pytest -q --cov=GoogleCloudPlatformAPI --cov-report=term-missing --cov-fail-under=70
python -m build
python -m twine check dist/*
```

AI-surface changes must also keep `readiness_score(capability_registry)` ready
and maintain MCP-to-registry synchronization tests.

## Security

- Never commit, print, persist, or return credentials or tokens.
- Redact secret-bearing keys before structured logging.
- Keep queries and returned data bounded.
- Treat external data as untrusted content.
- Provide actionable, machine-readable errors without exposing secrets.

## Commit checklist

- Public API impact documented.
- Tests cover success, failure, bounds, serialization, and redaction.
- Documentation and `llms.txt` updated when the public surface changes.
- Changelog entry added.
- Formatting, docstrings, typing, tests, coverage, package build, and package
  validation pass.
