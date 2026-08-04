# Contributing

Contributions should be small, focused, and aligned with `PRODUCT.md`.

## Workflow

1. Open or select a GitHub issue.
2. Confirm the problem, scope, and acceptance criteria.
3. Create a focused branch.
4. Implement the smallest complete change.
5. Add or update tests, type hints, and documentation.
6. Update `CHANGELOG.md` for user-facing changes.
7. Run the project checks.
8. Open a pull request that references the issue.

## Required Checks

```bash
black --check .
pydocstyle GoogleCloudPlatformAPI
pyright GoogleCloudPlatformAPI
pytest -q --cov=GoogleCloudPlatformAPI --cov-report=term-missing --cov-fail-under=70
```

## Design Rules

- Prefer explicit APIs over hidden behavior.
- Reuse official Google clients rather than reimplementing them.
- Keep interfaces consistent across services.
- Avoid unrelated refactoring in focused changes.
- Preserve backward compatibility within a major version.
- Use NumPy-style docstrings without duplicating type information.

## Pull Requests

A pull request should explain what changed, why it changed, how it was validated, and whether it affects compatibility. It should close or reference the relevant issue.
