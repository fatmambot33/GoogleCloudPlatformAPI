# Release process

## Pre-release checks

Run:

```bash
black --check .
pydocstyle GoogleCloudPlatformAPI
pyright GoogleCloudPlatformAPI
pytest -q --cov=GoogleCloudPlatformAPI --cov-report=term-missing --cov-fail-under=70
python -m build
python -m twine check dist/*
```

For AI-facing changes, also verify:

```python
from GoogleCloudPlatformAPI.ai_native import (
    capability_registry,
    readiness_score,
)

assert readiness_score(capability_registry)["ready"] is True
```

Confirm that:

- MCP definitions are generated from the canonical registry;
- every capability has schemas, permissions, safety level, version, and timeout;
- result and error envelopes remain JSON serializable;
- secret redaction tests pass;
- mutation tools have not bypassed authorization, dry-run, confirmation,
  idempotency, audit, and evaluation requirements;
- README, AI platform documentation, `llms.txt`, roadmap, and changelog are
  current.

## Versioning

Use semantic versioning. Additive capabilities may ship in a minor release.
Breaking public API, schema, capability-name, or result-envelope changes require
a major release unless preserved through a documented compatibility layer.

## Publish

1. Update the version and changelog.
2. Open and merge a release pull request after CI succeeds.
3. Create the GitHub release and tag.
4. Publish the validated distributions to PyPI using trusted publishing.
5. Verify installation, CLI entry points, MCP startup, registry readiness, and
   package metadata from the published artifact.
