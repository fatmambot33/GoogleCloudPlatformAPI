# Release process

## Pre-release checks

Run:

```bash
black --check .
pydocstyle GoogleCloudPlatformAPI
pyright GoogleCloudPlatformAPI
pytest -q --cov=GoogleCloudPlatformAPI --cov-report=term-missing --cov-fail-under=90
python -m build
python -m twine check dist/*
```

For AI-facing changes, also verify:

```python
from GoogleCloudPlatformAPI.ai_native import capability_registry, readiness_score

assert readiness_score(capability_registry)["ready"] is True
```

Confirm that:

- MCP definitions are generated from the canonical registry;
- every capability has schemas, permissions, safety level, version, and timeout;
- result and error envelopes remain JSON serializable;
- secret redaction tests pass;
- the built wheel contains `llms.txt`, the Codex skill, and AI platform docs;
- mutation tools have not bypassed authorization, dry-run, confirmation,
  idempotency, audit, and evaluation requirements;
- README, AI platform documentation, `llms.txt`, roadmap, and changelog are
  current.

## Versioning

Use semantic versioning. Additive capabilities may ship in a minor release.
Breaking public API, schema, capability-name, or result-envelope changes require
a major release unless preserved through a documented compatibility layer.

## Trusted Publishing setup

Configure the PyPI project once with this GitHub trusted publisher:

- owner: `fatmambot33`
- repository: `GoogleCloudPlatformAPI`
- workflow: `python-publish.yml`
- environment: `pypi`

The release workflow uses GitHub OIDC and does not read a long-lived PyPI token.
Protect the `pypi` environment with required reviewers when appropriate.

## Publish

1. Update the version and changelog.
2. Open and merge a release pull request after CI succeeds.
3. Create the GitHub release and tag, for example `v2.7.0`.
4. The release workflow builds and smoke-tests the wheel, generates an SBOM,
   creates provenance attestations, attaches release evidence, and publishes to
   PyPI through Trusted Publishing.
5. Verify installation, CLI entry points, MCP startup, packaged documentation,
   registry readiness, and package metadata from the published artifact.
