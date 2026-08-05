# Roadmap

## Completed

- [x] Product vision and repository standards
- [x] Typed Python packaging and public type marker
- [x] Explicit Python 3.8 through 3.12 runtime policy
- [x] Stable package-root imports and package exception hierarchy
- [x] Quality, test, coverage, package, and release automation
- [x] Discovery-first local read-only Codex/MCP server
- [x] BigQuery dataset, table, schema, and bounded query workflow
- [x] Cloud Storage listing, metadata, and bounded text-read workflow
- [x] Framework-neutral agent runtime and CLI
- [x] Canonical AI-native capability registry
- [x] Generated MCP schemas from one source of truth
- [x] Structured result and error envelopes
- [x] Request IDs, duration metadata, logging hooks, and secret redaction
- [x] Deterministic AI readiness evaluation and release scorecard
- [x] Human and machine-readable AI platform documentation

## Deliberately gated

Mutation tools, remote HTTP deployment, OpenAPI, TypeScript, and
framework-specific adapters are not default roadmap requirements. They should
only be added when concrete usage requires them and after the shared registry,
safety, authorization, audit, and compatibility contracts remain satisfied.
