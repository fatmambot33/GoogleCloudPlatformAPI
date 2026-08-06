# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Added

- Inspection, billable-read, and mutating capability safety classifications.
- Provider-enforced BigQuery dry runs, billing ceilings, deadlines,
  cancellation, and cost/job metadata.
- Conservative single-statement BigQuery SQL validation.
- Context-bound opaque cursors for BigQuery and Cloud Storage discovery.
- Cloud Storage byte-range reads that avoid downloading complete objects before
  truncation.
- Stable authentication, permission, quota, timeout, availability, not-found,
  and invalid-request error codes with recovery guidance.
- Structured MCP tool errors instead of leaking provider exceptions through the
  JSON-RPC boundary.
- Generic capability deadlines and optional OpenTelemetry tracing and metrics.
- Strict input and output JSON Schema contracts for every AI-facing capability.
- Dependency-free runtime validation before and after adapter execution.
- Generated MCP output schemas and safety annotations.
- Registry-driven adapter dispatch instead of a second hard-coded tool map.
- `gcp-api-agent --list-capabilities` for machine-readable capability discovery.
- Generated compatibility snapshots, API-diff classification, and Markdown
  capability references.
- Capability deprecation and replacement metadata.

### Changed

- BigQuery query execution is explicitly classified as a billable read.
- The registry compatibility snapshot now records safety and timeout changes.
- The registry schema version is 1.1.0.
- AI readiness requires strict schemas and a declared execution adapter.

### Security

- Secret redaction covers authorization headers, generic tokens, API keys,
  passwords, secrets, and credential paths.
- Queries and object reads are bounded at the provider request rather than only
  truncating fully retrieved results.

### Planned

- Behavioral agent evaluations and generated release evidence from #59.

## 2.7.0 - 2026-08-06

### Added

- Canonical AI-native capability registry with stable names, semantic versions,
  JSON-compatible schemas, permission metadata, safety classifications, and
  bounded timeouts.
- Structured capability result and error envelopes with request IDs, duration,
  truncation, warning, provenance, and cursor metadata.
- Observable capability execution runtime with structured logging hooks and
  recursive secret redaction.
- Deterministic AI readiness evaluations and release scorecard.
- Machine-readable `llms.txt` documentation index.
- Discovery-first BigQuery dataset, table, and schema tools.
- Cloud Storage object metadata inspection before bounded content reads.
- Stable package-root imports and a documented package exception hierarchy.
- Packaged Codex skill, machine-readable index, and AI platform documentation.
- Installed-wheel and MCP entry-point smoke tests.
- Minimum dependency compatibility testing, CycloneDX SBOM generation, and
  release provenance attestations.

### Changed

- MCP/Codex tool definitions are generated from the canonical capability
  registry instead of duplicated manually.
- The Codex workflow now discovers resources before running queries or reads.
- Python 3.10 through 3.14 support is explicit in package metadata and CI.
- Runtime dependencies now use bounded compatibility ranges.
- PyPI publishing uses Trusted Publishing with GitHub OIDC when configured and
  a masked migration token otherwise.

### Security

- The published tool surface remains bounded. Mutation tools are explicitly
  gated on authorization, dry-run, confirmation, idempotency, audit, and
  evaluation controls.
- Release distributions receive GitHub provenance attestations and an attached
  software bill of materials.

## 2.6.0

- Added the typed agent runtime and `gcp-api-agent` command.
- Added Codex/MCP support for bounded read-only BigQuery and Cloud Storage use.
- Added product, contribution, support, security, roadmap, release, and agent
  guidance documentation.

## 2.5.0

- Added a local read-only MCP server for Codex-compatible clients.

## 2.4.0

- Improved packaging, documentation, tests, and release automation.
