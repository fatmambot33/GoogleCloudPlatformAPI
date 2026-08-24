# Changelog

All notable changes to this project are documented in this file.

## Unreleased

### Fixed

- Removed stale package-version literals from CI wheel validation.
- MCP initialization now reports installed package metadata and uses an explicit
  `0+unknown` fallback when distribution metadata is unavailable.
- Added regression coverage so package and MCP version reporting cannot drift
  silently on future releases.

## 2.8.1 - 2026-08-06

### Changed

- Removed the legacy PyPI API-token publishing fallback.
- Releases now publish exclusively through PyPI Trusted Publishing with GitHub OIDC.

## 2.8.0 - 2026-08-06

### Added

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
- Fourteen deterministic golden scenarios covering all eight AI-facing
  capabilities, discovery sequencing, bounded arguments, refusal, truncation,
  pagination, and provider-error recovery.
- Prompt-injection containment fixtures for instructions embedded in BigQuery
  rows and Cloud Storage text.
- MCP protocol conformance checks for initialization, ping, tool discovery,
  strict schemas, notifications, and JSON-RPC errors.
- `gcp-api-eval` for generating JSON, Markdown, and JUnit AI readiness evidence
  without credentials, network access, or provider calls.
- Release metrics for schema validity, behavior, safety, protocol conformance,
  latency budgets, compatibility, and approximate token footprint.
- Baseline scorecard comparison that fails on category or overall-score
  regressions.
- Optional OpenAI Agents SDK tools and Agent builders generated from the
  canonical capability registry.
- Installed-wheel evaluation smoke tests and uploaded CI/release scorecards.

### Changed

- BigQuery query execution is explicitly classified as a billable read.
- Provider reads are bounded at the request rather than after full retrieval.
- AI readiness is an evidence-backed release gate rather than a registry-only
  checklist score.
- Release evidence is generated from the same deterministic checks used by CI.
- The roadmap is complete through issue #59.

### Security

- Secret redaction covers authorization headers, generic tokens, API keys,
  passwords, secrets, and credential paths.
- Provider-controlled row and object content is explicitly classified as
  untrusted data and must not create derived tool calls.
- Mutation requests remain refusals because the shipped capability surface has
  no write tools.

## 2.7.0 - 2026-08-06

### Added

- Canonical AI-native capability registry with stable names, semantic versions,
  JSON-compatible schemas, permission metadata, safety classifications, and
  bounded timeouts.
- Strict input and output JSON Schema contracts for every AI-facing capability.
- Registry-driven MCP and CLI tool discovery and adapter dispatch.
- Structured capability result and error envelopes with request IDs, duration,
  truncation, warnings, provenance, and cursor metadata.
- Machine-readable `llms.txt` documentation index and packaged Codex skill.
- Discovery-first BigQuery dataset, table, and schema tools.
- Cloud Storage object metadata inspection before bounded content reads.
- Framework-neutral agent runtime and `gcp-api-agent` command.
- Stable package-root imports and a documented package exception hierarchy.
- Python 3.10 through 3.14 CI, minimum-dependency testing, clean-wheel and MCP
  smoke tests, CycloneDX SBOMs, and provenance attestations.

### Changed

- MCP tool definitions are generated from the canonical capability registry.
- Runtime dependencies use bounded compatibility ranges.
- PyPI publishing uses Trusted Publishing with GitHub OIDC when configured and
  a masked migration token otherwise.

### Security

- The published tool surface contains no mutation tools.
- Credentials remain local and release distributions receive provenance and an
  attached software bill of materials.

## 2.6.0

- Added the typed agent runtime and `gcp-api-agent` command.
- Added Codex/MCP support for bounded read-only BigQuery and Cloud Storage use.
- Added product, contribution, support, security, roadmap, release, and agent
  guidance documentation.

## 2.5.0

- Added a local read-only MCP server for Codex-compatible clients.

## 2.4.0

- Improved packaging, documentation, tests, and release automation.
