# Changelog

All notable changes to this project are documented in this file.

## Unreleased

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

### Changed

- MCP/Codex tool definitions are generated from the canonical capability
  registry instead of duplicated manually.
- The Codex workflow now discovers resources before running queries or reads.
- Python 3.8 through 3.12 support is explicit in package metadata.

### Security

- The published tool surface remains read-only and bounded. Mutation tools are
  explicitly gated on authorization, dry-run, confirmation, idempotency, audit,
  and evaluation controls.

## 2.6.0

- Added the typed agent runtime and `gcp-api-agent` command.
- Added Codex/MCP support for bounded read-only BigQuery and Cloud Storage use.
- Added product, contribution, support, security, roadmap, release, and agent
  guidance documentation.

## 2.5.0

- Added a local read-only MCP server for Codex-compatible clients.

## 2.4.0

- Improved packaging, documentation, tests, and release automation.
