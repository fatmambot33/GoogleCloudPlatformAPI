# AI-native release scorecard

GoogleCloudPlatformAPI exposes one canonical, versioned capability registry for
all AI-facing operations.

## Required release gates

- [x] Stable capability names and semantic versions
- [x] Strict JSON Schema inputs and outputs
- [x] Inspection, billable-read, and mutation safety classifications
- [x] Permission metadata and bounded execution timeouts
- [x] MCP definitions generated from the canonical registry
- [x] Registry-driven adapter dispatch
- [x] Input and output validation at runtime
- [x] BigQuery dry-run estimates and `maximum_bytes_billed`
- [x] Single-statement SQL and `SELECT` classification enforcement
- [x] BigQuery result timeout and cancellation request
- [x] Provider-level ranged Cloud Storage reads
- [x] Context-bound opaque pagination cursors
- [x] Structured result and error envelopes
- [x] Stable provider error codes and recovery guidance
- [x] Request IDs, duration metadata, and structured logging hooks
- [x] Recursive key and message secret redaction
- [x] Optional OpenTelemetry traces and metrics
- [x] Deterministic readiness and contract compatibility evaluations
- [x] Contract, serialization, synchronization, safety, and installed-wheel tests
- [x] Machine-readable `llms.txt`

Mutation capabilities remain intentionally disabled. Any future mutating tool
must add explicit authorization, dry-run behavior, confirmation, idempotency,
audit metadata, and dedicated safety evaluations before release.
