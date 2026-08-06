# AI-native release scorecard

GoogleCloudPlatformAPI exposes one canonical, versioned capability registry for
all AI-facing operations.

## Required release gates

- [x] Stable capability names and semantic versions
- [x] JSON Schema inputs and outputs
- [x] Read-only safety classification and permission metadata
- [x] Bounded rows, bytes, pagination sizes, and timeouts
- [x] MCP definitions generated from the canonical registry
- [x] Structured result and error envelopes
- [x] Request IDs, duration metadata, and structured logging hooks
- [x] Recursive secret redaction
- [x] Deterministic readiness evaluations
- [x] Contract, serialization, synchronization, and safety tests
- [x] Machine-readable `llms.txt`

Mutation capabilities remain intentionally disabled. Any future mutating tool
must add explicit authorization, dry-run behavior, confirmation, idempotency,
audit metadata, and dedicated safety evaluations before release.
