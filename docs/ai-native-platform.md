# AI-native platform contract

GoogleCloudPlatformAPI is Python-first and exposes one canonical capability
registry for all AI-facing operations.

## Contract

Every capability declares:

- a stable name and semantic version;
- service and operation identifiers;
- JSON-compatible input and output schemas;
- safety classification and required permissions;
- a bounded execution timeout;
- deterministic, serializable result metadata.

The canonical registry is available as
`GoogleCloudPlatformAPI.ai_native.capability_registry`.

## Execution envelope

AI-facing executions return:

- `ok`;
- `data`;
- `metadata`, including request ID, service, operation, duration, truncation,
  warnings, provenance, and pagination cursor;
- a machine-readable `error` with code, message, retryability, guidance, and
  details.

`execute_capability` adds request IDs, timing metadata, structured logging hooks,
and stable errors around adapters. `redact` removes common secret-bearing keys
before arguments are logged.

## MCP and agents

The MCP server generates tool definitions directly from the registry. This
prevents schema drift between the package and Codex-compatible clients. The
framework-neutral registry can also be consumed by agent adapters without
introducing framework-specific dependencies into the core package.

## Evaluation

`evaluate_registry` and `readiness_score` provide deterministic CI-friendly
checks for registry completeness, object input schemas, bounded timeouts, and
semantic versions. Live model calls are intentionally not required.

## Safety

The shipped surface is read-only. BigQuery accepts only `SELECT`, `WITH`, and
`EXPLAIN`. Result rows, listed objects, object bytes, and execution timeouts are
bounded. Credentials are inherited from the local environment and are never
copied or persisted.

Mutating capabilities must not be added until they implement explicit
permissions, dry-run behavior, confirmation, idempotency, audit metadata, and
safety evaluations.

## Adding a capability

1. Add one `Capability` definition to the registry source.
2. Add or connect its Python handler.
3. Add contract, safety, boundedness, and serialization tests.
4. Confirm MCP definitions are generated without manual schema duplication.
5. Run the readiness scorecard and the full CI matrix.
6. Document compatibility and release impact.
