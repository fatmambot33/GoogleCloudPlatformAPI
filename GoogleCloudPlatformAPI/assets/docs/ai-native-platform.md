# AI-native platform contract

GoogleCloudPlatformAPI is Python-first and exposes one canonical capability
registry for all AI-facing operations.

## Strict capability contract

Every capability declares:

- a stable lowercase name and semantic version;
- service and operation identifiers;
- strict top-level JSON Schema inputs and outputs;
- one adapter method or bound Python handler;
- safety classification and required permissions;
- a bounded execution timeout;
- deprecation and replacement metadata when applicable.

Unknown top-level fields are rejected. Inputs are validated before execution and
adapter results are validated before crossing Python, CLI, MCP, or agent
boundaries. Contract failures use stable `input_validation_failed` and
`output_validation_failed` error codes.

The canonical registry is available as
`GoogleCloudPlatformAPI.ai_native.capability_registry`.

## Generated surfaces

The registry generates:

- MCP input schemas, output schemas, and safety annotations;
- the `gcp-api-agent --list-capabilities` JSON reference;
- compatibility snapshots for API-diff checks;
- a compact Markdown capability reference.

Adapter dispatch uses each capability's `adapter_method`; the Codex layer does
not maintain a second name-to-handler table.

```python
from GoogleCloudPlatformAPI.ai_native import (
    capability_reference_markdown,
    capability_registry,
    compatibility_snapshot,
)

print(capability_registry.schema())
print(compatibility_snapshot(capability_registry))
print(capability_reference_markdown(capability_registry))
```

## Execution envelope

AI-facing executions return:

- `ok`;
- validated `data`;
- `metadata`, including request ID, service, operation, duration, truncation,
  warnings, provenance, and pagination cursor;
- a machine-readable `error` with code, message, retryability, guidance, and
  details.

`execute_capability` validates arguments, adds request IDs and timing metadata,
validates results, and wraps adapter failures in a stable envelope. `redact`
removes common secret-bearing keys before arguments are logged.

## Compatibility policy

`compatibility_snapshot` records stable names, versions, schemas, and
deprecation metadata. `compare_compatibility_snapshots` classifies changes:

- `compatible`: no public contract change;
- `additive`: new capabilities or non-breaking metadata/schema expansion;
- `breaking`: removed capabilities, removed properties, changed property types,
  or newly required inputs.

Breaking changes require either a compatibility layer or a major release.
Deprecated capabilities must identify their replacement.

## Evaluation

`evaluate_registry` and `readiness_score` provide deterministic CI-friendly
checks for strict schemas, adapter binding, bounded timeouts, and semantic
versions. Live model calls are intentionally not required at this layer.

## Safety

The shipped surface remains read-only. BigQuery accepts only `SELECT`, `WITH`,
and `EXPLAIN`. Result rows, listed objects, object bytes, and declared execution
timeouts are bounded. Credentials are inherited from the local environment and
are never copied or persisted.

Mutating capabilities must not be added until they implement explicit
permissions, dry-run behavior, confirmation, idempotency, audit metadata, and
dedicated safety evaluations.

## Adding a capability

1. Add one strict `Capability` definition to the registry source.
2. Identify its adapter method or bind a direct Python handler.
3. Add contract, safety, boundedness, validation, and serialization tests.
4. Confirm MCP and CLI references are generated without manual duplication.
5. Review the compatibility diff and increment the capability version.
6. Run the readiness scorecard and the full CI matrix.
7. Document compatibility and release impact.
