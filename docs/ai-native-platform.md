# AI-native platform contract

GoogleCloudPlatformAPI exposes one canonical capability registry for Python,
CLI, MCP/Codex, and future agent adapters.

## Contract

Every capability has a stable name, service, operation, description, semantic
version, JSON-compatible input and output schemas, safety level, permissions,
and timeout. Capability results use one deterministic envelope containing:

- `ok` and JSON-compatible `data`;
- request, service, operation, duration, truncation, cursor, warning, and
  provenance metadata;
- a machine-readable error with a stable code, retry classification, recovery
  guidance, and optional details.

## Safety policy

Read-only capabilities are the default. Mutating capabilities must not be
published until they implement explicit authorization, dry-run behavior,
confirmation, idempotency, bounded execution, audit metadata, and regression
coverage.

Credentials must remain in the local process environment. Schemas, logs,
results, exceptions, tests, and documentation must never expose secrets.

## Current registry

- `gcp_context`
- `bigquery_query`
- `gcs_list_objects`
- `gcs_read_text`

The registry is available from Python:

```python
from GoogleCloudPlatformAPI.ai_native import capability_registry

for capability in capability_registry.list():
    print(capability.name, capability.safety.value)

schema = capability_registry.schema()
```

## Adding a capability

1. Define the typed operation in the service layer.
2. Add a stable `Capability` definition to the registry.
3. Keep inputs and outputs deterministic and JSON compatible.
4. Apply explicit row, byte, page, and time limits.
5. Add contract, permission, failure, truncation, and redaction tests.
6. Generate adapter metadata from the registry; do not duplicate schemas.
7. Record compatibility impact and update the changelog.
