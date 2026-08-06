# AI-native platform contract

GoogleCloudPlatformAPI is Python-first and exposes one canonical capability
registry for all AI-facing operations.

## Strict capability contract

Every capability declares:

- a stable lowercase name and semantic version;
- service and operation identifiers;
- strict top-level JSON Schema inputs and outputs;
- one adapter method or bound Python handler;
- inspection, billable-read, or mutating safety classification;
- required permissions and a bounded execution timeout;
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

## Execution envelope

AI-facing executions return:

- `ok`;
- validated `data`;
- `metadata`, including request ID, service, operation, duration, safety,
  timeout, truncation, warnings, provenance, and pagination cursor;
- a machine-readable `error` with code, message, retryability, guidance, and
  redacted details.

`execute_capability` validates arguments, adds request IDs and timing metadata,
enforces a wall-clock deadline, validates results, emits structured logs, and
wraps adapter failures in a stable envelope.

## BigQuery billable-read controls

`bigquery_query` is a billable read, not passive inspection. It:

1. validates one conservative `SELECT`, `WITH`, or `EXPLAIN` statement;
2. rejects scripts, multiple statements, and mutating statement keywords;
3. performs a dry run and requires BigQuery to classify the statement as
   `SELECT`;
4. rejects estimates above `maximum_bytes_billed`;
5. executes with the same billing ceiling and a bounded job timeout;
6. requests job cancellation after a result timeout;
7. returns dry-run bytes, processed and billed bytes, cache status, job ID, and
   statement type.

The default billing ceiling is 1 GB and can be raised only within the declared
1 TB schema maximum.

## Bounded discovery and object reads

BigQuery dataset/table discovery and Cloud Storage object discovery return one
bounded provider page plus an opaque `next_cursor`. Cursors are bound to the
service, operation, and resource context, so a cursor for one dataset, bucket,
or prefix cannot be reused for another.

Cloud Storage text reads request only bytes `0..max_bytes` and return at most
`max_bytes`; the extra byte is used solely to report truncation. The adapter
never downloads the complete object merely to truncate it locally.

## Error recovery

Provider exceptions are normalized into stable codes for authentication,
permission, quota, timeout, service availability, not-found, and invalid
request failures. Errors include retryability and recovery guidance. Secret
redaction covers authorization headers, generic and named tokens, API keys,
passwords, secrets, and credential paths.

MCP tool failures use structured `isError` results rather than exposing raw
provider exceptions through the JSON-RPC protocol boundary.

## Optional observability

Structured logs are always available. Install the optional telemetry extra to
add OpenTelemetry spans and counters without changing the core dependency set:

```bash
pip install 'GoogleCloudPlatformAPI[telemetry]'
```

The telemetry hooks record capability name, service, operation, safety,
duration, success, and retry count. They never export data by themselves; the
application controls the OpenTelemetry provider and exporter.

## Compatibility policy

`compatibility_snapshot` records stable names, versions, schemas, safety,
timeouts, and deprecation metadata. `compare_compatibility_snapshots`
classifies removed capabilities/properties, type changes, new required fields,
and safety changes as breaking.

Breaking changes require either a compatibility layer or a major release.
Deprecated capabilities must identify their replacement.

## Safety

The shipped surface provides inspection and explicitly bounded billable reads.
No mutation capability is exposed. Credentials are inherited from the local
environment and are never copied or persisted.

Mutating capabilities must not be added until they implement explicit
permissions, dry-run behavior, confirmation, idempotency, audit metadata, and
dedicated safety evaluations.
