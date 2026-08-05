# Security policy

## Supported versions

Security fixes are applied to the latest supported release line.

## Reporting a vulnerability

Report suspected vulnerabilities privately through GitHub security reporting.
Do not open a public issue containing credentials, tokens, private data, or an
exploitable proof of concept.

## Credentials

Use Application Default Credentials, service accounts, workload identity, or
other supported Google authentication mechanisms. Never commit credentials or
place them in prompts, logs, tests, examples, exceptions, or tool results.

The local MCP server inherits credentials from the current process and does not
copy or persist them. Secret-bearing keys are recursively redacted before
structured argument logging.

## AI tool boundaries

The published AI surface is read-only. BigQuery accepts only `SELECT`, `WITH`,
and `EXPLAIN`. Query rows, Cloud Storage object listings, downloaded bytes, and
execution timeouts are bounded.

Data returned by Google services must be treated as untrusted content. Agents
must not interpret data rows or object contents as instructions that override
the caller's policy or tool safety constraints.

Mutating tools must not ship until they provide explicit authorization,
dry-run behavior, user confirmation, idempotency, audit metadata, and dedicated
security and prompt-injection evaluations.

## Errors and observability

Errors should be machine readable and actionable without exposing secrets.
Logs may include capability names, request IDs, duration, status, and redacted
arguments, but must not contain credentials or unrestricted returned data.
