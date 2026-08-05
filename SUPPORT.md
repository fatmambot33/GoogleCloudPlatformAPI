# Support

## Getting help

Use GitHub issues for reproducible bugs, documentation problems, and focused
feature requests. Include the package version, Python version, operating system,
service involved, minimal reproduction, and the sanitized error code or request
ID.

Never include credentials, access tokens, service-account JSON, private query
results, or sensitive object contents.

## AI and MCP diagnostics

For agent-facing problems, include:

- the capability name and semantic version;
- the MCP client or agent adapter;
- sanitized arguments;
- the machine-readable error code and guidance;
- whether the result was truncated;
- the readiness scorecard output;
- confirmation that the issue reproduces with current `main`.

## Feature requests

New capabilities should identify the user workflow, required Google permission,
read-only or mutation classification, expected input and output schemas, bounds,
and compatibility impact.

Requests for mutation tools must also explain authorization, dry-run,
confirmation, idempotency, audit, and evaluation requirements. Framework or UI
integrations should demonstrate concrete demand before becoming core scope.
