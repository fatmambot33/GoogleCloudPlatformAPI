# Public API contract

GoogleCloudPlatformAPI exposes its stable service classes and package exceptions from the package root:

```python
from GoogleCloudPlatformAPI import (
    AuthenticationError,
    BigQuery,
    CloudStorage,
    ConfigurationError,
    GoogleCloudPlatformAPIError,
    ServiceError,
    TransportError,
)
```

Direct module imports such as `from GoogleCloudPlatformAPI.BigQuery import BigQuery` remain supported for backward compatibility.

## Naming

- Service helpers use singular PascalCase class names.
- Public methods use snake_case.
- Existing historical method names remain supported within the current major version.
- New aliases are additive and removals require a future major release with prior deprecation notice.

## Exceptions

All package-defined operational exceptions derive from `GoogleCloudPlatformAPIError`:

- `AuthenticationError`: missing, invalid, or rejected Google credentials.
- `ConfigurationError`: invalid local or service configuration.
- `TransportError`: networking, timeout, or request transport failures.
- `ServiceError`: a Google service rejected a valid request.

Each exception provides `message`, optional `operation`, optional JSON-compatible `details`, and `to_dict()` for structured handling. Existing helpers may still propagate documented upstream Google client exceptions where changing behavior would break compatibility; new public APIs should prefer the package hierarchy.

## Compatibility

Package-root exports, exception class names, and capability names are stable public contracts. Additions are backward compatible. Renames and removals require a major version.
