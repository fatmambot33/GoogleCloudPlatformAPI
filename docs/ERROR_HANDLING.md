# Error Handling

## Principles

- Fail explicitly.
- Preserve the original exception as the cause.
- Include the operation and affected resource when known.
- Never expose credentials or sensitive payloads.
- Do not return `None` to hide an unexpected API failure.

## Expected absence

Methods that search or list resources may return an empty collection when no matching resource exists. Methods documented as optional lookups may return `None`.

## External failures

Authentication, permission, quota, transport, and Google API errors propagate unless additional package context materially improves the message. Wrapped errors must use exception chaining.

```python
try:
    ...
except ExternalError as exc:
    raise RuntimeError("Unable to load BigQuery table metadata") from exc
```

## Logging

Logs may contain service names, operation names, resource identifiers, and status information. They must not contain tokens, private keys, complete credential documents, or unredacted request payloads.
