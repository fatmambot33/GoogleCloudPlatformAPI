# Public API Consistency

GoogleCloudPlatformAPI exposes service-oriented helpers for BigQuery, Cloud Storage, Analytics, Ad Manager, and authentication.

## Conventions

- Public classes use service names.
- Public methods use `snake_case`.
- Required identifiers are explicit arguments.
- Optional configuration uses keyword arguments with documented defaults.
- Methods return the underlying Google object, a pandas object, or `None`; they must not silently switch between unrelated return shapes.
- Collection methods return empty collections when no result exists.
- API failures raise the original Google client exception unless a package exception adds actionable context without hiding the cause.
- Credentials are resolved through the authentication policy in `AUTHENTICATION.md`.

## Compatibility

Existing public names remain supported throughout the 2.x series. Normalization must use additive aliases or deprecation warnings before removal in a future major release.

## Review checklist

Every public API change must confirm naming, parameter order, return type, exception behavior, typing, documentation, tests, and backward compatibility.
