# Authentication

GoogleCloudPlatformAPI uses Google service-account credentials.

## Resolution order

1. An explicit credential file supplied to a helper.
2. `GOOGLE_APPLICATION_CREDENTIALS`.
3. Google Application Default Credentials when supported by the underlying client.

Explicit configuration always wins. Helpers must not mutate credential environment variables globally.

## Failure behavior

Missing, unreadable, or invalid credentials must raise an actionable exception. Error messages may include the credential source type, but must never include private keys, access tokens, refresh tokens, or full credential payloads.

## Local development

Set `GOOGLE_APPLICATION_CREDENTIALS` to a service-account JSON file outside the repository. Never commit credentials.

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/.config/gcloud/service-account.json"
```

## Production

Prefer workload identity or platform-provided Application Default Credentials. Use a file only when the deployment environment requires it.
