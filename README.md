# GoogleCloudPlatformAPI

Lightweight helpers for common Google Cloud Platform services. The package wraps
Google APIs such as BigQuery, Cloud Storage, Analytics, and Ad Manager to reduce
boilerplate when building data pipelines or analytics tools.

## Installation

```bash
pip install GoogleCloudPlatformAPI
```

For development, install the project in editable mode with the tooling extras:

```bash
pip install -e .[dev]
```

Run the project checks locally to match the CI configuration:

```bash
black --check .
pydocstyle GoogleCloudPlatformAPI
pyright GoogleCloudPlatformAPI
pytest -q --cov=GoogleCloudPlatformAPI --cov-report=term-missing --cov-fail-under=70
```

Configure authentication with a service account JSON file via the
`GOOGLE_APPLICATION_CREDENTIALS` environment variable or supply the path to
individual helpers.

## Local Codex plugin surface

The package includes a read-only MCP server for Codex CLI, the Codex desktop
app, and compatible local MCP clients. It runs locally over stdio, inherits the
current process environment, and never copies or persists Google credentials.

Install the project and verify the entry point:

```bash
pip install -e '.[codex]'
gcp-api-mcp
```

Register the server in your Codex MCP configuration:

```toml
[mcp_servers.google_cloud_platform_api]
command = "gcp-api-mcp"
```

When using a virtual environment, configure the absolute path to its
`gcp-api-mcp` executable. The server exposes four read-only tools:

- `gcp_context`
- `bigquery_query`
- `gcs_list_objects`
- `gcs_read_text`

BigQuery accepts only statements beginning with `SELECT`, `WITH`, or `EXPLAIN`.
Cloud Storage reads and all result sets are bounded. No upload, delete, table
creation, or other mutation tool is exposed.

Example Codex prompts:

```text
Inspect my local GCP configuration.
Run SELECT CURRENT_DATE() AS today in BigQuery.
List objects under reports/ in bucket example-bucket.
Read reports/latest.json from bucket example-bucket, limited to 50000 bytes.
```

The reusable workflow guidance is stored in
`.codex/skills/google-cloud-platform-api/SKILL.md`.

## Usage

### BigQuery

```python
from GoogleCloudPlatformAPI.BigQuery import BigQuery

bq = BigQuery()
df = bq.bigquery_to_dataframe("SELECT CURRENT_DATE() AS today")
print(df)
```

### Cloud Storage

```python
from GoogleCloudPlatformAPI.CloudStorage import CloudStorage

storage = CloudStorage()
storage.upload_file_from_filename(
    local_file_path="local.txt",
    destination_file_path="data/local.txt",
    bucket_name="my-bucket",
)
```

### AdManager

```python
from GoogleCloudPlatformAPI.AdManager import GamClient

# Assumes GOOGLE_APPLICATION_CREDENTIALS is set
gam_client = GamClient()
network_service = gam_client.get_service(
    service_name="NetworkService",
    gam_version="v202602"
)
print(network_service.getCurrentNetwork())
```

### Analytics

```python
from GoogleCloudPlatformAPI.Analytics import Analytics

# Assumes GOOGLE_APPLICATION_CREDENTIALS is set
analytics = Analytics()
profile_id = "12345678"  # Replace with your Profile ID
report = analytics.get_realtime_report(profile_id)
print(report)
```

### Oauth

```python
from GoogleCloudPlatformAPI.Oauth import ServiceAccount

# Assumes GOOGLE_APPLICATION_CREDENTIALS is set
creds = ServiceAccount.get_service_account_client()
print(creds.project_id)
```
