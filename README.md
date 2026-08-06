# GoogleCloudPlatformAPI

Lightweight helpers for common Google Cloud Platform services. The package wraps
Google APIs such as BigQuery, Cloud Storage, Analytics, and Ad Manager to reduce
boilerplate when building data pipelines or analytics tools.

## Installation

```bash
pip install GoogleCloudPlatformAPI
```

Python 3.10 through 3.14 are supported. Runtime dependencies use bounded
compatibility ranges, and CI tests both the minimum Python 3.10 dependency set
and the newest compatible dependencies.

For development, install the project in editable mode with the tooling extras:

```bash
pip install -e '.[dev]'
```

Run the project checks locally to match CI:

```bash
black --check .
pydocstyle GoogleCloudPlatformAPI
pyright GoogleCloudPlatformAPI
pytest -q --cov=GoogleCloudPlatformAPI --cov-report=term-missing --cov-fail-under=90
python -m build
python -m twine check dist/*
```

Configure authentication with a service account JSON file through
`GOOGLE_APPLICATION_CREDENTIALS` or supply the path to individual helpers.

## Stable public API

Core services and package-defined exceptions are available from the package root:

```python
from GoogleCloudPlatformAPI import (
    AuthenticationError,
    BigQuery,
    CloudStorage,
    GoogleCloudPlatformAPIError,
)
```

Direct module imports remain supported. See `docs/public-api.md` for naming,
exception, and compatibility rules.

## AI-native platform surface

The package uses one canonical capability registry for Python, agent, and
MCP/Codex integrations. Each operation has a stable name, semantic version,
JSON-compatible input and output schemas, permission metadata, safety level,
and bounded timeout.

```python
from GoogleCloudPlatformAPI.ai_native import capability_registry, readiness_score

print(capability_registry.schema())
print(readiness_score(capability_registry))
```

The wheel includes the machine-readable documentation index, Codex skill, and
AI platform documentation:

```python
from GoogleCloudPlatformAPI.assets import read_text_resource

print(read_text_resource("llms.txt"))
print(read_text_resource("codex/SKILL.md"))
```

See `docs/ai-native-platform.md`, `docs/ai-native-scorecard.md`, and `llms.txt`
for the repository copies of these contracts.

## Local Codex plugin surface

The package includes a read-only MCP server for Codex CLI, the Codex desktop
app, and compatible local MCP clients. It runs locally over stdio, inherits the
current process environment, and never copies or persists Google credentials.

Install the project and verify the entry point:

```bash
pip install 'GoogleCloudPlatformAPI[codex]'
gcp-api-mcp
```

Register the server in your Codex MCP configuration:

```toml
[mcp_servers.google_cloud_platform_api]
command = "gcp-api-mcp"
```

When using a virtual environment, configure the absolute path to its
`gcp-api-mcp` executable. The server exposes eight read-only tools:

- `gcp_context`
- `bigquery_list_datasets`
- `bigquery_list_tables`
- `bigquery_table_schema`
- `bigquery_query`
- `gcs_list_objects`
- `gcs_object_metadata`
- `gcs_read_text`

The intended workflow is discovery first:

```text
BigQuery:      context -> datasets -> tables -> schema -> bounded query
Cloud Storage: context -> objects -> metadata -> bounded text read
```

BigQuery accepts only statements beginning with `SELECT`, `WITH`, or `EXPLAIN`.
Cloud Storage reads and all result sets are bounded. No upload, delete, table
creation, or other mutation tool is exposed.

Example Codex prompts:

```text
Show my active GCP context, then list available BigQuery datasets.
Inspect project.dataset.table and draft a safe bounded query.
List JSON objects under reports/ and inspect the newest object's metadata.
Read reports/latest.json, limited to 50000 bytes.
```

The reusable workflow guidance is stored in
`.codex/skills/google-cloud-platform-api/SKILL.md` and in the installed wheel as
`GoogleCloudPlatformAPI/assets/codex/SKILL.md`.

## Usage

### BigQuery

```python
from GoogleCloudPlatformAPI import BigQuery

bq = BigQuery()
df = bq.bigquery_to_dataframe("SELECT CURRENT_DATE() AS today")
print(df)
```

### Cloud Storage

```python
from GoogleCloudPlatformAPI import CloudStorage

storage = CloudStorage()
storage.upload_file_from_filename(
    local_file_path="local.txt",
    destination_file_path="data/local.txt",
    bucket_name="my-bucket",
)
```

### Ad Manager

```python
from GoogleCloudPlatformAPI.AdManager import GamClient

# Assumes GOOGLE_APPLICATION_CREDENTIALS is set
gam_client = GamClient()
network_service = gam_client.get_service(
    service_name="NetworkService",
    gam_version="v202602",
)
print(network_service.getCurrentNetwork())
```

### Analytics

```python
from GoogleCloudPlatformAPI import Analytics

# Assumes GOOGLE_APPLICATION_CREDENTIALS is set
analytics = Analytics()
profile_id = "12345678"
report = analytics.get_realtime_report(profile_id)
print(report)
```

### OAuth

```python
from GoogleCloudPlatformAPI import ServiceAccount

# Assumes GOOGLE_APPLICATION_CREDENTIALS is set
creds = ServiceAccount.get_service_account_client()
print(creds.project_id)
```
