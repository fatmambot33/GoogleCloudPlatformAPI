# GoogleCloudPlatformAPI

Lightweight helpers for common Google Cloud Platform services. The package wraps
Google APIs such as BigQuery, Cloud Storage, Analytics, and Ad Manager to reduce
boilerplate when building data pipelines or analytics tools.

## Installation

```bash
pip install GoogleCloudPlatformAPI
```

Configure authentication with a service account JSON file via the
`GOOGLE_APPLICATION_CREDENTIALS` environment variable or supply the path to
individual helpers.

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
