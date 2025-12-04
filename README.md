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

### AdManager

```python
from GoogleCloudPlatformAPI.AdManager import GamClient

# Assumes GOOGLE_APPLICATION_CREDENTIALS is set
gam_client = GamClient()
network_service = gam_client.get_service(
    service_name="NetworkService",
    gam_version="v202505"
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
