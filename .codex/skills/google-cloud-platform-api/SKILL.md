---
name: google-cloud-platform-api
description: Inspect and query Google Cloud locally through the repository's read-only MCP tools.
---

# Google Cloud Platform API

Use the local `google_cloud_platform_api` MCP server for Google Cloud work.

## Workflow

1. Call `gcp_context` before cloud operations to confirm local configuration.
2. Use `bigquery_query` only for bounded read-only analysis.
3. Use `gcs_list_objects` before `gcs_read_text` when the object path is uncertain.
4. Keep responses small with `max_rows`, `max_results`, and `max_bytes`.
5. Never attempt writes through this surface. Ask the user to perform or explicitly enable a separate write workflow.

## Example prompts

- Inspect my local GCP configuration.
- Run `SELECT CURRENT_DATE() AS today` in BigQuery.
- List JSON objects under `reports/` in bucket `example-bucket`.
- Read the first 50 KB of `reports/latest.json`.
