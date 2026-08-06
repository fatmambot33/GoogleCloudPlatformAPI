---
name: google-cloud-platform-api
description: Explore BigQuery and Cloud Storage safely through a focused read-only MCP surface.
---

# Google Cloud Platform API

Use the local `google_cloud_platform_api` MCP server for Google Cloud inspection and analysis.

## Operating contract

- Start with `gcp_context` to confirm the active project and defaults.
- Treat the entire surface as read-only. Never claim that a mutation was performed.
- Discover resources before querying them; do not guess dataset, table, bucket, or object names.
- Keep every response bounded with `max_rows`, `max_results`, or `max_bytes`.
- Prefer metadata inspection before downloading object content.
- Explain the query and expected cost shape before running broad BigQuery scans.

## BigQuery workflow

1. `bigquery_list_datasets`
2. `bigquery_list_tables`
3. `bigquery_table_schema`
4. `bigquery_query`

Use fully qualified table names in generated SQL. Select only needed columns, include restrictive filters, and add a `LIMIT` whenever practical.

## Cloud Storage workflow

1. `gcs_list_objects`
2. `gcs_object_metadata`
3. `gcs_read_text`

Read only objects that are plausibly textual. Start with a small `max_bytes` value and increase it only when necessary.

## Example prompts

- Show my active GCP context, then list available BigQuery datasets.
- Inspect the schema of `project.dataset.table` and draft a safe analysis query.
- List tables in a dataset and identify likely event tables.
- List JSON objects under `reports/`, inspect the newest object's metadata, and read its first 50 KB.
- Explain why a requested operation is unavailable when it requires a cloud write.
