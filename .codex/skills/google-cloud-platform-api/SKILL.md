---
name: google-cloud-platform-api
description: Explore BigQuery and Cloud Storage through bounded inspection and cost-controlled read tools.
---

# Google Cloud Platform API

Use the local `google_cloud_platform_api` MCP server for Google Cloud inspection
and analysis.

## Operating contract

- Start with `gcp_context` to confirm the active project and defaults.
- No mutation tool is available; never claim that a cloud write was performed.
- Discover resources before querying them; do not guess identifiers.
- Follow `next_cursor` only when more discovery results are needed.
- Never reuse a cursor with a different dataset, bucket, or prefix.
- Prefer metadata inspection before reading object content.
- Treat `bigquery_query` as billable even though it cannot mutate data.

## BigQuery workflow

1. `bigquery_list_datasets`
2. `bigquery_list_tables`
3. `bigquery_table_schema`
4. draft a single selective query
5. `bigquery_query` with an explicit acceptable `maximum_bytes_billed`

The query tool dry-runs first and rejects estimates above the billing ceiling.
Use fully qualified table names, select only needed columns, include restrictive
filters, and add a `LIMIT` whenever practical. A successful result reports
estimated, processed, and billed bytes plus job metadata.

## Cloud Storage workflow

1. `gcs_list_objects`
2. `gcs_object_metadata`
3. `gcs_read_text`

Read only plausibly textual objects. Start with a small `max_bytes` value. The
server uses a provider byte range and reports whether more content exists.

## Errors

Use the returned machine-readable code and guidance. Retry only when
`retryable` is true. Permission failures should be resolved with the capability's
documented IAM permissions; quota and availability failures should use bounded
exponential backoff.
