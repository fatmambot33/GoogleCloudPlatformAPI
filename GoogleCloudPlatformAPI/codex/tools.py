"""Safe, bounded tool adapters for the local Codex MCP server."""

import json
import os
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, Callable, Dict, List, Optional, cast

from GoogleCloudPlatformAPI.ai_native import (
    CapabilityError,
    CapabilityExecutionError,
    CapabilityTimeoutError,
    SchemaValidationError,
    capability_registry,
    decode_cursor,
    encode_cursor,
    mcp_tool_definitions,
    normalize_exception,
    run_with_timeout,
    validate_single_read_query,
)

_TOOL_ORDER = (
    "gcp_context",
    "bigquery_list_datasets",
    "bigquery_list_tables",
    "bigquery_table_schema",
    "bigquery_query",
    "gcs_list_objects",
    "gcs_object_metadata",
    "gcs_read_text",
)


def _json_value(value: Any) -> Any:
    """Convert common Google client values into JSON-compatible values."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return str(value)


def _row_to_dict(row: Any) -> Dict[str, Any]:
    """Convert a BigQuery row-like object into a JSON-compatible dictionary."""
    if hasattr(row, "items"):
        return {str(key): _json_value(value) for key, value in row.items()}
    return {"value": _json_value(row)}


def _optional_int(value: Any) -> Optional[int]:
    """Return an integer provider statistic or None."""
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _optional_bool(value: Any) -> Optional[bool]:
    """Return a boolean provider statistic or None."""
    return value if isinstance(value, bool) else None


def _optional_str(value: Any) -> Optional[str]:
    """Return a string provider value or None."""
    return value if isinstance(value, str) and value else None


def _bounded(value: int, name: str, maximum: int) -> None:
    """Validate a positive bounded integer argument."""
    if value < 1 or value > maximum:
        raise ValueError("{0} must be between 1 and {1}.".format(name, maximum))


class CodexTools:
    """Bounded adapters around the package's existing GCP helpers.

    Parameters
    ----------
    bigquery_factory : callable, optional
        Factory returning a configured ``BigQuery`` helper.
    storage_factory : callable, optional
        Factory returning a configured ``CloudStorage`` helper.
    """

    def __init__(
        self,
        bigquery_factory: Optional[Callable[[], Any]] = None,
        storage_factory: Optional[Callable[[], Any]] = None,
    ) -> None:
        self._bigquery_factory = bigquery_factory
        self._storage_factory = storage_factory

    def _bigquery(self) -> Any:
        if self._bigquery_factory is not None:
            return self._bigquery_factory()
        from GoogleCloudPlatformAPI.BigQuery import BigQuery

        return BigQuery()

    def _storage(self) -> Any:
        if self._storage_factory is not None:
            return self._storage_factory()
        from GoogleCloudPlatformAPI.CloudStorage import CloudStorage

        return CloudStorage()

    def context(self) -> Dict[str, Any]:
        """Return local Google Cloud configuration without exposing secrets."""
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        return {
            "project_id": os.environ.get("GOOGLE_CLOUD_PROJECT")
            or os.environ.get("GCLOUD_PROJECT"),
            "credentials_configured": bool(credentials_path),
            "credentials_file": (
                os.path.basename(credentials_path) if credentials_path else None
            ),
            "write_tools_enabled": False,
        }

    def bigquery_list_datasets(
        self, max_results: int = 100, cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """List one bounded page of datasets visible to BigQuery."""
        _bounded(max_results, "max_results", 1000)
        page_token = decode_cursor(
            cursor, "bigquery", "list_datasets", {"project": "active"}
        )
        arguments: Dict[str, Any] = {"max_results": max_results}
        if page_token:
            arguments["page_token"] = page_token
        iterator = self._bigquery()._client.list_datasets(**arguments)
        datasets = list(iterator)
        limited = datasets[:max_results]
        provider_cursor = _optional_str(getattr(iterator, "next_page_token", None))
        next_cursor = encode_cursor(
            "bigquery", "list_datasets", provider_cursor, {"project": "active"}
        )
        return {
            "datasets": [
                {
                    "dataset_id": item.dataset_id,
                    "project": getattr(item, "project", None),
                    "full_id": getattr(item, "full_dataset_id", None),
                }
                for item in limited
            ],
            "returned_datasets": len(limited),
            "truncated": bool(next_cursor) or len(datasets) > max_results,
            "next_cursor": next_cursor,
        }

    def bigquery_list_tables(
        self,
        dataset_id: str,
        max_results: int = 100,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List one bounded page of tables and views in a dataset."""
        _bounded(max_results, "max_results", 1000)
        context = {"dataset_id": dataset_id}
        page_token = decode_cursor(cursor, "bigquery", "list_tables", context)
        arguments: Dict[str, Any] = {"max_results": max_results}
        if page_token:
            arguments["page_token"] = page_token
        iterator = self._bigquery()._client.list_tables(dataset_id, **arguments)
        tables = list(iterator)
        limited = tables[:max_results]
        provider_cursor = _optional_str(getattr(iterator, "next_page_token", None))
        next_cursor = encode_cursor(
            "bigquery", "list_tables", provider_cursor, context
        )
        return {
            "dataset_id": dataset_id,
            "tables": [
                {
                    "table_id": item.table_id,
                    "table_type": getattr(item, "table_type", None),
                    "full_id": getattr(item, "full_table_id", None),
                }
                for item in limited
            ],
            "returned_tables": len(limited),
            "truncated": bool(next_cursor) or len(tables) > max_results,
            "next_cursor": next_cursor,
        }

    def bigquery_table_schema(self, table_id: str) -> Dict[str, Any]:
        """Describe a BigQuery table and its schema."""
        table = self._bigquery()._client.get_table(table_id)
        return {
            "table_id": getattr(table, "full_table_id", table_id),
            "table_type": getattr(table, "table_type", None),
            "description": getattr(table, "description", None),
            "num_rows": getattr(table, "num_rows", None),
            "num_bytes": getattr(table, "num_bytes", None),
            "partitioning": _json_value(getattr(table, "time_partitioning", None)),
            "fields": [
                {
                    "name": field.name,
                    "type": field.field_type,
                    "mode": field.mode,
                    "description": field.description,
                }
                for field in table.schema
            ],
        }

    def bigquery_query(
        self,
        query: str,
        max_rows: int = 100,
        maximum_bytes_billed: int = 1000000000,
        timeout_seconds: int = 60,
    ) -> Dict[str, Any]:
        """Dry-run, cost-bound, and execute one read-only BigQuery statement."""
        from google.cloud import bigquery

        safe_query = validate_single_read_query(query)
        _bounded(max_rows, "max_rows", 1000)
        _bounded(maximum_bytes_billed, "maximum_bytes_billed", 1000000000000)
        _bounded(timeout_seconds, "timeout_seconds", 300)
        client = self._bigquery()._client

        dry_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dry_job = client.query(
            safe_query, job_config=dry_config, timeout=timeout_seconds
        )
        dry_bytes = _optional_int(getattr(dry_job, "total_bytes_processed", None)) or 0
        statement_type = (
            _optional_str(getattr(dry_job, "statement_type", None)) or ""
        ).upper()
        if statement_type != "SELECT":
            raise ValueError(
                "BigQuery dry run classified this as {0}, not SELECT.".format(
                    statement_type or "unknown"
                )
            )
        if dry_bytes > maximum_bytes_billed:
            raise CapabilityExecutionError(
                CapabilityError(
                    code="billing_limit_exceeded",
                    message="BigQuery dry-run estimate exceeds maximum_bytes_billed.",
                    retryable=False,
                    guidance="Add filters or explicitly raise the bounded billing limit.",
                    details={
                        "estimated_bytes": dry_bytes,
                        "maximum_bytes_billed": maximum_bytes_billed,
                    },
                )
            )

        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=maximum_bytes_billed,
            job_timeout_ms=timeout_seconds * 1000,
            use_query_cache=True,
        )
        job = client.query(
            safe_query, job_config=job_config, timeout=timeout_seconds
        )
        try:
            iterator = job.result(
                timeout=timeout_seconds, max_results=max_rows + 1
            )
            rows = list(iterator)
        except FutureTimeoutError as exc:
            try:
                job.cancel(timeout=min(float(timeout_seconds), 10.0))
            finally:
                raise CapabilityTimeoutError(
                    "BigQuery query exceeded its bounded timeout and cancellation was requested."
                ) from exc

        limited = rows[:max_rows]
        total_rows = _optional_int(getattr(iterator, "total_rows", None))
        return {
            "rows": [_row_to_dict(row) for row in limited],
            "returned_rows": len(limited),
            "truncated": len(rows) > max_rows
            or (total_rows is not None and total_rows > max_rows),
            "dry_run_bytes_processed": dry_bytes,
            "total_bytes_processed": _optional_int(
                getattr(job, "total_bytes_processed", None)
            ),
            "total_bytes_billed": _optional_int(
                getattr(job, "total_bytes_billed", None)
            ),
            "maximum_bytes_billed": maximum_bytes_billed,
            "cache_hit": _optional_bool(getattr(job, "cache_hit", None)),
            "job_id": _optional_str(getattr(job, "job_id", None)),
            "statement_type": statement_type,
        }

    def storage_list(
        self,
        bucket_name: str,
        prefix: str = "",
        max_results: int = 100,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List one bounded page of Cloud Storage object names."""
        _bounded(max_results, "max_results", 1000)
        context = {"bucket_name": bucket_name, "prefix": prefix}
        page_token = decode_cursor(cursor, "cloud_storage", "list_objects", context)
        arguments: Dict[str, Any] = {
            "prefix": prefix,
            "max_results": max_results,
        }
        if page_token:
            arguments["page_token"] = page_token
        iterator = self._storage()._client.list_blobs(bucket_name, **arguments)
        blobs = list(iterator)
        limited = blobs[:max_results]
        provider_cursor = _optional_str(getattr(iterator, "next_page_token", None))
        next_cursor = encode_cursor(
            "cloud_storage", "list_objects", provider_cursor, context
        )
        return {
            "objects": [str(blob.name) for blob in limited],
            "returned_objects": len(limited),
            "truncated": bool(next_cursor) or len(blobs) > max_results,
            "next_cursor": next_cursor,
        }

    def storage_object_metadata(
        self, bucket_name: str, object_name: str
    ) -> Dict[str, Any]:
        """Return metadata for one Cloud Storage object."""
        blob = self._storage()._client.bucket(bucket_name).get_blob(object_name)
        if blob is None:
            raise ValueError("Object not found: {0}".format(object_name))
        return {
            "bucket_name": bucket_name,
            "object_name": object_name,
            "size": getattr(blob, "size", None),
            "content_type": getattr(blob, "content_type", None),
            "generation": getattr(blob, "generation", None),
            "updated": _json_value(getattr(blob, "updated", None)),
            "md5_hash": getattr(blob, "md5_hash", None),
        }

    def storage_read_text(
        self,
        bucket_name: str,
        object_name: str,
        max_bytes: int = 100000,
        timeout_seconds: int = 30,
    ) -> Dict[str, Any]:
        """Read at most ``max_bytes`` without downloading the complete object."""
        _bounded(max_bytes, "max_bytes", 1000000)
        _bounded(timeout_seconds, "timeout_seconds", 300)
        blob = self._storage()._client.bucket(bucket_name).blob(object_name)
        data = blob.download_as_bytes(
            start=0,
            end=max_bytes,
            timeout=timeout_seconds,
            checksum=None,
        )
        limited = data[:max_bytes]
        return {
            "text": limited.decode("utf-8", errors="replace"),
            "bytes_returned": len(limited),
            "truncated": len(data) > max_bytes,
        }

    def call(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Validate, execute, and normalize one registered MCP tool call."""
        try:
            capability = capability_registry.get(name)
        except KeyError as exc:
            raise ValueError("Unknown tool: {0}".format(name)) from exc
        if not capability.adapter_method:
            raise ValueError("Tool has no local adapter: {0}".format(name))
        handler = getattr(self, capability.adapter_method, None)
        if not callable(handler):
            raise ValueError("Tool adapter is unavailable: {0}".format(name))
        typed_handler = cast(Callable[..., Dict[str, Any]], handler)
        try:
            capability_registry.validate_input(name, arguments)
            payload = run_with_timeout(
                typed_handler, arguments, capability.timeout_seconds
            )
            capability_registry.validate_output(name, payload)
            return payload
        except SchemaValidationError:
            raise
        except Exception as exc:
            raise CapabilityExecutionError(normalize_exception(exc)) from exc


def tool_definitions() -> List[Dict[str, Any]]:
    """Generate MCP tool definitions in discovery-first workflow order."""
    return mcp_tool_definitions(capability_registry, _TOOL_ORDER)


def text_content(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    """Encode a structured payload as MCP text content."""
    return [{"type": "text", "text": json.dumps(payload, indent=2, sort_keys=True)}]
