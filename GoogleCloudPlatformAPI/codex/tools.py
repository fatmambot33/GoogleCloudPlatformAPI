"""Safe, read-only tool adapters for the local Codex MCP server."""

import json
import os
import re
from typing import Any, Callable, Dict, List, Optional

from GoogleCloudPlatformAPI.ai_native import capability_registry

_READ_ONLY_SQL = re.compile(r"^\s*(select|with|explain)\b", re.IGNORECASE)


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


def _bounded(value: int, name: str, maximum: int) -> None:
    """Validate a positive bounded integer argument."""
    if value < 1 or value > maximum:
        raise ValueError("{0} must be between 1 and {1}.".format(name, maximum))


class CodexTools:
    """Read-only adapters around the package's existing GCP helpers.

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
            "default_bigquery_dataset": os.environ.get("DEFAULT_BQ_DATASET"),
            "default_storage_bucket": os.environ.get("DEFAULT_GCS_BUCKET"),
            "credentials_configured": bool(credentials_path),
            "credentials_file": (
                os.path.basename(credentials_path) if credentials_path else None
            ),
            "access": "read-only",
            "write_tools_enabled": False,
        }

    def bigquery_list_datasets(self, max_results: int = 100) -> Dict[str, Any]:
        """List datasets visible to the configured BigQuery client."""
        _bounded(max_results, "max_results", 1000)
        datasets = list(
            self._bigquery()._client.list_datasets(max_results=max_results + 1)
        )
        limited = datasets[:max_results]
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
            "truncated": len(datasets) > max_results,
        }

    def bigquery_list_tables(
        self, dataset_id: str, max_results: int = 100
    ) -> Dict[str, Any]:
        """List tables and views in a BigQuery dataset."""
        _bounded(max_results, "max_results", 1000)
        tables = list(
            self._bigquery()._client.list_tables(
                dataset_id, max_results=max_results + 1
            )
        )
        limited = tables[:max_results]
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
            "truncated": len(tables) > max_results,
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

    def bigquery_query(self, query: str, max_rows: int = 100) -> Dict[str, Any]:
        """Execute a read-only BigQuery query and return structured rows."""
        if not isinstance(query, str) or not _READ_ONLY_SQL.match(query):
            raise ValueError("Only SELECT, WITH, and EXPLAIN queries are allowed.")
        _bounded(max_rows, "max_rows", 1000)
        rows = self._bigquery().execute_query(query)
        limited = rows[:max_rows]
        return {
            "rows": [_row_to_dict(row) for row in limited],
            "returned_rows": len(limited),
            "truncated": len(rows) > max_rows,
        }

    def storage_list(
        self, bucket_name: str, prefix: str = "", max_results: int = 100
    ) -> Dict[str, Any]:
        """List object names in a Cloud Storage bucket."""
        _bounded(max_results, "max_results", 1000)
        names = self._storage().list_files(bucket_name, prefix)
        return {
            "objects": names[:max_results],
            "returned_objects": min(len(names), max_results),
            "truncated": len(names) > max_results,
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
        self, bucket_name: str, object_name: str, max_bytes: int = 100000
    ) -> Dict[str, Any]:
        """Read a UTF-8 Cloud Storage object without writing it locally."""
        _bounded(max_bytes, "max_bytes", 1000000)
        blob = self._storage()._client.bucket(bucket_name).blob(object_name)
        data = blob.download_as_bytes()
        limited = data[:max_bytes]
        return {
            "text": limited.decode("utf-8", errors="replace"),
            "bytes_returned": len(limited),
            "truncated": len(data) > max_bytes,
        }

    def call(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch a named MCP tool call."""
        try:
            capability_registry.get(name)
        except KeyError:
            raise ValueError("Unknown tool: {0}".format(name))
        handlers = {
            "gcp_context": lambda: self.context(),
            "bigquery_list_datasets": lambda: self.bigquery_list_datasets(
                **arguments
            ),
            "bigquery_list_tables": lambda: self.bigquery_list_tables(**arguments),
            "bigquery_table_schema": lambda: self.bigquery_table_schema(**arguments),
            "bigquery_query": lambda: self.bigquery_query(**arguments),
            "gcs_list_objects": lambda: self.storage_list(**arguments),
            "gcs_object_metadata": lambda: self.storage_object_metadata(**arguments),
            "gcs_read_text": lambda: self.storage_read_text(**arguments),
        }
        handler = handlers.get(name)
        if handler is None:
            raise ValueError("Tool has no local handler: {0}".format(name))
        return handler()


def tool_definitions() -> List[Dict[str, Any]]:
    """Generate MCP tool definitions from the canonical capability registry."""
    return [
        {
            "name": capability.name,
            "description": capability.description,
            "inputSchema": capability.input_schema,
        }
        for capability in capability_registry.list()
    ]


def text_content(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    """Encode a structured payload as MCP text content."""
    return [{"type": "text", "text": json.dumps(payload, indent=2, sort_keys=True)}]
