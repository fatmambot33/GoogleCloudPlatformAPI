"""Read-only tool adapters for the local Codex MCP server."""

import json
import os
import re
from typing import Any, Callable, Dict, List, Optional

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
            "credentials_configured": bool(credentials_path),
            "credentials_file": (
                os.path.basename(credentials_path) if credentials_path else None
            ),
            "write_tools_enabled": False,
        }

    def bigquery_query(self, query: str, max_rows: int = 100) -> Dict[str, Any]:
        """Execute a read-only BigQuery query and return structured rows."""
        if not isinstance(query, str) or not _READ_ONLY_SQL.match(query):
            raise ValueError("Only SELECT, WITH, and EXPLAIN queries are allowed.")
        if max_rows < 1 or max_rows > 1000:
            raise ValueError("max_rows must be between 1 and 1000.")
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
        if max_results < 1 or max_results > 1000:
            raise ValueError("max_results must be between 1 and 1000.")
        names = self._storage().list_files(bucket_name, prefix)
        return {
            "objects": names[:max_results],
            "returned_objects": min(len(names), max_results),
            "truncated": len(names) > max_results,
        }

    def storage_read_text(
        self, bucket_name: str, object_name: str, max_bytes: int = 100000
    ) -> Dict[str, Any]:
        """Read a UTF-8 Cloud Storage object without writing it locally."""
        if max_bytes < 1 or max_bytes > 1000000:
            raise ValueError("max_bytes must be between 1 and 1000000.")
        storage = self._storage()
        blob = storage._client.bucket(bucket_name).blob(object_name)
        data = blob.download_as_bytes()
        limited = data[:max_bytes]
        return {
            "text": limited.decode("utf-8", errors="replace"),
            "bytes_returned": len(limited),
            "truncated": len(data) > max_bytes,
        }

    def call(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch a named MCP tool call."""
        handlers = {
            "gcp_context": lambda: self.context(),
            "bigquery_query": lambda: self.bigquery_query(**arguments),
            "gcs_list_objects": lambda: self.storage_list(**arguments),
            "gcs_read_text": lambda: self.storage_read_text(**arguments),
        }
        if name not in handlers:
            raise ValueError("Unknown tool: {0}".format(name))
        return handlers[name]()


def tool_definitions() -> List[Dict[str, Any]]:
    """Return MCP tool definitions exposed by the local server."""
    return [
        {
            "name": "gcp_context",
            "description": (
                "Inspect local GCP project and credential configuration without "
                "exposing secrets."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        },
        {
            "name": "bigquery_query",
            "description": "Run a read-only BigQuery SELECT, WITH, or EXPLAIN query.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "max_rows": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000,
                        "default": 100,
                    },
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        },
        {
            "name": "gcs_list_objects",
            "description": "List object names in a Cloud Storage bucket.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string"},
                    "prefix": {"type": "string", "default": ""},
                    "max_results": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000,
                        "default": 100,
                    },
                },
                "required": ["bucket_name"],
                "additionalProperties": False,
            },
        },
        {
            "name": "gcs_read_text",
            "description": (
                "Read a UTF-8 Cloud Storage object with a bounded response size."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "bucket_name": {"type": "string"},
                    "object_name": {"type": "string"},
                    "max_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000000,
                        "default": 100000,
                    },
                },
                "required": ["bucket_name", "object_name"],
                "additionalProperties": False,
            },
        },
    ]


def text_content(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    """Encode a structured payload as MCP text content."""
    return [{"type": "text", "text": json.dumps(payload, indent=2, sort_keys=True)}]
