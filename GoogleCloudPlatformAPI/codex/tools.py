"""Safe, bounded Codex adapters backed by the public service helpers."""

from typing import Any, Dict, Optional

from GoogleCloudPlatformAPI.ai_native import decode_cursor, encode_cursor

from ._tools_core import (
    CodexTools as _CodexToolsCore,
    _bounded,
    _json_value,
    _optional_str,
    text_content,
    tool_definitions,
)


class CodexTools(_CodexToolsCore):
    """Use public resource APIs for Codex discovery and inspection reads."""

    def bigquery_list_datasets(
        self, max_results: int = 100, cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """List one bounded page of datasets visible to BigQuery."""
        _bounded(max_results, "max_results", 1000)
        page_token = decode_cursor(
            cursor, "bigquery", "list_datasets", {"project": "active"}
        )
        helper = self._bigquery()
        if hasattr(helper, "list_datasets"):
            iterator = helper.list_datasets(
                max_results=max_results,
                page_token=page_token or None,
            )
        else:
            arguments: Dict[str, Any] = {"max_results": max_results}
            if page_token:
                arguments["page_token"] = page_token
            iterator = helper._client.list_datasets(**arguments)
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
        helper = self._bigquery()
        if hasattr(helper, "list_tables"):
            iterator = helper.list_tables(
                dataset_id,
                max_results=max_results,
                page_token=page_token or None,
            )
        else:
            arguments: Dict[str, Any] = {"max_results": max_results}
            if page_token:
                arguments["page_token"] = page_token
            iterator = helper._client.list_tables(dataset_id, **arguments)
        tables = list(iterator)
        limited = tables[:max_results]
        provider_cursor = _optional_str(getattr(iterator, "next_page_token", None))
        next_cursor = encode_cursor("bigquery", "list_tables", provider_cursor, context)
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
        helper = self._bigquery()
        if hasattr(helper, "get_table"):
            table = helper.get_table(table_id)
        else:
            table = helper._client.get_table(table_id)
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
        helper = self._storage()
        if hasattr(helper, "list_objects"):
            iterator = helper.list_objects(
                bucket_name,
                prefix=prefix,
                max_results=max_results,
                page_token=page_token or None,
            )
        else:
            arguments: Dict[str, Any] = {
                "prefix": prefix,
                "max_results": max_results,
            }
            if page_token:
                arguments["page_token"] = page_token
            iterator = helper._client.list_blobs(bucket_name, **arguments)
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
        helper = self._storage()
        if hasattr(helper, "get_object_metadata"):
            try:
                return helper.get_object_metadata(bucket_name, object_name)
            except FileNotFoundError as exc:
                raise ValueError(f"Object not found: {object_name}") from exc
        blob = helper._client.bucket(bucket_name).get_blob(object_name)
        if blob is None:
            raise ValueError(f"Object not found: {object_name}")
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
        helper = self._storage()
        if hasattr(helper, "get_object"):
            blob = helper.get_object(bucket_name, object_name)
            if blob is None:
                raise ValueError(f"Object not found: {object_name}")
        else:
            blob = helper._client.bucket(bucket_name).blob(object_name)
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


__all__ = ["CodexTools", "text_content", "tool_definitions"]
