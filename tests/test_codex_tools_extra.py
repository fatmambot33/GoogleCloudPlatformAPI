"""Additional deterministic tests for Codex tool adapters."""

import datetime
from unittest.mock import MagicMock

import pytest

from GoogleCloudPlatformAPI.codex.tools import (
    CodexTools,
    _json_value,
    tool_definitions,
)


def test_json_value_handles_dates_tuples_and_fallback_values():
    value = object()

    assert _json_value(datetime.date(2026, 8, 4)) == "2026-08-04"
    assert _json_value((1, b"two")) == [1, "two"]
    assert _json_value(value) == str(value)


def test_bigquery_query_limits_rows_and_rejects_unsafe_inputs():
    bigquery = MagicMock()
    bigquery.execute_query.return_value = [
        {"value": 1},
        {"value": 2},
        {"value": 3},
    ]
    tools = CodexTools(bigquery_factory=lambda: bigquery)

    result = tools.bigquery_query("SELECT value FROM table", max_rows=2)

    assert result == {
        "rows": [{"value": 1}, {"value": 2}],
        "returned_rows": 2,
        "truncated": True,
    }
    with pytest.raises(ValueError, match="Only SELECT"):
        tools.bigquery_query("DELETE FROM table")
    with pytest.raises(ValueError, match="between 1 and 1000"):
        tools.bigquery_query("SELECT 1", max_rows=0)


def test_storage_adapters_list_and_read_bounded_data():
    storage = MagicMock()
    storage.list_files.return_value = ["a", "b", "c"]
    blob = storage._client.bucket.return_value.blob.return_value
    blob.download_as_bytes.return_value = b"abcdef"
    tools = CodexTools(storage_factory=lambda: storage)

    listed = tools.storage_list("bucket", prefix="events/", max_results=2)
    read = tools.storage_read_text("bucket", "events/a.txt", max_bytes=3)

    assert listed == {
        "objects": ["a", "b"],
        "returned_objects": 2,
        "truncated": True,
    }
    assert read == {"text": "abc", "bytes_returned": 3, "truncated": True}
    with pytest.raises(ValueError, match="between 1 and 1000"):
        tools.storage_list("bucket", max_results=1001)
    with pytest.raises(ValueError, match="between 1 and 1000000"):
        tools.storage_read_text("bucket", "object", max_bytes=0)


def test_call_dispatches_registry_adapters_and_validates_results(monkeypatch):
    tools = CodexTools()
    monkeypatch.setattr(
        tools,
        "context",
        lambda: {
            "project_id": None,
            "credentials_configured": False,
            "credentials_file": None,
            "write_tools_enabled": False,
        },
    )
    monkeypatch.setattr(
        tools,
        "bigquery_query",
        lambda **kwargs: {"rows": [], "returned_rows": 0, "truncated": False},
    )
    monkeypatch.setattr(
        tools,
        "storage_list",
        lambda **kwargs: {"objects": [], "returned_objects": 0, "truncated": False},
    )
    monkeypatch.setattr(
        tools,
        "storage_read_text",
        lambda **kwargs: {"text": "", "bytes_returned": 0, "truncated": False},
    )

    assert tools.call("gcp_context", {})["write_tools_enabled"] is False
    assert tools.call("bigquery_query", {"query": "SELECT 1"})["rows"] == []
    assert tools.call("gcs_list_objects", {"bucket_name": "bucket"})["objects"] == []
    assert (
        tools.call("gcs_read_text", {"bucket_name": "bucket", "object_name": "object"})[
            "text"
        ]
        == ""
    )
    with pytest.raises(ValueError, match="Unknown tool"):
        tools.call("missing", {})
    with pytest.raises(ValueError, match="unexpected property"):
        tools.call("gcp_context", {"extra": True})


def test_tool_definitions_include_generated_output_contracts():
    definitions = tool_definitions()

    assert [definition["name"] for definition in definitions] == [
        "gcp_context",
        "bigquery_list_datasets",
        "bigquery_list_tables",
        "bigquery_table_schema",
        "bigquery_query",
        "gcs_list_objects",
        "gcs_object_metadata",
        "gcs_read_text",
    ]
    assert all("outputSchema" in definition for definition in definitions)
    assert all(definition["annotations"]["readOnlyHint"] for definition in definitions)
