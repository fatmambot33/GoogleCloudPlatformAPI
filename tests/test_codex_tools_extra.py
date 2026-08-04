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


def test_call_dispatches_all_tools_and_rejects_unknown_name(monkeypatch):
    tools = CodexTools()
    monkeypatch.setattr(tools, "context", lambda: {"context": True})
    monkeypatch.setattr(tools, "bigquery_query", lambda **kwargs: kwargs)
    monkeypatch.setattr(tools, "storage_list", lambda **kwargs: kwargs)
    monkeypatch.setattr(tools, "storage_read_text", lambda **kwargs: kwargs)

    assert tools.call("gcp_context", {}) == {"context": True}
    assert tools.call("bigquery_query", {"query": "SELECT 1"}) == {
        "query": "SELECT 1"
    }
    assert tools.call("gcs_list_objects", {"bucket_name": "bucket"}) == {
        "bucket_name": "bucket"
    }
    assert tools.call(
        "gcs_read_text", {"bucket_name": "bucket", "object_name": "object"}
    ) == {"bucket_name": "bucket", "object_name": "object"}
    with pytest.raises(ValueError, match="Unknown tool"):
        tools.call("missing", {})


def test_tool_definitions_include_all_read_only_tools():
    definitions = tool_definitions()

    assert [definition["name"] for definition in definitions] == [
        "gcp_context",
        "bigquery_query",
        "gcs_list_objects",
        "gcs_read_text",
    ]
