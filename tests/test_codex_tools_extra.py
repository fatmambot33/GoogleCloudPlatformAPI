"""Additional deterministic tests for Codex tool adapters."""

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from GoogleCloudPlatformAPI.ai_native import CapabilityExecutionError
from GoogleCloudPlatformAPI.codex.tools import (
    CodexTools,
    _json_value,
    tool_definitions,
)


def _query_tools(rows=None, dry_bytes=20):
    """Build tools with deterministic dry-run and execution jobs."""
    dry_job = SimpleNamespace(
        total_bytes_processed=dry_bytes,
        statement_type="SELECT",
    )
    iterator = MagicMock()
    iterator.__iter__.return_value = iter(rows or [])
    iterator.total_rows = len(rows or [])
    query_job = MagicMock()
    query_job.result.return_value = iterator
    query_job.total_bytes_processed = dry_bytes
    query_job.total_bytes_billed = dry_bytes
    query_job.cache_hit = False
    query_job.job_id = "job-1"
    bigquery = MagicMock()
    bigquery._client.query.side_effect = [dry_job, query_job]
    return CodexTools(bigquery_factory=lambda: bigquery), bigquery, query_job


def test_json_value_handles_dates_tuples_and_fallback_values():
    value = object()

    assert _json_value(datetime.date(2026, 8, 4)) == "2026-08-04"
    assert _json_value((1, b"two")) == [1, "two"]
    assert _json_value(value) == str(value)


def test_bigquery_query_limits_rows_and_rejects_unsafe_inputs():
    tools, bigquery, _ = _query_tools([{"value": 1}, {"value": 2}, {"value": 3}])

    result = tools.bigquery_query("SELECT value FROM table", max_rows=2)

    assert result["rows"] == [{"value": 1}, {"value": 2}]
    assert result["returned_rows"] == 2
    assert result["truncated"] is True
    assert result["dry_run_bytes_processed"] == 20
    assert result["maximum_bytes_billed"] == 1000000000
    assert bigquery._client.query.call_count == 2
    with pytest.raises(ValueError, match="Only SELECT"):
        tools.bigquery_query("DELETE FROM table")
    with pytest.raises(ValueError, match="between 1 and 1000"):
        tools.bigquery_query("SELECT 1", max_rows=0)


def test_bigquery_query_rejects_estimates_over_billing_limit():
    tools, _, _ = _query_tools([], dry_bytes=101)

    with pytest.raises(CapabilityExecutionError) as raised:
        tools.bigquery_query("SELECT 1", maximum_bytes_billed=100)
    assert raised.value.error.code == "billing_limit_exceeded"
    assert raised.value.error.details["estimated_bytes"] == 101


def test_storage_adapters_list_and_read_bounded_data():
    storage = MagicMock()
    iterator = MagicMock()
    iterator.__iter__.return_value = iter(
        [SimpleNamespace(name="a"), SimpleNamespace(name="b")]
    )
    iterator.next_page_token = None
    storage._client.list_blobs.return_value = iterator
    blob = storage._client.bucket.return_value.blob.return_value
    blob.download_as_bytes.return_value = b"abcd"
    tools = CodexTools(storage_factory=lambda: storage)

    listed = tools.storage_list("bucket", prefix="events/", max_results=2)
    read = tools.storage_read_text("bucket", "events/a.txt", max_bytes=3)

    assert listed == {
        "objects": ["a", "b"],
        "returned_objects": 2,
        "truncated": False,
        "next_cursor": None,
    }
    assert read == {"text": "abc", "bytes_returned": 3, "truncated": True}
    blob.download_as_bytes.assert_called_once_with(
        start=0, end=3, timeout=30, checksum=None
    )
    with pytest.raises(ValueError, match="between 1 and 1000"):
        tools.storage_list("bucket", max_results=1001)
    with pytest.raises(ValueError, match="between 1 and 1000000"):
        tools.storage_read_text("bucket", "object", max_bytes=0)


def test_call_dispatches_registry_adapters_and_validates_results(monkeypatch):
    tools = CodexTools()
    query_result = {
        "rows": [],
        "returned_rows": 0,
        "truncated": False,
        "dry_run_bytes_processed": 0,
        "total_bytes_processed": 0,
        "total_bytes_billed": 0,
        "maximum_bytes_billed": 1000000000,
        "cache_hit": False,
        "job_id": "job-1",
        "statement_type": "SELECT",
    }
    list_result = {
        "objects": [],
        "returned_objects": 0,
        "truncated": False,
        "next_cursor": None,
    }
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
    monkeypatch.setattr(tools, "bigquery_query", lambda **kwargs: query_result)
    monkeypatch.setattr(tools, "storage_list", lambda **kwargs: list_result)
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


def test_tool_definitions_include_generated_safety_contracts():
    definitions = {item["name"]: item for item in tool_definitions()}

    assert len(definitions) == 8
    assert all("outputSchema" in definition for definition in definitions.values())
    assert definitions["bigquery_query"]["annotations"]["readOnlyHint"] is True
    assert definitions["bigquery_query"]["annotations"]["destructiveHint"] is False
    assert definitions["bigquery_query"]["annotations"]["openWorldHint"] is False
