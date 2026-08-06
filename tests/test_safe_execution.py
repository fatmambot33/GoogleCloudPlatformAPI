"""Tests for bounded execution, cursors, errors, and SQL safety."""

import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from GoogleCloudPlatformAPI.ai_native import (
    CapabilityExecutionError,
    CursorError,
    SafetyLevel,
    decode_cursor,
    encode_cursor,
    normalize_exception,
    run_with_timeout,
    sanitize_message,
    validate_single_read_query,
)
from GoogleCloudPlatformAPI.codex.server import MCPServer
from GoogleCloudPlatformAPI.codex.tools import CodexTools


def test_sql_validator_allows_literals_and_rejects_scripts_and_mutation():
    """Only one conservative read statement crosses the billable boundary."""
    assert validate_single_read_query("SELECT ';' AS value;") == "SELECT ';' AS value"
    assert validate_single_read_query(
        "-- comment\nWITH x AS (SELECT 1) SELECT * FROM x"
    )
    with pytest.raises(ValueError, match="multiple statements"):
        validate_single_read_query("SELECT 1; SELECT 2")
    with pytest.raises(ValueError, match="disabled statement keywords"):
        validate_single_read_query("WITH x AS (SELECT 1) DELETE FROM table")


def test_opaque_cursor_round_trip_and_context_binding():
    """Provider tokens cannot be reused for a different resource context."""
    cursor = encode_cursor(
        "bigquery", "list_tables", "provider-token", {"dataset_id": "events"}
    )
    assert (
        decode_cursor(cursor, "bigquery", "list_tables", {"dataset_id": "events"})
        == "provider-token"
    )
    with pytest.raises(CursorError, match="does not match"):
        decode_cursor(cursor, "bigquery", "list_tables", {"dataset_id": "archive"})


def test_error_normalization_is_stable_retryable_and_redacted():
    """Provider details become safe error codes with recovery guidance."""
    TooManyRequests = type("TooManyRequests", (Exception,), {})
    error = normalize_exception(TooManyRequests("token=super-secret quota"))

    assert error.code == "quota_exceeded"
    assert error.retryable is True
    assert "[REDACTED]" in error.message
    assert "super-secret" not in error.message
    assert sanitize_message("Authorization: Bearer abc") == (
        "Authorization: Bearer [REDACTED]"
    )


def test_generic_timeout_is_enforced():
    """Capability handlers cannot wait forever."""

    def slow():
        time.sleep(0.05)
        return True

    with pytest.raises(TimeoutError):
        run_with_timeout(slow, {}, 0.001)


def test_bigquery_timeout_requests_job_cancellation():
    """Timed-out query jobs request provider cancellation."""
    dry_job = SimpleNamespace(total_bytes_processed=1, statement_type="SELECT")
    job = MagicMock()
    job.result.side_effect = FutureTimeoutError()
    bigquery = MagicMock()
    bigquery._client.query.side_effect = [dry_job, job]
    tools = CodexTools(bigquery_factory=lambda: bigquery)

    with pytest.raises(TimeoutError, match="cancellation was requested"):
        tools.bigquery_query("SELECT 1", timeout_seconds=1)
    job.cancel.assert_called_once_with(timeout=1.0)


def test_mcp_errors_are_machine_readable_without_json_rpc_failure(monkeypatch):
    """Provider failures return MCP tool errors with stable structured content."""
    tools = CodexTools()
    monkeypatch.setattr(
        tools,
        "call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            CapabilityExecutionError(
                normalize_exception(type("Forbidden", (Exception,), {})("denied"))
            )
        ),
    )
    response = MCPServer(tools).handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "gcp_context", "arguments": {}},
        }
    )

    assert response["result"]["isError"] is True
    assert response["result"]["structuredContent"]["error"]["code"] == (
        "permission_denied"
    )


def test_safety_levels_distinguish_inspection_and_billable_reads():
    """Compatibility alias remains read-only while cost is explicit."""
    assert SafetyLevel.READ_ONLY.value == "inspection"
    assert SafetyLevel.INSPECTION.value == "inspection"
    assert SafetyLevel.BILLABLE_READ.value == "billable_read"
