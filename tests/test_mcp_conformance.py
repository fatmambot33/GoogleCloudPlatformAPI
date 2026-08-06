"""MCP protocol conformance tests for the local Codex server."""

import io
import json

from GoogleCloudPlatformAPI.ai_native import (
    CapabilityError,
    CapabilityExecutionError,
    evaluate_mcp_protocol,
)
from GoogleCloudPlatformAPI.codex.server import MCPServer


class FakeTools:
    """Small deterministic tool adapter for protocol tests."""

    def call(self, name, arguments):
        """Return one structured result or a normalized provider error."""
        if name == "fail":
            raise CapabilityExecutionError(
                CapabilityError(
                    code="permission_denied",
                    message="Missing permission.",
                    retryable=False,
                    guidance="Grant the documented IAM permission.",
                )
            )
        return {"name": name, "arguments": arguments}


def test_static_mcp_conformance_suite_passes():
    """Handshake, ping, discovery, schemas, and notifications conform."""
    results = evaluate_mcp_protocol()
    assert results
    assert all(result.passed for result in results)


def test_tools_call_returns_structured_content():
    """Successful calls preserve JSON data alongside text content."""
    server = MCPServer(FakeTools())
    response = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "echo", "arguments": {"value": 7}},
        }
    )
    assert response["result"]["structuredContent"] == {
        "name": "echo",
        "arguments": {"value": 7},
    }
    assert response["result"].get("isError") is None


def test_tools_call_normalizes_capability_errors():
    """Provider failures remain structured MCP tool errors."""
    server = MCPServer(FakeTools())
    response = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "fail", "arguments": {}},
        }
    )
    payload = response["result"]["structuredContent"]
    assert response["result"]["isError"] is True
    assert payload["ok"] is False
    assert payload["error"]["code"] == "permission_denied"


def test_stdio_server_handles_notifications_and_parse_errors():
    """The newline transport ignores notifications and emits parse errors."""
    stdin = io.StringIO(
        "\n".join(
            [
                json.dumps({"jsonrpc": "2.0", "method": "ping"}),
                "not-json",
                json.dumps({"jsonrpc": "2.0", "id": 2, "method": "ping"}),
            ]
        )
        + "\n"
    )
    stdout = io.StringIO()
    MCPServer(FakeTools()).run(stdin, stdout)
    responses = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert len(responses) == 2
    assert responses[0]["error"]["code"] == -32700
    assert responses[1]["result"] == {}
