"""Coverage for remaining deterministic runtime edges."""

import io
from unittest.mock import MagicMock, patch

import pytest

from GoogleCloudPlatformAPI.agents import AgentContext, AgentPlugin
from GoogleCloudPlatformAPI.codex import server as servermod
from GoogleCloudPlatformAPI.codex.tools import CodexTools


class DelegatingPlugin(AgentPlugin):
    """Plugin exposing the abstract base implementation for testing."""

    name = "delegating"

    def run(self, context: AgentContext):
        return super().run(context)


def test_agent_plugin_base_run_raises_not_implemented():
    with pytest.raises(NotImplementedError):
        DelegatingPlugin().run(AgentContext())


def test_codex_tools_default_factories_construct_clients():
    bigquery = MagicMock()
    storage = MagicMock()

    with patch(
        "GoogleCloudPlatformAPI.BigQuery.BigQuery", return_value=bigquery
    ) as bigquery_class, patch(
        "GoogleCloudPlatformAPI.CloudStorage.CloudStorage", return_value=storage
    ) as storage_class:
        tools = CodexTools()

        assert tools._bigquery() is bigquery
        assert tools._storage() is storage

    bigquery_class.assert_called_once_with()
    storage_class.assert_called_once_with()


def test_mcp_server_maps_invalid_tool_arguments_to_protocol_error():
    tools = MagicMock()
    tools.call.side_effect = ValueError("invalid arguments")
    server = servermod.MCPServer(tools)

    response = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {"name": "demo", "arguments": {}},
        }
    )

    assert response == {
        "jsonrpc": "2.0",
        "id": 7,
        "error": {"code": -32602, "message": "invalid arguments"},
    }


def test_mcp_server_run_flushes_successful_response():
    stdout = MagicMock(spec=io.StringIO)
    server = servermod.MCPServer(MagicMock())

    server.run(io.StringIO('{"jsonrpc":"2.0","id":1,"method":"ping"}\n'), stdout)

    stdout.write.assert_called_once_with('{"jsonrpc":"2.0","id":1,"result":{}}\n')
    stdout.flush.assert_called_once_with()


def test_server_main_runs_stdio_transport(monkeypatch):
    server = MagicMock()
    monkeypatch.setattr(servermod, "MCPServer", MagicMock(return_value=server))
    stdin = io.StringIO("")
    stdout = io.StringIO()
    monkeypatch.setattr(servermod.sys, "stdin", stdin)
    monkeypatch.setattr(servermod.sys, "stdout", stdout)

    servermod.main()

    server.run.assert_called_once_with(stdin, stdout)
