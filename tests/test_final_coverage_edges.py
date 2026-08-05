"""Coverage for remaining deterministic runtime edges."""

import importlib
import io
import runpy
import sys
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
    bigquery_module = importlib.import_module("GoogleCloudPlatformAPI.BigQuery")
    storage_module = importlib.import_module("GoogleCloudPlatformAPI.CloudStorage")

    with patch.object(
        bigquery_module, "BigQuery", return_value=bigquery
    ) as bigquery_class, patch.object(
        storage_module, "CloudStorage", return_value=storage
    ) as storage_class:
        tools = CodexTools()

        assert tools._bigquery() is bigquery
        assert tools._storage() is storage

    bigquery_class.assert_called_once_with()
    storage_class.assert_called_once_with()


def test_bigquery_explicit_credentials_and_context_close():
    bigquery_module = importlib.import_module("GoogleCloudPlatformAPI.BigQuery")
    credentials = MagicMock(project_id="project")
    client = MagicMock()

    with patch.object(
        bigquery_module.ServiceAccount,
        "from_service_account_file",
        return_value=credentials,
    ) as from_file, patch.object(
        bigquery_module.bigquery, "Client", return_value=client
    ) as client_class:
        instance = bigquery_module.BigQuery("service-account.json")

    from_file.assert_called_once_with("service-account.json")
    client_class.assert_called_once_with(credentials=credentials, project="project")
    assert instance.__enter__() is instance
    instance.__exit__(None, None, None)
    client.close.assert_called_once_with()


def test_bigquery_environment_credentials(monkeypatch):
    bigquery_module = importlib.import_module("GoogleCloudPlatformAPI.BigQuery")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")
    credentials = MagicMock(project_id="project")
    client = MagicMock()

    with patch.object(
        bigquery_module.ServiceAccount,
        "from_service_account_file",
        return_value=credentials,
    ) as from_file, patch.object(
        bigquery_module.bigquery, "Client", return_value=client
    ) as client_class:
        bigquery_module.BigQuery()

    from_file.assert_called_once_with()
    client_class.assert_called_once_with(credentials=credentials, project="project")


def test_bigquery_application_default_credentials(monkeypatch):
    bigquery_module = importlib.import_module("GoogleCloudPlatformAPI.BigQuery")
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    credentials = MagicMock()
    client = MagicMock()

    with patch.object(
        bigquery_module.auth,
        "default",
        return_value=(credentials, "project"),
    ) as default_credentials, patch.object(
        bigquery_module.bigquery, "Client", return_value=client
    ) as client_class:
        bigquery_module.BigQuery()

    default_credentials.assert_called_once_with(scopes=bigquery_module.BigQuery.SCOPES)
    client_class.assert_called_once_with(credentials=credentials, project="project")


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


def test_agent_cli_module_entry_point(monkeypatch, capsys):
    """Execute the CLI module through its guarded entry point."""
    monkeypatch.setattr(sys, "argv", ["gcp-agent"])

    with pytest.raises(SystemExit) as error:
        runpy.run_module("GoogleCloudPlatformAPI.agents.cli", run_name="__main__")

    assert error.value.code == 0
    assert capsys.readouterr().out == "{}\n"


def test_codex_server_module_entry_point(monkeypatch):
    """Execute the MCP server module through its guarded entry point."""
    monkeypatch.setattr(sys, "stdin", io.StringIO(""))
    monkeypatch.setattr(sys, "stdout", io.StringIO())

    runpy.run_module("GoogleCloudPlatformAPI.codex.server", run_name="__main__")
