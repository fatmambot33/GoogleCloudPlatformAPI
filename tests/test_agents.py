"""Tests for the public agent, plugin, CLI, and MCP APIs."""

import io
import json

import pytest

from GoogleCloudPlatformAPI.agents import Agent, AgentContext, AgentPlugin, load_plugin
from GoogleCloudPlatformAPI.agents.cli import _json_object, main
from GoogleCloudPlatformAPI.agents.loading import (
    PLUGIN_ENV_VAR,
    configured_plugin_references,
    load_plugins,
)
from GoogleCloudPlatformAPI.codex.server import MCPServer
from GoogleCloudPlatformAPI.codex.tools import CodexTools, _json_value, _row_to_dict


class AddProjectPlugin(AgentPlugin):
    """Test plugin adding a project identifier."""

    name = "project"
    description = "Add a project identifier."

    def run(self, context: AgentContext):
        return {"project_id": context.values.get("requested_project", "default")}


class EmptyNamePlugin(AgentPlugin):
    """Test plugin with an invalid empty name."""

    def run(self, context: AgentContext):
        return {}


class InvalidResultPlugin(AgentPlugin):
    """Test plugin returning an invalid result."""

    name = "invalid"

    def run(self, context: AgentContext):
        return None


class FakeTools:
    """Small MCP tool double."""

    def call(self, name, arguments):
        return {"name": name, "arguments": arguments}


def test_agent_runs_plugins_and_merges_context():
    agent = Agent([AddProjectPlugin()])

    result = agent.run({"requested_project": "demo"})

    assert result == {"requested_project": "demo", "project_id": "demo"}


def test_agent_rejects_non_plugin():
    with pytest.raises(TypeError, match="AgentPlugin"):
        Agent([object()])


def test_agent_rejects_empty_plugin_name():
    with pytest.raises(ValueError, match="must not be empty"):
        Agent([EmptyNamePlugin()])


def test_agent_rejects_duplicate_plugin_names():
    with pytest.raises(ValueError, match="already registered"):
        Agent([AddProjectPlugin(), AddProjectPlugin()])


def test_agent_rejects_unknown_selected_plugin():
    with pytest.raises(ValueError, match="Unknown plugin"):
        Agent([AddProjectPlugin()]).run(plugin_names=["missing"])


def test_agent_rejects_non_mapping_result():
    with pytest.raises(TypeError, match="must return a mapping"):
        Agent([InvalidResultPlugin()]).run()


def test_load_plugin_from_module_reference():
    plugin = load_plugin("tests.test_agents:AddProjectPlugin")

    assert isinstance(plugin, AddProjectPlugin)


def test_load_plugin_rejects_bad_reference():
    with pytest.raises(ValueError, match="package.module:attribute"):
        load_plugin("invalid")


def test_load_plugin_rejects_non_plugin_attribute():
    with pytest.raises(TypeError, match="not an AgentPlugin"):
        load_plugin("tests.test_agents:json")


def test_configured_plugin_references_from_value():
    assert configured_plugin_references(" a:b, c:d ,, ") == ["a:b", "c:d"]


def test_configured_plugin_references_from_environment(monkeypatch):
    monkeypatch.setenv(PLUGIN_ENV_VAR, "tests.test_agents:AddProjectPlugin")

    assert configured_plugin_references() == ["tests.test_agents:AddProjectPlugin"]


def test_load_plugins_from_explicit_references():
    plugins = load_plugins(["tests.test_agents:AddProjectPlugin"])

    assert len(plugins) == 1
    assert isinstance(plugins[0], AddProjectPlugin)


def test_load_plugins_from_environment(monkeypatch):
    monkeypatch.setenv(PLUGIN_ENV_VAR, "tests.test_agents:AddProjectPlugin")

    plugins = load_plugins()

    assert isinstance(plugins[0], AddProjectPlugin)


def test_json_object_accepts_object():
    assert _json_object('{"project_id": "demo"}') == {"project_id": "demo"}


def test_json_object_rejects_non_object():
    with pytest.raises(Exception, match="JSON object"):
        _json_object("[]")


def test_cli_runs_plugin_and_prints_json(capsys):
    exit_code = main(
        [
            "--plugin",
            "tests.test_agents:AddProjectPlugin",
            "--context",
            '{"requested_project": "demo"}',
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out) == {
        "requested_project": "demo",
        "project_id": "demo",
    }


def test_json_helpers_cover_common_values():
    assert _json_value(b"hello") == "hello"
    assert _json_value({"items": [1, None]}) == {"items": [1, None]}
    assert _row_to_dict({"value": b"ok"}) == {"value": "ok"}
    assert _row_to_dict("row") == {"value": "row"}


def test_codex_tools_context(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "demo")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/key.json")

    assert CodexTools().context() == {
        "project_id": "demo",
        "credentials_configured": True,
        "credentials_file": "key.json",
        "write_tools_enabled": False,
    }


def test_mcp_server_handles_core_protocol_methods():
    server = MCPServer(FakeTools())

    initialized = server.handle(
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    )
    listed = server.handle({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    called = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "demo", "arguments": {"value": 1}},
        }
    )
    missing = server.handle({"jsonrpc": "2.0", "id": 4, "method": "missing"})

    assert initialized["result"]["protocolVersion"]
    assert listed["result"]["tools"]
    assert called["result"]["structuredContent"]["name"] == "demo"
    assert missing["error"]["code"] == -32601
    assert server.handle({"jsonrpc": "2.0", "method": "ping"}) is None


def test_mcp_server_runs_json_lines():
    server = MCPServer(FakeTools())
    stdin = io.StringIO('\n{"jsonrpc":"2.0","id":1,"method":"ping"}\nnot-json\n')
    stdout = io.StringIO()

    server.run(stdin, stdout)

    responses = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert responses[0]["result"] == {}
    assert responses[1]["error"]["code"] == -32700
