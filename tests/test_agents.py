"""Tests for the public agent and plugin API."""

import pytest

from GoogleCloudPlatformAPI.agents import Agent, AgentContext, AgentPlugin, load_plugin


class AddProjectPlugin(AgentPlugin):
    """Test plugin adding a project identifier."""

    name = "project"
    description = "Add a project identifier."

    def run(self, context: AgentContext):
        return {"project_id": context.values.get("requested_project", "default")}


def test_agent_runs_plugins_and_merges_context():
    agent = Agent([AddProjectPlugin()])

    result = agent.run({"requested_project": "demo"})

    assert result == {"requested_project": "demo", "project_id": "demo"}


def test_agent_rejects_duplicate_plugin_names():
    with pytest.raises(ValueError, match="already registered"):
        Agent([AddProjectPlugin(), AddProjectPlugin()])


def test_agent_rejects_unknown_selected_plugin():
    with pytest.raises(ValueError, match="Unknown plugin"):
        Agent([AddProjectPlugin()]).run(plugin_names=["missing"])


def test_load_plugin_from_module_reference():
    plugin = load_plugin("tests.test_agents:AddProjectPlugin")

    assert isinstance(plugin, AddProjectPlugin)
