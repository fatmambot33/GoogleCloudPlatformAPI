"""Tests for the optional OpenAI Agents SDK adapter."""

import asyncio
import json
import types

import pytest

from GoogleCloudPlatformAPI.agents import openai as openai_adapter
from GoogleCloudPlatformAPI.ai_native import Capability, CapabilityRegistry


def _schema(properties, required):
    """Build a strict test schema."""
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def _registry():
    """Return a minimal registry for adapter execution tests."""
    registry = CapabilityRegistry()
    registry.register(
        Capability(
            name="echo_value",
            service="test",
            operation="echo",
            description="Echo one integer.",
            input_schema=_schema({"value": {"type": "integer"}}, ["value"]),
            output_schema=_schema({"value": {"type": "integer"}}, ["value"]),
            adapter_method="echo",
        )
    )
    return registry


def test_openai_specs_are_generated_from_canonical_contracts():
    """Framework-neutral specs preserve strict registry schemas."""
    registry = _registry()
    specs = openai_adapter.openai_tool_specs(registry)
    assert specs == [
        {
            "type": "function",
            "name": "echo_value",
            "description": "Echo one integer.",
            "parameters": registry.get("echo_value").input_schema,
            "strict": True,
        }
    ]


def test_missing_optional_sdk_has_actionable_error(monkeypatch):
    """Importing the base package never requires the OpenAI Agents SDK."""

    def missing(_name):
        raise ImportError("missing")

    monkeypatch.setattr(openai_adapter.importlib, "import_module", missing)
    with pytest.raises(RuntimeError, match="openai-agents"):
        openai_adapter.build_openai_tools(
            lambda _name, arguments: arguments, _registry()
        )


def test_build_openai_tools_validates_and_invokes(monkeypatch):
    """SDK tools use canonical schemas before and after invocation."""

    class FakeFunctionTool:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    fake_module = types.SimpleNamespace(FunctionTool=FakeFunctionTool)
    monkeypatch.setattr(
        openai_adapter.importlib, "import_module", lambda _name: fake_module
    )
    tools = openai_adapter.build_openai_tools(
        lambda _name, arguments: {"value": arguments["value"]}, _registry()
    )
    assert len(tools) == 1
    assert tools[0].name == "echo_value"
    assert tools[0].params_json_schema["additionalProperties"] is False
    output = asyncio.run(tools[0].on_invoke_tool(None, '{"value":7}'))
    assert json.loads(output) == {"value": 7}
    with pytest.raises(ValueError, match="unexpected property"):
        asyncio.run(tools[0].on_invoke_tool(None, '{"value":7,"unexpected":true}'))


def test_build_openai_agent_uses_generated_tools(monkeypatch):
    """The optional Agent wrapper remains a thin generated adapter."""

    class FakeFunctionTool:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    class FakeAgent:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    fake_module = types.SimpleNamespace(
        FunctionTool=FakeFunctionTool,
        Agent=FakeAgent,
    )
    monkeypatch.setattr(
        openai_adapter.importlib, "import_module", lambda _name: fake_module
    )
    agent = openai_adapter.build_openai_agent(
        name="GCP assistant",
        instructions="Use bounded tools.",
        invoke=lambda _name, arguments: {"value": arguments["value"]},
        registry=_registry(),
        model="gpt-5.4-mini",
    )
    assert agent.name == "GCP assistant"
    assert agent.model == "gpt-5.4-mini"
    assert [tool.name for tool in agent.tools] == ["echo_value"]
