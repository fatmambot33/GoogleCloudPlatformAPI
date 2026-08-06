"""Optional OpenAI Agents SDK adapters generated from shared contracts."""

import importlib
import inspect
import json
from typing import Any, Callable, Dict, List, Optional

from GoogleCloudPlatformAPI.ai_native.registry import (
    CapabilityRegistry,
    capability_registry,
)

CapabilityInvoker = Callable[[str, Dict[str, Any]], Any]


def openai_tool_specs(
    registry: CapabilityRegistry = capability_registry,
) -> List[Dict[str, Any]]:
    """Return framework-neutral OpenAI function tool specifications."""
    return [
        {
            "type": "function",
            "name": capability.name,
            "description": capability.description,
            "parameters": capability.input_schema,
            "strict": True,
        }
        for capability in registry.list()
    ]


def _agents_module() -> Any:
    """Import the optional OpenAI Agents SDK with actionable guidance."""
    try:
        return importlib.import_module("agents")
    except ImportError as exc:
        raise RuntimeError(
            "Install GoogleCloudPlatformAPI[openai-agents] to build SDK tools."
        ) from exc


def _json_ready(value: Any) -> Any:
    """Normalize common typed results before JSON serialization."""
    to_dict = getattr(value, "to_dict", None)
    return to_dict() if callable(to_dict) else value


def build_openai_tools(
    invoke: CapabilityInvoker,
    registry: CapabilityRegistry = capability_registry,
) -> List[Any]:
    """Build OpenAI Agents SDK ``FunctionTool`` objects lazily.

    Parameters
    ----------
    invoke : callable
        Function receiving ``(capability_name, arguments)``. It may return a
        value directly or an awaitable.
    registry : CapabilityRegistry, optional
        Canonical capability registry used to generate tool contracts.
    """
    agents_module = _agents_module()
    function_tool = getattr(agents_module, "FunctionTool")
    tools = []
    for capability in registry.list():
        capability_name = capability.name

        async def on_invoke_tool(
            _context: Any,
            arguments_json: str,
            _capability_name: str = capability_name,
        ) -> str:
            arguments = json.loads(arguments_json)
            registry.validate_input(_capability_name, arguments)
            result = invoke(_capability_name, arguments)
            if inspect.isawaitable(result):
                result = await result
            normalized = _json_ready(result)
            registry.validate_output(_capability_name, normalized)
            return json.dumps(normalized, sort_keys=True, separators=(",", ":"))

        tools.append(
            function_tool(
                name=capability.name,
                description=capability.description,
                params_json_schema=capability.input_schema,
                on_invoke_tool=on_invoke_tool,
            )
        )
    return tools


def build_openai_agent(
    name: str,
    instructions: str,
    invoke: CapabilityInvoker,
    registry: CapabilityRegistry = capability_registry,
    model: Optional[str] = None,
) -> Any:
    """Build an optional OpenAI ``Agent`` from canonical capability tools."""
    agents_module = _agents_module()
    agent_class = getattr(agents_module, "Agent")
    arguments: Dict[str, Any] = {
        "name": name,
        "instructions": instructions,
        "tools": build_openai_tools(invoke, registry),
    }
    if model is not None:
        arguments["model"] = model
    return agent_class(**arguments)
