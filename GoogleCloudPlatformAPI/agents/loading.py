"""Utilities for loading agent plugins from explicit Python references."""

import importlib
import os
from typing import Iterable, List, Optional

from .core import AgentPlugin

PLUGIN_ENV_VAR = "GCP_API_PLUGINS"


def load_plugin(reference: str) -> AgentPlugin:
    """Load a plugin from ``package.module:attribute``.

    The attribute may be an ``AgentPlugin`` instance or an ``AgentPlugin`` class
    with a zero-argument constructor.
    """
    module_name, separator, attribute_name = reference.partition(":")
    if not separator or not module_name or not attribute_name:
        raise ValueError("Plugin reference must use package.module:attribute")
    module = importlib.import_module(module_name)
    candidate = getattr(module, attribute_name)
    plugin = candidate() if isinstance(candidate, type) else candidate
    if not isinstance(plugin, AgentPlugin):
        raise TypeError("Loaded object is not an AgentPlugin: {0}".format(reference))
    return plugin


def configured_plugin_references(value: Optional[str] = None) -> List[str]:
    """Return normalized references from an environment-style value."""
    raw = os.environ.get(PLUGIN_ENV_VAR, "") if value is None else value
    return [item.strip() for item in raw.split(",") if item.strip()]


def load_plugins(references: Optional[Iterable[str]] = None) -> List[AgentPlugin]:
    """Load plugins from references or ``GCP_API_PLUGINS``."""
    selected = (
        configured_plugin_references() if references is None else list(references)
    )
    return [load_plugin(reference) for reference in selected]
