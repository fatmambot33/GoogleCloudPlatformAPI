"""Small, typed runtime for composing GCP-focused agents and plugins."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional


@dataclass
class AgentContext:
    """Mutable state shared across plugins during one agent run.

    Parameters
    ----------
    values : mapping, optional
        Initial context values.
    """

    values: MutableMapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, values: Optional[Mapping[str, Any]] = None) -> "AgentContext":
        """Create a context from an optional mapping."""
        return cls(dict(values or {}))


class AgentPlugin(ABC):
    """Base class for deterministic, composable agent plugins."""

    name = ""
    description = ""

    @abstractmethod
    def run(self, context: AgentContext) -> Mapping[str, Any]:
        """Execute the plugin and return values to merge into the context."""
        raise NotImplementedError


class Agent:
    """Run registered plugins in a predictable order.

    Parameters
    ----------
    plugins : iterable of AgentPlugin, optional
        Plugins to register initially.
    """

    def __init__(self, plugins: Optional[Iterable[AgentPlugin]] = None) -> None:
        self._plugins: Dict[str, AgentPlugin] = {}
        for plugin in plugins or ():
            self.register(plugin)

    @property
    def plugin_names(self) -> List[str]:
        """Return registered plugin names in execution order."""
        return list(self._plugins)

    def register(self, plugin: AgentPlugin) -> None:
        """Register one plugin.

        Raises
        ------
        TypeError
            If the object is not an ``AgentPlugin``.
        ValueError
            If its name is empty or already registered.
        """
        if not isinstance(plugin, AgentPlugin):
            raise TypeError("plugin must be an AgentPlugin instance")
        name = plugin.name.strip()
        if not name:
            raise ValueError("plugin.name must not be empty")
        if name in self._plugins:
            raise ValueError("Plugin already registered: {0}".format(name))
        self._plugins[name] = plugin

    def run(
        self,
        values: Optional[Mapping[str, Any]] = None,
        plugin_names: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        """Run selected plugins and return the final context.

        Parameters
        ----------
        values : mapping, optional
            Initial context values.
        plugin_names : iterable of str, optional
            Ordered subset to execute. All plugins run when omitted.
        """
        context = AgentContext.from_mapping(values)
        names = list(plugin_names) if plugin_names is not None else self.plugin_names
        for name in names:
            try:
                plugin = self._plugins[name]
            except KeyError:
                raise ValueError("Unknown plugin: {0}".format(name))
            result = plugin.run(context)
            if not isinstance(result, Mapping):
                raise TypeError("Plugin {0} must return a mapping".format(name))
            context.values.update(result)
        return dict(context.values)
