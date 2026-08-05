"""Canonical registry for AI-facing capabilities."""

from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional

from GoogleCloudPlatformAPI.ai_native.contracts import SafetyLevel


@dataclass(frozen=True)
class Capability:
    """Describe one stable operation exposed to humans and agents."""

    name: str
    service: str
    operation: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    safety: SafetyLevel = SafetyLevel.READ_ONLY
    version: str = "1.0.0"
    permissions: List[str] = field(default_factory=list)
    timeout_seconds: int = 30
    handler: Optional[Callable[..., Any]] = field(default=None, compare=False, repr=False)

    def to_dict(self) -> Dict[str, Any]:
        """Return public, JSON-serializable capability metadata."""
        value = asdict(self)
        value.pop("handler", None)
        value["safety"] = self.safety.value
        return value


class CapabilityRegistry:
    """Store and validate capability definitions from one source of truth."""

    def __init__(self) -> None:
        self._capabilities: Dict[str, Capability] = {}

    def register(self, capability: Capability) -> Capability:
        """Register a unique capability and return it."""
        if capability.name in self._capabilities:
            raise ValueError("Capability already registered: %s" % capability.name)
        if capability.timeout_seconds <= 0:
            raise ValueError("Capability timeout must be positive")
        self._capabilities[capability.name] = capability
        return capability

    def get(self, name: str) -> Capability:
        """Return one capability by stable name."""
        try:
            return self._capabilities[name]
        except KeyError:
            raise KeyError("Unknown capability: %s" % name)

    def list(self, service: Optional[str] = None) -> List[Capability]:
        """Return capabilities, optionally filtered by service."""
        values: Iterable[Capability] = self._capabilities.values()
        if service is not None:
            values = (item for item in values if item.service == service)
        return sorted(values, key=lambda item: item.name)

    def schema(self) -> Dict[str, Any]:
        """Return a machine-readable registry snapshot."""
        return {
            "schema_version": "1.0.0",
            "capabilities": [item.to_dict() for item in self.list()],
        }


capability_registry = CapabilityRegistry()
