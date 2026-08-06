"""Canonical registry for AI-facing capabilities."""

import re
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Callable, Dict, Iterable, List, Optional

from GoogleCloudPlatformAPI.ai_native.contracts import SafetyLevel
from GoogleCloudPlatformAPI.ai_native.schema import validate_json_schema

_CAPABILITY_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_SEMANTIC_VERSION = re.compile(r"^\d+\.\d+\.\d+$")


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
    adapter_method: Optional[str] = None
    deprecated: bool = False
    replaced_by: Optional[str] = None
    handler: Optional[Callable[..., Any]] = field(
        default=None, compare=False, repr=False
    )

    def to_dict(self) -> Dict[str, Any]:
        """Return public, JSON-serializable capability metadata."""
        value = asdict(self)
        value.pop("handler", None)
        value["safety"] = self.safety.value
        return value


class CapabilityRegistry:
    """Store, validate, and execute capability contracts from one source."""

    def __init__(self) -> None:
        self._capabilities: Dict[str, Capability] = {}

    @staticmethod
    def _validate_contract(capability: Capability) -> None:
        """Validate invariant fields before a capability is registered."""
        if not _CAPABILITY_NAME.fullmatch(capability.name):
            raise ValueError("Capability names must use lowercase snake_case")
        if not _SEMANTIC_VERSION.fullmatch(capability.version):
            raise ValueError("Capability versions must use semantic versioning")
        if capability.timeout_seconds <= 0:
            raise ValueError("Capability timeout must be positive")
        for label, schema in (
            ("input", capability.input_schema),
            ("output", capability.output_schema),
        ):
            if schema.get("type") != "object":
                raise ValueError(f"Capability {label} schema must be an object")
            if schema.get("additionalProperties") is not False:
                raise ValueError(
                    f"Capability {label} schema must reject unknown top-level fields"
                )
        if capability.deprecated and not capability.replaced_by:
            raise ValueError("Deprecated capabilities must identify a replacement")

    def register(self, capability: Capability) -> Capability:
        """Register a unique capability and return it."""
        if capability.name in self._capabilities:
            raise ValueError("Capability already registered: %s" % capability.name)
        self._validate_contract(capability)
        self._capabilities[capability.name] = capability
        return capability

    def bind(self, name: str, handler: Callable[..., Any]) -> Capability:
        """Attach a direct execution handler without changing public metadata."""
        capability = replace(self.get(name), handler=handler)
        self._capabilities[name] = capability
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

    def validate_input(self, name: str, arguments: Dict[str, Any]) -> None:
        """Validate capability arguments before adapter execution."""
        validate_json_schema(arguments, self.get(name).input_schema)

    def validate_output(self, name: str, result: Any) -> None:
        """Validate adapter output before it crosses a tool boundary."""
        validate_json_schema(result, self.get(name).output_schema)

    def schema(self) -> Dict[str, Any]:
        """Return a machine-readable registry snapshot."""
        return {
            "schema_version": "1.1.0",
            "capabilities": [item.to_dict() for item in self.list()],
        }


capability_registry = CapabilityRegistry()
