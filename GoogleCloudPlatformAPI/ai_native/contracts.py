"""Typed contracts for AI-native capability execution."""

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class SafetyLevel(str, Enum):
    """Safety classification for a capability."""

    READ_ONLY = "read_only"
    MUTATING = "mutating"


@dataclass(frozen=True)
class CapabilityError:
    """Machine-readable error returned by capability execution."""

    code: str
    message: str
    retryable: bool = False
    guidance: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class ResultMetadata:
    """Operational metadata attached to every capability result."""

    request_id: str
    service: str
    operation: str
    duration_ms: int
    truncated: bool = False
    warnings: List[str] = field(default_factory=list)
    provenance: Dict[str, Any] = field(default_factory=dict)
    next_cursor: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation."""
        return asdict(self)


@dataclass(frozen=True)
class CapabilityResult:
    """Stable result envelope shared by CLI, MCP, and agent adapters."""

    ok: bool
    data: Any
    metadata: ResultMetadata
    error: Optional[CapabilityError] = None

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "ok": self.ok,
            "data": self.data,
            "metadata": self.metadata.to_dict(),
            "error": self.error.to_dict() if self.error else None,
        }
