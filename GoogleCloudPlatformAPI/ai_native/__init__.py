"""AI-native contracts and capability registry."""

from GoogleCloudPlatformAPI.ai_native.contracts import (
    CapabilityError,
    CapabilityResult,
    ResultMetadata,
    SafetyLevel,
)
from GoogleCloudPlatformAPI.ai_native.defaults import register_default_capabilities
from GoogleCloudPlatformAPI.ai_native.evaluation import (
    EvaluationResult,
    evaluate_registry,
    readiness_score,
)
from GoogleCloudPlatformAPI.ai_native.generation import (
    capability_reference_markdown,
    compare_compatibility_snapshots,
    compatibility_snapshot,
    mcp_tool_definitions,
)
from GoogleCloudPlatformAPI.ai_native.registry import (
    Capability,
    CapabilityRegistry,
    capability_registry,
)
from GoogleCloudPlatformAPI.ai_native.runtime import execute_capability, redact
from GoogleCloudPlatformAPI.ai_native.schema import (
    SchemaValidationError,
    validate_json_schema,
)

__all__ = [
    "Capability",
    "CapabilityError",
    "CapabilityRegistry",
    "CapabilityResult",
    "EvaluationResult",
    "ResultMetadata",
    "SafetyLevel",
    "SchemaValidationError",
    "capability_reference_markdown",
    "capability_registry",
    "compare_compatibility_snapshots",
    "compatibility_snapshot",
    "evaluate_registry",
    "execute_capability",
    "mcp_tool_definitions",
    "readiness_score",
    "redact",
    "register_default_capabilities",
    "validate_json_schema",
]
