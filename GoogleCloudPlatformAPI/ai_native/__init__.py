"""AI-native contracts and capability registry."""

from GoogleCloudPlatformAPI.ai_native.contracts import (
    CapabilityError,
    CapabilityResult,
    ResultMetadata,
    SafetyLevel,
)
from GoogleCloudPlatformAPI.ai_native.defaults import register_default_capabilities
from GoogleCloudPlatformAPI.ai_native.errors import (
    CapabilityExecutionError,
    CapabilityTimeoutError,
    normalize_exception,
    sanitize_message,
)
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
from GoogleCloudPlatformAPI.ai_native.pagination import (
    CursorError,
    decode_cursor,
    encode_cursor,
)
from GoogleCloudPlatformAPI.ai_native.registry import (
    Capability,
    CapabilityRegistry,
    capability_registry,
)
from GoogleCloudPlatformAPI.ai_native.runtime import (
    execute_capability,
    redact,
    run_with_timeout,
)
from GoogleCloudPlatformAPI.ai_native.schema import (
    SchemaValidationError,
    validate_json_schema,
)
from GoogleCloudPlatformAPI.ai_native.sql import (
    ReadOnlyQueryError,
    validate_single_read_query,
)

__all__ = [
    "Capability",
    "CapabilityError",
    "CapabilityExecutionError",
    "CapabilityRegistry",
    "CapabilityResult",
    "CapabilityTimeoutError",
    "CursorError",
    "EvaluationResult",
    "ReadOnlyQueryError",
    "ResultMetadata",
    "SafetyLevel",
    "SchemaValidationError",
    "capability_reference_markdown",
    "capability_registry",
    "compare_compatibility_snapshots",
    "compatibility_snapshot",
    "decode_cursor",
    "encode_cursor",
    "evaluate_registry",
    "execute_capability",
    "mcp_tool_definitions",
    "normalize_exception",
    "readiness_score",
    "redact",
    "register_default_capabilities",
    "run_with_timeout",
    "sanitize_message",
    "validate_json_schema",
    "validate_single_read_query",
]
