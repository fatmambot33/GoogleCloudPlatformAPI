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
from GoogleCloudPlatformAPI.ai_native.evidence import (
    ScenarioObservation,
    compare_scorecards,
    contains_prompt_injection,
    default_observations,
    evaluate_mcp_protocol,
    evaluate_scenarios,
    junit_xml,
    release_scorecard,
    scorecard_markdown,
    write_release_evidence,
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
from GoogleCloudPlatformAPI.ai_native.scenarios import (
    GoldenScenario,
    InjectionFixture,
    ScenarioStep,
    covered_capabilities,
    estimate_token_footprint,
    golden_scenarios,
    prompt_injection_fixtures,
    scenario_index,
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
    "GoldenScenario",
    "InjectionFixture",
    "ReadOnlyQueryError",
    "ResultMetadata",
    "SafetyLevel",
    "ScenarioObservation",
    "ScenarioStep",
    "SchemaValidationError",
    "capability_reference_markdown",
    "capability_registry",
    "compare_compatibility_snapshots",
    "compare_scorecards",
    "compatibility_snapshot",
    "contains_prompt_injection",
    "covered_capabilities",
    "decode_cursor",
    "default_observations",
    "encode_cursor",
    "estimate_token_footprint",
    "evaluate_mcp_protocol",
    "evaluate_registry",
    "evaluate_scenarios",
    "execute_capability",
    "golden_scenarios",
    "junit_xml",
    "mcp_tool_definitions",
    "normalize_exception",
    "prompt_injection_fixtures",
    "readiness_score",
    "redact",
    "register_default_capabilities",
    "release_scorecard",
    "run_with_timeout",
    "sanitize_message",
    "scenario_index",
    "scorecard_markdown",
    "validate_json_schema",
    "validate_single_read_query",
    "write_release_evidence",
]
