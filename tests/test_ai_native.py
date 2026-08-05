"""Tests for the canonical AI-native capability contracts."""

import json

import pytest

from GoogleCloudPlatformAPI.ai_native import (
    Capability,
    CapabilityError,
    CapabilityRegistry,
    CapabilityResult,
    ResultMetadata,
    SafetyLevel,
    capability_registry,
    execute_capability,
    readiness_score,
    redact,
)
from GoogleCloudPlatformAPI.codex.tools import tool_definitions


def test_default_registry_exposes_current_tools():
    """The current MCP surface is represented by stable contracts."""
    assert {item.name for item in capability_registry.list()} == {
        "bigquery_query",
        "gcp_context",
        "gcs_list_objects",
        "gcs_read_text",
    }
    json.dumps(capability_registry.schema())


def test_mcp_definitions_are_generated_from_registry():
    """MCP schemas stay synchronized with the canonical registry."""
    definitions = {item["name"]: item for item in tool_definitions()}
    assert set(definitions) == {
        item.name for item in capability_registry.list()
    }
    for capability in capability_registry.list():
        assert definitions[capability.name]["inputSchema"] == capability.input_schema


def test_registry_rejects_duplicates():
    """Stable capability names cannot be silently replaced."""
    registry = CapabilityRegistry()
    capability = Capability(
        name="example",
        service="test",
        operation="read",
        description="Example capability.",
        input_schema={"type": "object"},
        output_schema={"type": "object"},
    )
    registry.register(capability)
    with pytest.raises(ValueError):
        registry.register(capability)


def test_result_envelope_is_json_serializable():
    """Results are deterministic and safe to pass between tool adapters."""
    result = CapabilityResult(
        ok=False,
        data=None,
        metadata=ResultMetadata(
            request_id="request-1",
            service="bigquery",
            operation="query",
            duration_ms=12,
            truncated=False,
        ),
        error=CapabilityError(
            code="permission_denied",
            message="Missing permission.",
            guidance="Grant bigquery.jobs.create.",
        ),
    )
    payload = result.to_dict()
    assert payload["error"]["code"] == "permission_denied"
    assert SafetyLevel.READ_ONLY.value == "read_only"
    json.dumps(payload)


def test_execution_runtime_returns_stable_envelope():
    """Runtime execution adds metadata without changing handler results."""
    result = execute_capability(
        capability_registry,
        "gcp_context",
        {},
        handler=lambda: {"project_id": "example"},
    )
    assert result.ok is True
    assert result.data == {"project_id": "example"}
    assert result.metadata.service == "gcp"
    json.dumps(result.to_dict())


def test_secret_redaction_is_recursive():
    """Secret-bearing keys are removed before logging."""
    assert redact(
        {"token": "secret", "nested": {"password": "secret", "safe": 1}}
    ) == {
        "token": "[REDACTED]",
        "nested": {"password": "[REDACTED]", "safe": 1},
    }


def test_readiness_score_is_release_friendly():
    """The deterministic scorecard is complete and JSON serializable."""
    scorecard = readiness_score(capability_registry)
    assert scorecard["ready"] is True
    assert scorecard["score"] == 100.0
    json.dumps(scorecard)
