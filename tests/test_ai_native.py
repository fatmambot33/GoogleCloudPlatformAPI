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
)


def test_default_registry_exposes_current_tools():
    """The current MCP surface is represented by stable contracts."""
    assert {item.name for item in capability_registry.list()} == {
        "bigquery_query",
        "gcp_context",
        "gcs_list_objects",
        "gcs_read_text",
    }
    json.dumps(capability_registry.schema())


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
