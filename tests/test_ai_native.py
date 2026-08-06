"""Tests for the canonical AI-native capability contracts."""

import json
from dataclasses import replace

import pytest

from GoogleCloudPlatformAPI.ai_native import (
    Capability,
    CapabilityError,
    CapabilityRegistry,
    CapabilityResult,
    ResultMetadata,
    SafetyLevel,
    capability_reference_markdown,
    capability_registry,
    compare_compatibility_snapshots,
    compatibility_snapshot,
    execute_capability,
    readiness_score,
    redact,
)
from GoogleCloudPlatformAPI.codex.tools import tool_definitions


def _strict_schema(properties=None, required=None):
    """Build a strict object schema for focused registry tests."""
    return {
        "type": "object",
        "properties": properties or {},
        "required": required or [],
        "additionalProperties": False,
    }


def _example_capability(name="example"):
    """Build one valid test capability."""
    return Capability(
        name=name,
        service="test",
        operation="read",
        description="Example capability.",
        input_schema=_strict_schema(
            {"value": {"type": "integer", "minimum": 1}}, ["value"]
        ),
        output_schema=_strict_schema({"value": {"type": "integer"}}, ["value"]),
        adapter_method="read",
    )


def _context_result(project_id="example"):
    """Return one schema-valid GCP context result."""
    return {
        "project_id": project_id,
        "credentials_configured": False,
        "credentials_file": None,
        "write_tools_enabled": False,
    }


def test_default_registry_exposes_strict_current_tools():
    """The MCP surface is represented by strict stable contracts."""
    assert {item.name for item in capability_registry.list()} == {
        "bigquery_list_datasets",
        "bigquery_list_tables",
        "bigquery_query",
        "bigquery_table_schema",
        "gcp_context",
        "gcs_list_objects",
        "gcs_object_metadata",
        "gcs_read_text",
    }
    for capability in capability_registry.list():
        assert capability.adapter_method
        assert capability.input_schema["additionalProperties"] is False
        assert capability.output_schema["additionalProperties"] is False
    json.dumps(capability_registry.schema())


def test_mcp_definitions_are_generated_from_registry():
    """MCP input and output schemas stay synchronized with the registry."""
    definitions = {item["name"]: item for item in tool_definitions()}
    assert set(definitions) == {item.name for item in capability_registry.list()}
    for capability in capability_registry.list():
        definition = definitions[capability.name]
        assert definition["inputSchema"] == capability.input_schema
        assert definition["outputSchema"] == capability.output_schema
        assert definition["annotations"]["readOnlyHint"] is True


def test_registry_rejects_duplicates_and_loose_contracts():
    """Names are unique and top-level schemas must reject unknown fields."""
    registry = CapabilityRegistry()
    capability = _example_capability()
    registry.register(capability)
    with pytest.raises(ValueError, match="already registered"):
        registry.register(capability)

    loose = replace(
        capability,
        name="loose",
        output_schema={"type": "object", "additionalProperties": True},
    )
    with pytest.raises(ValueError, match="reject unknown"):
        registry.register(loose)


def test_registry_binds_handlers_and_validates_values():
    """Bound handlers retain metadata and inputs and outputs are validated."""
    registry = CapabilityRegistry()
    registry.register(_example_capability())
    bound = registry.bind("example", lambda value: {"value": value})

    assert bound.handler is not None
    registry.validate_input("example", {"value": 2})
    registry.validate_output("example", {"value": 2})
    with pytest.raises(ValueError, match="minimum value"):
        registry.validate_input("example", {"value": 0})
    with pytest.raises(ValueError, match="unexpected property"):
        registry.validate_output("example", {"value": 2, "extra": True})


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
    assert SafetyLevel.READ_ONLY.value == "inspection"
    json.dumps(payload)


def test_execution_runtime_returns_stable_validated_envelope():
    """Runtime execution validates contracts and adds operational metadata."""
    result = execute_capability(
        capability_registry,
        "gcp_context",
        {},
        handler=lambda: _context_result(),
    )
    assert result.ok is True
    assert result.data == _context_result()
    assert result.metadata.service == "gcp"
    json.dumps(result.to_dict())


def test_execution_runtime_rejects_invalid_inputs_and_outputs():
    """Contract violations return stable machine-readable errors."""
    invalid_input = execute_capability(
        capability_registry,
        "gcp_context",
        {"unexpected": True},
        handler=lambda: _context_result(),
    )
    assert invalid_input.ok is False
    assert invalid_input.error.code == "input_validation_failed"

    invalid_output = execute_capability(
        capability_registry,
        "gcp_context",
        {},
        handler=lambda: {"project_id": "example"},
    )
    assert invalid_output.ok is False
    assert invalid_output.error.code == "output_validation_failed"


def test_secret_redaction_is_recursive():
    """Secret-bearing keys are removed before logging."""
    assert redact({"token": "secret", "nested": {"password": "secret", "safe": 1}}) == {
        "token": "[REDACTED]",
        "nested": {"password": "[REDACTED]", "safe": 1},
    }


def test_compatibility_snapshots_classify_contract_changes():
    """Generated snapshots distinguish additive and breaking changes."""
    registry = CapabilityRegistry()
    registry.register(_example_capability())
    before = compatibility_snapshot(registry)

    registry.register(_example_capability("second"))
    additive = compare_compatibility_snapshots(before, compatibility_snapshot(registry))
    assert additive["classification"] == "additive"

    changed = CapabilityRegistry()
    changed.register(
        replace(
            _example_capability(),
            input_schema=_strict_schema(
                {
                    "value": {"type": "integer", "minimum": 1},
                    "required_later": {"type": "string"},
                },
                ["value", "required_later"],
            ),
        )
    )
    breaking = compare_compatibility_snapshots(before, compatibility_snapshot(changed))
    assert breaking["classification"] == "breaking"


def test_generated_reference_and_readiness_are_release_friendly():
    """Generated reference and scorecard stay deterministic and serializable."""
    reference = capability_reference_markdown(capability_registry)
    assert "`gcp_context`" in reference
    assert "`gcs_read_text`" in reference

    scorecard = readiness_score(capability_registry)
    assert scorecard["ready"] is True
    assert scorecard["score"] == 100.0
    json.dumps(scorecard)
