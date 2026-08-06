"""Tests for schema validation and generated capability surfaces."""

import hashlib
import json
from pathlib import Path

import pytest

from GoogleCloudPlatformAPI.agents.cli import main
from GoogleCloudPlatformAPI.ai_native import (
    SchemaValidationError,
    capability_reference_markdown,
    capability_registry,
    compatibility_snapshot,
    validate_json_schema,
)


def test_schema_validation_reports_nested_paths():
    """Nested contract errors identify the failing JSON location."""
    schema = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"name": {"type": "string", "minLength": 1}},
                    "required": ["name"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["items"],
        "additionalProperties": False,
    }

    with pytest.raises(SchemaValidationError, match=r"\$\.items\[0\]\.name"):
        validate_json_schema({"items": [{"name": ""}]}, schema)


def test_schema_validation_rejects_unknown_fields_and_bounds():
    """Strict schemas reject unknown fields and numeric overflow."""
    schema = {
        "type": "object",
        "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 3}},
        "required": ["limit"],
        "additionalProperties": False,
    }

    with pytest.raises(SchemaValidationError, match="unexpected property"):
        validate_json_schema({"limit": 2, "extra": True}, schema)
    with pytest.raises(SchemaValidationError, match="maximum value"):
        validate_json_schema({"limit": 4}, schema)


def test_agent_cli_lists_generated_capability_registry(capsys):
    """The CLI exposes the canonical registry without duplicating tool metadata."""
    assert main(["--list-capabilities"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["schema_version"] == "1.1.0"
    assert len(payload["capabilities"]) == 8
    assert all(item["adapter_method"] for item in payload["capabilities"])


def test_generated_contract_artifacts_are_current():
    """Committed human and machine references match the canonical registry."""
    root = Path(__file__).resolve().parents[1]
    assert (root / "docs/capabilities.md").read_text(encoding="utf-8") == (
        capability_reference_markdown(capability_registry)
    )
    payload = json.dumps(
        compatibility_snapshot(capability_registry),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    expected = (
        root / "tests/snapshots/capability-contracts.sha256"
    ).read_text(encoding="utf-8").strip()
    assert hashlib.sha256(payload).hexdigest() == expected
