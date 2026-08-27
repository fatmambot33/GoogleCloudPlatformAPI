"""Regression tests for the repository AI-native platform validator."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts import validate_ai_native_platform as validator


def load_manifest() -> dict[str, Any]:
    """Load a fresh copy of the repository AI-native manifest."""
    data = yaml.safe_load(validator.MANIFEST.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


@pytest.mark.parametrize("guarantees", [42, [["unhashable"]]])
def test_malformed_guarantees_return_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, guarantees: object
) -> None:
    """Malformed guarantee values should report schema errors, not traceback."""
    data = load_manifest()
    data["agent"]["guarantees"] = guarantees
    manifest = tmp_path / "AI_NATIVE_PLATFORM.yaml"
    manifest.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    monkeypatch.setattr(validator, "MANIFEST", manifest)

    errors = validator.validate()

    assert errors
    assert any(error.startswith("schema [agent.guarantees") for error in errors)


def test_schema_drift_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A valid but byte-different vendored schema should fail pin verification."""
    schema = tmp_path / "ai-native-platform.schema.json"
    schema.write_bytes(validator.SCHEMA.read_bytes() + b"\n")
    monkeypatch.setattr(validator, "SCHEMA", schema)

    assert "vendored schema does not match standard.ref" in validator.validate()


def test_vendored_schema_matches_pinned_standard() -> None:
    """The committed schema should match the trusted blob for its standard ref."""
    data = load_manifest()
    reference = data["standard"]["ref"]

    assert validator.git_blob_sha(validator.SCHEMA.read_bytes()) == (
        validator.PINNED_SCHEMA_BLOBS[reference]
    )
