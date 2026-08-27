"""Tests for documentation assets included in built distributions."""

from pathlib import Path

import pytest

from GoogleCloudPlatformAPI.assets import read_text_resource, resource_path

ROOT = Path(__file__).resolve().parents[1]


def test_packaged_assets_are_readable() -> None:
    """Expose the machine-readable index, skill, and platform documentation."""
    assert "gcp-api-eval" in read_text_resource("llms.txt")
    assert "maximum_bytes_billed" in read_text_resource("codex/SKILL.md")
    assert resource_path("docs/ai-native-platform.md").is_file()
    assert resource_path("docs/ai-readiness.md").is_file()


def test_packaged_ai_native_contract_matches_repository() -> None:
    """Keep installed AI-native resources synchronized with repository sources."""
    for relative_path in (
        "llms.txt",
        "AI_NATIVE_PLATFORM.yaml",
        "schemas/ai-native-platform.schema.json",
        "scripts/validate_ai_native_platform.py",
    ):
        assert read_text_resource(relative_path) == (ROOT / relative_path).read_text(
            encoding="utf-8"
        )


@pytest.mark.parametrize("path", ["", "../README.md", "/tmp/secret"])
def test_packaged_assets_reject_unsafe_paths(path: str) -> None:
    """Reject empty, absolute, and traversal paths."""
    with pytest.raises(ValueError, match="inside the package"):
        resource_path(path)
