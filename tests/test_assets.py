"""Tests for documentation assets included in built distributions."""

import pytest

from GoogleCloudPlatformAPI.assets import read_text_resource, resource_path


def test_packaged_assets_are_readable() -> None:
    """Expose the machine-readable index, skill, and platform documentation."""
    assert "GoogleCloudPlatformAPI" in read_text_resource("llms.txt")
    assert "read-only" in read_text_resource("codex/SKILL.md")
    assert resource_path("docs/ai-native-platform.md").is_file()


@pytest.mark.parametrize("path", ["", "../README.md", "/tmp/secret"])
def test_packaged_assets_reject_unsafe_paths(path: str) -> None:
    """Reject empty, absolute, and traversal paths."""
    with pytest.raises(ValueError, match="inside the package"):
        resource_path(path)
