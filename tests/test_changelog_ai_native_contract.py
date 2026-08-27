"""Changelog coverage for the AI-native repository contract."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_unreleased_documents_ai_native_contract() -> None:
    """The public AI-native contract changes should remain release-noted."""
    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    unreleased = changelog.split("## 2.8.1", maxsplit=1)[0]

    assert "AI-native" in unreleased
    assert "`ci`, `validate`" in unreleased
    assert "vendored schema" in unreleased
