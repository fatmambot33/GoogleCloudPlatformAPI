"""Contract truthfulness tests for the repository AI-native manifest."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_autonomy_declarations_match_repository_automation() -> None:
    """Issue discovery should not claim autonomous PR or CI execution."""
    manifest = yaml.safe_load(
        (ROOT / "AI_NATIVE_PLATFORM.yaml").read_text(encoding="utf-8")
    )
    autonomous = manifest["self_improvement"]["autonomous"]

    assert autonomous["discover_improvements"] is True
    assert autonomous["create_issues"] is True
    assert autonomous["generate_pr"] is False
    assert autonomous["run_ci"] is False
