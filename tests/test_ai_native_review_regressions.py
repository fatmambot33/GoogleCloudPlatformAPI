"""Aggregate regression assertions for AI-native review findings."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_review_regression_assets_exist() -> None:
    """Keep the review-fix tests and contract artifacts present together."""
    expected = (
        "AI_NATIVE_PLATFORM.yaml",
        "CHANGELOG.md",
        "llms.txt",
        "scripts/validate_ai_native_platform.py",
        "tests/test_ai_native_platform_validator.py",
    )

    assert all((ROOT / path).exists() for path in expected)
