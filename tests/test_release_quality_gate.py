"""Release workflow contract tests."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_release_reuses_full_ci_gate() -> None:
    """Publishing must depend on the same full CI workflow required for PRs."""
    ci_workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    release_workflow = (ROOT / ".github/workflows/python-publish.yml").read_text(
        encoding="utf-8"
    )

    assert "  workflow_call:" in ci_workflow
    assert "quality-gate:" in release_workflow
    assert "uses: ./.github/workflows/ci.yml" in release_workflow
    assert "needs: quality-gate" in release_workflow
