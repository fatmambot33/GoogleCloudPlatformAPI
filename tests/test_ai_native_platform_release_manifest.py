"""Release-manifest integration assertions."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_release_claim_matches_enforced_workflow() -> None:
    """A declared quality block should be backed by the reusable CI gate."""
    manifest = yaml.safe_load(
        (ROOT / "AI_NATIVE_PLATFORM.yaml").read_text(encoding="utf-8")
    )
    release_workflow = (ROOT / ".github/workflows/python-publish.yml").read_text(
        encoding="utf-8"
    )

    assert manifest["release"]["block_if_quality_fails"] is True
    assert "uses: ./.github/workflows/ci.yml" in release_workflow
