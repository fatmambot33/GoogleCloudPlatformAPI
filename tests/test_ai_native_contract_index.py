"""Documentation index coverage for the AI-native repository contract."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_llms_index_exposes_ai_native_contract() -> None:
    """Agents should discover the manifest, vendored schema, and validator."""
    index = (ROOT / "llms.txt").read_text(encoding="utf-8")

    assert "AI_NATIVE_PLATFORM.yaml" in index
    assert "schemas/ai-native-platform.schema.json" in index
    assert "scripts/validate_ai_native_platform.py" in index
