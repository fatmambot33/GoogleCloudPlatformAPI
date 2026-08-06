"""Tests for generated AI readiness release evidence."""

import json
from pathlib import Path

from GoogleCloudPlatformAPI.ai_native import (
    compare_scorecards,
    junit_xml,
    release_scorecard,
    scorecard_markdown,
    write_release_evidence,
)
from GoogleCloudPlatformAPI.ai_native.eval_cli import main


def test_release_evidence_is_deterministic_and_serializable():
    """Repeated generation produces the same machine-readable scorecard."""
    first = release_scorecard()
    second = release_scorecard()
    assert first == second
    assert json.loads(json.dumps(first)) == first


def test_scorecard_formats_include_metrics_and_checks():
    """Human and JUnit formats are generated from the same result payload."""
    scorecard = release_scorecard()
    markdown = scorecard_markdown(scorecard)
    junit = junit_xml(scorecard)
    assert "AI Readiness Scorecard" in markdown
    assert "Golden scenarios" in markdown
    assert '<testsuite name="ai-readiness"' in junit
    assert junit.count("<testcase") == scorecard["total"]


def test_write_release_evidence_creates_all_artifacts(tmp_path: Path):
    """CI receives JSON, Markdown, and JUnit evidence from one command."""
    paths = write_release_evidence(tmp_path)
    assert set(paths) == {"json", "markdown", "junit"}
    assert all(path.is_file() for path in paths.values())
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["ready"] is True


def test_scorecard_comparison_detects_metric_regressions():
    """A lower category rate or overall score is a release regression."""
    baseline = release_scorecard()
    current = dict(baseline)
    current["score"] = 99.0
    current["metrics"] = dict(baseline["metrics"])
    current["metrics"]["safety"] = 90.0
    comparison = compare_scorecards(baseline, current)
    assert comparison["passed"] is False
    assert any("safety" in item for item in comparison["regressions"])
    assert any("score" in item for item in comparison["regressions"])


def test_evaluation_cli_passes_and_writes_evidence(tmp_path: Path):
    """The command-line release gate exits successfully at 100 percent."""
    output = tmp_path / "evidence"
    assert main(["--output", str(output), "--fail-under", "100"]) == 0
    assert (output / "scorecard.json").is_file()


def test_evaluation_cli_rejects_impossible_threshold(tmp_path: Path):
    """A threshold above the deterministic score fails the command."""
    assert (
        main(
            [
                "--output",
                str(tmp_path / "evidence"),
                "--fail-under",
                "101",
            ]
        )
        == 1
    )
