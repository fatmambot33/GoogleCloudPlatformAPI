"""Command-line interface for deterministic AI release evidence."""

import argparse
import json
from pathlib import Path
from typing import Optional, Sequence

from GoogleCloudPlatformAPI.ai_native.evidence import (
    compare_scorecards,
    release_scorecard,
    write_release_evidence,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the release evidence argument parser."""
    parser = argparse.ArgumentParser(
        prog="gcp-api-eval",
        description="Generate deterministic AI readiness release evidence.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("build/ai-readiness"),
        help="Directory for JSON, Markdown, and JUnit evidence.",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=None,
        help="Optional prior scorecard used to reject metric regressions.",
    )
    parser.add_argument(
        "--fail-under",
        type=float,
        default=100.0,
        help="Minimum overall readiness score.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Generate evidence and return a non-zero status on regressions."""
    args = build_parser().parse_args(argv)
    scorecard = release_scorecard()
    paths = write_release_evidence(args.output, scorecard)
    regression = {"passed": True, "regressions": []}
    if args.baseline is not None:
        baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
        regression = compare_scorecards(baseline, scorecard)
    passed = (
        bool(scorecard["ready"])
        and float(scorecard["score"]) >= args.fail_under
        and bool(regression["passed"])
    )
    print(
        json.dumps(
            {
                "passed": passed,
                "score": scorecard["score"],
                "ready": scorecard["ready"],
                "regressions": regression["regressions"],
                "artifacts": {name: str(path) for name, path in paths.items()},
            },
            sort_keys=True,
        )
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
