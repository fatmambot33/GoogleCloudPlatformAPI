"""Command-line runner for configured agent plugins."""

import argparse
import json
from typing import Any, Dict, Optional, Sequence

from .core import Agent
from .loading import load_plugins


def _json_object(value: str) -> Dict[str, Any]:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("context must be a JSON object")
    return parsed


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run configured plugins and print the final context as JSON."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--plugin",
        action="append",
        dest="plugins",
        help="Plugin reference package.module:attribute; repeatable.",
    )
    parser.add_argument(
        "--context", type=_json_object, default={}, help="Initial JSON object."
    )
    parser.add_argument(
        "--only", action="append", help="Registered plugin name to run; repeatable."
    )
    args = parser.parse_args(argv)
    result = Agent(load_plugins(args.plugins)).run(args.context, args.only)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
