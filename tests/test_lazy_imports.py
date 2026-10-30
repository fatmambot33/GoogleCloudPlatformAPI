"""Regression tests for lightweight package imports."""

import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run_import_probe(source: str) -> None:
    """Run an isolated Python import probe against the source tree."""
    environment = os.environ.copy()
    python_path = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        f"{PROJECT_ROOT}{os.pathsep}{python_path}" if python_path else str(PROJECT_ROOT)
    )
    result = subprocess.run(
        [sys.executable, "-c", source],
        capture_output=True,
        env=environment,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_package_root_does_not_import_service_stacks():
    """Importing the package root must not initialize client dependencies."""
    _run_import_probe(
        """
import sys
import GoogleCloudPlatformAPI as gcp

def imported(package):
    return any(
        name == package or name.startswith(f"{package}.")
        for name in sys.modules
    )

assert "Analytics" in dir(gcp)
assert not imported("googleads")
assert not imported("pandas")
assert not imported("googleapiclient")
assert not imported("google.cloud.bigquery")
assert not imported("google.cloud.storage")
"""
    )


def test_analytics_does_not_import_ad_manager_stack():
    """Analytics access must not load the independent Ad Manager client."""
    _run_import_probe(
        """
import sys
from GoogleCloudPlatformAPI import Analytics

def imported(package):
    return any(
        name == package or name.startswith(f"{package}.")
        for name in sys.modules
    )

assert Analytics.__name__ == "Analytics"
assert not imported("GoogleCloudPlatformAPI.AdManager")
assert not imported("googleads")
"""
    )
