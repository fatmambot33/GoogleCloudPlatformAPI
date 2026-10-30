"""Tests for the stable package-level public API."""

import json
import subprocess
import sys

import GoogleCloudPlatformAPI as gcp


def test_core_services_are_available_from_the_package_root():
    assert gcp.BigQuery.__name__ == "BigQuery"
    assert gcp.CloudStorage.__name__ == "CloudStorage"
    assert gcp.Analytics.__name__ == "Analytics"
    assert gcp.ServiceAccount.__name__ == "ServiceAccount"


def test_exception_hierarchy_is_stable_and_serializable():
    error = gcp.AuthenticationError(
        "Credentials were rejected.",
        operation="bigquery.query",
        details={"reason": "invalid"},
    )

    assert isinstance(error, gcp.GoogleCloudPlatformAPIError)
    assert issubclass(gcp.ConfigurationError, gcp.GoogleCloudPlatformAPIError)
    assert issubclass(gcp.TransportError, gcp.GoogleCloudPlatformAPIError)
    assert issubclass(gcp.ServiceError, gcp.GoogleCloudPlatformAPIError)
    assert error.to_dict() == {
        "type": "AuthenticationError",
        "message": "Credentials were rejected.",
        "operation": "bigquery.query",
        "details": {"reason": "invalid"},
    }
    json.dumps(error.to_dict())


def test_analytics_import_does_not_load_ad_manager() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "from GoogleCloudPlatformAPI import Analytics; "
                "assert Analytics.__name__ == 'Analytics'; "
                "assert 'GoogleCloudPlatformAPI.AdManager' not in sys.modules"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_package_export_remains_a_class_after_direct_module_import() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import GoogleCloudPlatformAPI.BigQuery; "
                "import GoogleCloudPlatformAPI as gcp; "
                "assert gcp.BigQuery.__name__ == 'BigQuery'"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
