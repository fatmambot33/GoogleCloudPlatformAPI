"""Tests for first-class resource retrieval methods."""

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from google.cloud.exceptions import NotFound

from GoogleCloudPlatformAPI import BigQuery, CloudStorage
from GoogleCloudPlatformAPI.AdManager import (
    AudienceService,
    CustomTargetingService,
    TargetingPresetService,
)


def _without_init(cls):
    """Create a helper instance without authenticating a provider client."""
    return object.__new__(cls)


def test_bigquery_resource_reads_delegate_to_client():
    helper = _without_init(BigQuery)
    helper._client = MagicMock()
    dataset = SimpleNamespace(dataset_id="events")
    table = SimpleNamespace(schema=[SimpleNamespace(name="id")])
    helper._client.get_dataset.return_value = dataset
    helper._client.get_table.return_value = table

    assert helper.get_dataset("project.events") is dataset
    assert helper.get_table("project.events.sessions") is table
    assert helper.get_table_schema("project.events.sessions") == table.schema
    assert helper.table_exists("project.events.sessions") is True

    helper._client.get_table.side_effect = NotFound("missing")
    assert helper.table_exists("project.events.missing") is False


def test_bigquery_discovery_preserves_provider_pagination():
    helper = _without_init(BigQuery)
    helper._client = MagicMock()
    datasets = MagicMock()
    tables = MagicMock()
    helper._client.list_datasets.return_value = datasets
    helper._client.list_tables.return_value = tables

    assert (
        helper.list_datasets(
            project="project", max_results=25, page_token="datasets-next"
        )
        is datasets
    )
    helper._client.list_datasets.assert_called_once_with(
        project="project", max_results=25, page_token="datasets-next"
    )
    assert (
        helper.list_tables(
            "project.events", max_results=50, page_token="tables-next"
        )
        is tables
    )
    helper._client.list_tables.assert_called_once_with(
        "project.events", max_results=50, page_token="tables-next"
    )


def test_cloud_storage_resource_reads_are_typed_and_json_compatible():
    helper = _without_init(CloudStorage)
    helper._client = MagicMock()
    bucket = MagicMock()
    blob = SimpleNamespace(
        size=12,
        content_type="application/json",
        generation=3,
        updated=datetime.datetime(2026, 9, 1, 10, 0),
        md5_hash="hash",
    )
    helper._client.bucket.return_value = bucket
    bucket.get_blob.return_value = blob

    assert helper.get_object("bucket", "reports/latest.json") is blob
    assert helper.object_exists("bucket", "reports/latest.json") is True
    assert helper.get_object_metadata("bucket", "reports/latest.json") == {
        "bucket_name": "bucket",
        "object_name": "reports/latest.json",
        "size": 12,
        "content_type": "application/json",
        "generation": 3,
        "updated": "2026-09-01T10:00:00",
        "md5_hash": "hash",
    }

    bucket.get_blob.return_value = None
    assert helper.object_exists("bucket", "missing.json") is False


def test_cloud_storage_list_objects_preserves_provider_pagination():
    helper = _without_init(CloudStorage)
    helper._client = MagicMock()
    iterator = MagicMock()
    helper._client.list_blobs.return_value = iterator

    assert (
        helper.list_objects(
            "bucket", prefix="reports/", max_results=10, page_token="next"
        )
        is iterator
    )
    helper._client.list_blobs.assert_called_once_with(
        "bucket", prefix="reports/", max_results=10, page_token="next"
    )


def test_ad_manager_services_get_single_resources():
    audience = _without_init(AudienceService)
    audience._gam_service = MagicMock()
    audience._gam_service.getAudienceSegmentsByStatement.return_value = {
        "results": [{"id": 1, "name": "Audience"}]
    }
    assert audience.get(1) == {"id": 1, "name": "Audience"}

    targeting = _without_init(CustomTargetingService)
    targeting._gam_service = MagicMock()
    targeting._gam_service.getCustomTargetingValuesByStatement.return_value = {
        "results": [{"customTargetingKeyId": 2, "id": 3, "name": "Value"}]
    }
    assert targeting.get(2, 3)["id"] == 3

    preset = _without_init(TargetingPresetService)
    preset._gam_service = MagicMock()
    preset._gam_service.getTargetingPresetsByStatement.return_value = {
        "results": [{"id": 4, "name": "Preset", "targeting": {}}]
    }
    assert preset.get(4)["id"] == 4


def test_ad_manager_get_returns_none_for_missing_resources():
    audience = _without_init(AudienceService)
    audience._gam_service = MagicMock()
    audience._gam_service.getAudienceSegmentsByStatement.return_value = {}

    targeting = _without_init(CustomTargetingService)
    targeting._gam_service = MagicMock()
    targeting._gam_service.getCustomTargetingValuesByStatement.return_value = {
        "results": []
    }

    preset = _without_init(TargetingPresetService)
    preset._gam_service = MagicMock()
    preset._gam_service.getTargetingPresetsByStatement.return_value = None

    assert audience.get(99) is None
    assert targeting.get(2, 99) is None
    assert preset.get(99) is None
