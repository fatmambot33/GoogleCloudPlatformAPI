"""Tests for discovery-first Codex tools."""

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from GoogleCloudPlatformAPI.codex.tools import CodexTools


def test_bigquery_discovery_is_bounded_and_structured():
    client = MagicMock()
    client.list_datasets.return_value = [
        SimpleNamespace(
            dataset_id="events",
            project="project",
            full_dataset_id="project:events",
        ),
        SimpleNamespace(
            dataset_id="archive",
            project="project",
            full_dataset_id="project:archive",
        ),
    ]
    client.list_tables.return_value = [
        SimpleNamespace(
            table_id="sessions",
            table_type="TABLE",
            full_table_id="project:events.sessions",
        )
    ]
    client.get_table.return_value = SimpleNamespace(
        full_table_id="project:events.sessions",
        table_type="TABLE",
        description="Sessions",
        num_rows=10,
        num_bytes=100,
        time_partitioning=None,
        schema=[
            SimpleNamespace(
                name="session_id",
                field_type="STRING",
                mode="REQUIRED",
                description="Identifier",
            )
        ],
    )
    bigquery = SimpleNamespace(_client=client)
    tools = CodexTools(bigquery_factory=lambda: bigquery)

    datasets = tools.bigquery_list_datasets(max_results=1)
    tables = tools.bigquery_list_tables("project.events")
    schema = tools.bigquery_table_schema("project.events.sessions")

    assert datasets["returned_datasets"] == 1
    assert datasets["truncated"] is True
    assert tables["tables"][0]["table_id"] == "sessions"
    assert schema["fields"][0]["name"] == "session_id"
    with pytest.raises(ValueError, match="between 1 and 1000"):
        tools.bigquery_list_datasets(max_results=0)


def test_storage_metadata_is_structured_and_missing_objects_are_clear():
    blob = SimpleNamespace(
        size=12,
        content_type="application/json",
        generation=3,
        updated=datetime.datetime(2026, 8, 5, 12, 0),
        md5_hash="hash",
    )
    bucket = MagicMock()
    bucket.get_blob.return_value = blob
    storage = SimpleNamespace(_client=MagicMock())
    storage._client.bucket.return_value = bucket
    tools = CodexTools(storage_factory=lambda: storage)

    metadata = tools.storage_object_metadata("bucket", "reports/latest.json")

    assert metadata["size"] == 12
    assert metadata["updated"] == "2026-08-05T12:00:00"
    bucket.get_blob.return_value = None
    with pytest.raises(ValueError, match="Object not found"):
        tools.storage_object_metadata("bucket", "missing.json")


def test_dispatch_includes_discovery_tools(monkeypatch):
    tools = CodexTools()
    monkeypatch.setattr(tools, "bigquery_list_datasets", lambda **kwargs: kwargs)
    monkeypatch.setattr(tools, "bigquery_list_tables", lambda **kwargs: kwargs)
    monkeypatch.setattr(tools, "bigquery_table_schema", lambda **kwargs: kwargs)
    monkeypatch.setattr(tools, "storage_object_metadata", lambda **kwargs: kwargs)

    assert tools.call("bigquery_list_datasets", {"max_results": 10}) == {
        "max_results": 10
    }
    assert tools.call("bigquery_list_tables", {"dataset_id": "events"}) == {
        "dataset_id": "events"
    }
    assert tools.call("bigquery_table_schema", {"table_id": "events.sessions"}) == {
        "table_id": "events.sessions"
    }
    assert tools.call(
        "gcs_object_metadata",
        {"bucket_name": "bucket", "object_name": "object"},
    ) == {"bucket_name": "bucket", "object_name": "object"}
