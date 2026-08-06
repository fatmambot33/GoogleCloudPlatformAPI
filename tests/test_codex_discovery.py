"""Tests for discovery-first Codex tools."""

import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from GoogleCloudPlatformAPI.codex.tools import CodexTools


def test_bigquery_discovery_is_bounded_and_structured():
    client = MagicMock()
    datasets_iterator = MagicMock()
    datasets_iterator.__iter__.return_value = iter(
        [
            SimpleNamespace(
                dataset_id="events",
                project="project",
                full_dataset_id="project:events",
            ),
        ]
    )
    datasets_iterator.next_page_token = "next-datasets"
    client.list_datasets.return_value = datasets_iterator
    tables_iterator = MagicMock()
    tables_iterator.__iter__.return_value = iter(
        [
            SimpleNamespace(
                table_id="sessions",
                table_type="TABLE",
                full_table_id="project:events.sessions",
            )
        ]
    )
    tables_iterator.next_page_token = None
    client.list_tables.return_value = tables_iterator
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
    assert datasets["next_cursor"]
    assert tables["tables"][0]["table_id"] == "sessions"
    assert tables["next_cursor"] is None
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
    datasets = {
        "datasets": [],
        "returned_datasets": 0,
        "truncated": False,
        "next_cursor": None,
    }
    tables = {
        "dataset_id": "events",
        "tables": [],
        "returned_tables": 0,
        "truncated": False,
        "next_cursor": None,
    }
    schema = {
        "table_id": "events.sessions",
        "table_type": "TABLE",
        "description": None,
        "num_rows": 0,
        "num_bytes": 0,
        "partitioning": None,
        "fields": [],
    }
    metadata = {
        "bucket_name": "bucket",
        "object_name": "object",
        "size": 0,
        "content_type": None,
        "generation": None,
        "updated": None,
        "md5_hash": None,
    }
    monkeypatch.setattr(tools, "bigquery_list_datasets", lambda **kwargs: datasets)
    monkeypatch.setattr(tools, "bigquery_list_tables", lambda **kwargs: tables)
    monkeypatch.setattr(tools, "bigquery_table_schema", lambda **kwargs: schema)
    monkeypatch.setattr(tools, "storage_object_metadata", lambda **kwargs: metadata)

    assert tools.call("bigquery_list_datasets", {"max_results": 10}) == datasets
    assert tools.call("bigquery_list_tables", {"dataset_id": "events"}) == tables
    assert (
        tools.call("bigquery_table_schema", {"table_id": "events.sessions"}) == schema
    )
    assert (
        tools.call(
            "gcs_object_metadata",
            {"bucket_name": "bucket", "object_name": "object"},
        )
        == metadata
    )
