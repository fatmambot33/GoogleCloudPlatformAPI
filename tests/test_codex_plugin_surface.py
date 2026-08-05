"""Tests for the focused Codex plugin surface."""

from types import SimpleNamespace

import pytest

from GoogleCloudPlatformAPI.codex.tools import CodexTools, tool_definitions


class _BigQueryClient:
    def list_datasets(self, max_results):
        return [
            SimpleNamespace(dataset_id="analytics", project="demo", full_dataset_id="demo:analytics"),
            SimpleNamespace(dataset_id="warehouse", project="demo", full_dataset_id="demo:warehouse"),
        ]

    def list_tables(self, dataset_id, max_results):
        assert dataset_id == "demo.analytics"
        return [
            SimpleNamespace(table_id="events", table_type="TABLE", full_table_id="demo:analytics.events")
        ]

    def get_table(self, table_id):
        assert table_id == "demo.analytics.events"
        field = SimpleNamespace(
            name="event_name", field_type="STRING", mode="NULLABLE", description="Event label"
        )
        return SimpleNamespace(
            full_table_id="demo:analytics.events",
            table_type="TABLE",
            description="Events",
            num_rows=12,
            num_bytes=256,
            time_partitioning=None,
            schema=[field],
        )


class _BigQuery:
    def __init__(self):
        self._client = _BigQueryClient()

    def execute_query(self, query):
        assert query.startswith("SELECT")
        return [{"value": 1}, {"value": 2}]


class _Blob:
    size = 11
    content_type = "text/plain"
    generation = 7
    updated = None
    md5_hash = "abc"

    def download_as_bytes(self):
        return b"hello world"


class _Bucket:
    def get_blob(self, object_name):
        return _Blob() if object_name == "hello.txt" else None

    def blob(self, object_name):
        assert object_name == "hello.txt"
        return _Blob()


class _StorageClient:
    def bucket(self, bucket_name):
        assert bucket_name == "demo"
        return _Bucket()


class _Storage:
    def __init__(self):
        self._client = _StorageClient()

    def list_files(self, bucket_name, prefix):
        assert bucket_name == "demo"
        assert prefix == "reports/"
        return ["reports/a.json", "reports/b.json"]


def _tools():
    return CodexTools(bigquery_factory=_BigQuery, storage_factory=_Storage)


def test_tool_catalog_is_small_and_read_only():
    names = [item["name"] for item in tool_definitions()]
    assert names == [
        "gcp_context",
        "bigquery_list_datasets",
        "bigquery_list_tables",
        "bigquery_table_schema",
        "bigquery_query",
        "gcs_list_objects",
        "gcs_object_metadata",
        "gcs_read_text",
    ]
    assert not any(token in name for name in names for token in ("create", "update", "delete", "write"))


def test_bigquery_discovery_and_schema():
    tools = _tools()
    assert tools.bigquery_list_datasets()["returned_datasets"] == 2
    assert tools.bigquery_list_tables("demo.analytics")["tables"][0]["table_id"] == "events"
    schema = tools.bigquery_table_schema("demo.analytics.events")
    assert schema["fields"][0]["name"] == "event_name"


def test_query_and_storage_are_bounded():
    tools = _tools()
    assert tools.bigquery_query("SELECT 1", max_rows=1)["truncated"] is True
    assert tools.storage_list("demo", "reports/", max_results=1)["truncated"] is True
    assert tools.storage_read_text("demo", "hello.txt", max_bytes=5)["text"] == "hello"


def test_rejects_mutating_sql_and_missing_objects():
    tools = _tools()
    with pytest.raises(ValueError, match="Only SELECT"):
        tools.bigquery_query("DELETE FROM demo.analytics.events")
    with pytest.raises(ValueError, match="Object not found"):
        tools.storage_object_metadata("demo", "missing.txt")
