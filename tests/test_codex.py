"""Tests for the local Codex MCP surface."""

import json
from io import StringIO
from types import SimpleNamespace

import pytest

from GoogleCloudPlatformAPI.codex.server import MCPServer
from GoogleCloudPlatformAPI.codex.tools import CodexTools


class FakeIterator(list):
    """List-like result page with provider pagination metadata."""

    next_page_token = None
    total_rows = 2


class FakeDryJob:
    """Completed BigQuery dry-run metadata."""

    total_bytes_processed = 10
    statement_type = "SELECT"


class FakeQueryJob:
    """Completed BigQuery query metadata and rows."""

    total_bytes_processed = 10
    total_bytes_billed = 10
    cache_hit = False
    job_id = "job-1"

    def result(self, **kwargs):
        return FakeIterator([{"answer": 42}, {"answer": 43}])

    def cancel(self, **kwargs):
        return True


class FakeBigQueryClient:
    """Return one dry run followed by one executed query."""

    def __init__(self):
        self.calls = 0

    def query(self, query, **kwargs):
        self.calls += 1
        return FakeDryJob() if self.calls % 2 else FakeQueryJob()


class FakeBigQuery:
    """Minimal BigQuery helper used by Codex adapter tests."""

    _client = FakeBigQueryClient()


class FakeBlob:
    """Minimal ranged Cloud Storage blob."""

    def download_as_bytes(self, **kwargs):
        return b"hello world"


class FakeBucket:
    """Minimal Cloud Storage bucket."""

    def blob(self, name):
        return FakeBlob()


class FakeStorageClient:
    """Minimal Cloud Storage client with bounded listing."""

    def bucket(self, name):
        return FakeBucket()

    def list_blobs(self, bucket_name, **kwargs):
        prefix = kwargs.get("prefix", "")
        return FakeIterator(
            [
                SimpleNamespace(name=prefix + "a.txt"),
                SimpleNamespace(name=prefix + "b.txt"),
            ]
        )


class FakeStorage:
    """Minimal Cloud Storage helper."""

    _client = FakeStorageClient()


def tools():
    return CodexTools(lambda: FakeBigQuery(), lambda: FakeStorage())


def test_bigquery_is_read_only():
    with pytest.raises(ValueError):
        tools().bigquery_query("DELETE FROM dataset.table")


def test_read_tools_return_structured_results():
    query = tools().bigquery_query("SELECT 42")
    assert query["rows"][0]["answer"] == 42
    assert query["dry_run_bytes_processed"] == 10
    assert tools().storage_list("bucket", "data/")["objects"] == [
        "data/a.txt",
        "data/b.txt",
    ]
    assert tools().storage_read_text("bucket", "hello.txt")["text"] == "hello world"


def test_server_lists_and_calls_tools():
    server = MCPServer(tools())
    listed = server.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert len(listed["result"]["tools"]) == 8
    called = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "bigquery_query", "arguments": {"query": "SELECT 42"}},
        }
    )
    assert called["result"]["structuredContent"]["returned_rows"] == 2


def test_server_stdio_smoke():
    stdin = StringIO(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"}) + "\n")
    stdout = StringIO()
    MCPServer(tools()).run(stdin, stdout)
    assert json.loads(stdout.getvalue())["result"] == {}
