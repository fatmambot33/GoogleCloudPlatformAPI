"""Tests for the local Codex MCP surface."""

import json
from io import StringIO

import pytest

from GoogleCloudPlatformAPI.codex.server import MCPServer
from GoogleCloudPlatformAPI.codex.tools import CodexTools


class FakeBigQuery:
    def execute_query(self, query):
        return [{"answer": 42}, {"answer": 43}]


class FakeBlob:
    def download_as_bytes(self):
        return b"hello world"


class FakeBucket:
    def blob(self, name):
        return FakeBlob()


class FakeStorageClient:
    def bucket(self, name):
        return FakeBucket()


class FakeStorage:
    _client = FakeStorageClient()

    def list_files(self, bucket_name, prefix):
        return [prefix + "a.txt", prefix + "b.txt"]


def tools():
    return CodexTools(lambda: FakeBigQuery(), lambda: FakeStorage())


def test_bigquery_is_read_only():
    with pytest.raises(ValueError):
        tools().bigquery_query("DELETE FROM dataset.table")


def test_read_tools_return_structured_results():
    assert tools().bigquery_query("SELECT 42")["rows"][0]["answer"] == 42
    assert tools().storage_list("bucket", "data/")["objects"] == [
        "data/a.txt",
        "data/b.txt",
    ]
    assert tools().storage_read_text("bucket", "hello.txt")["text"] == "hello world"


def test_server_lists_and_calls_tools():
    server = MCPServer(tools())
    listed = server.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert len(listed["result"]["tools"]) == 4
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
