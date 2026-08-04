"""Dependency-free MCP stdio server for local Codex use."""

import json
import sys
from typing import Any, Dict, Optional, TextIO

from .tools import CodexTools, text_content, tool_definitions

_PROTOCOL_VERSION = "2025-06-18"


class MCPServer:
    """Minimal JSON-RPC MCP server running over newline-delimited stdio."""

    def __init__(self, tools: Optional[CodexTools] = None) -> None:
        self._tools = tools or CodexTools()

    def handle(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle one MCP JSON-RPC request or notification."""
        request_id = request.get("id")
        method = request.get("method")
        params = request.get("params") or {}

        if request_id is None:
            return None
        try:
            if method == "initialize":
                result = {
                    "protocolVersion": params.get("protocolVersion", _PROTOCOL_VERSION),
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {
                        "name": "google-cloud-platform-api",
                        "version": "2.4.0",
                    },
                }
            elif method == "ping":
                result = {}
            elif method == "tools/list":
                result = {"tools": tool_definitions()}
            elif method == "tools/call":
                payload = self._tools.call(
                    str(params.get("name", "")), params.get("arguments") or {}
                )
                result = {
                    "content": text_content(payload),
                    "structuredContent": payload,
                }
            else:
                return self._error(request_id, -32601, "Method not found")
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except (TypeError, ValueError) as exc:
            return self._error(request_id, -32602, str(exc))
        except Exception as exc:  # pragma: no cover - defensive protocol boundary
            return self._error(
                request_id, -32603, "Tool execution failed: {0}".format(exc)
            )

    @staticmethod
    def _error(request_id: Any, code: int, message: str) -> Dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }

    def run(self, stdin: TextIO, stdout: TextIO) -> None:
        """Read newline-delimited JSON-RPC messages until stdin closes."""
        for line in stdin:
            if not line.strip():
                continue
            try:
                request = json.loads(line)
                response = self.handle(request)
            except (TypeError, ValueError) as exc:
                response = self._error(None, -32700, "Parse error: {0}".format(exc))
            if response is not None:
                stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
                stdout.flush()


def main() -> None:
    """Run the local Codex MCP server over stdin and stdout."""
    MCPServer().run(sys.stdin, sys.stdout)


if __name__ == "__main__":
    main()
