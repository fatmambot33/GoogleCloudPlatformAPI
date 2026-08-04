"""Local Codex integration for GoogleCloudPlatformAPI.

The package exposes a small, dependency-free MCP server that runs over stdio.
"""

from .server import main

__all__ = ["main"]
