"""Access documentation assets packaged with the distribution."""

from importlib.resources import files
from pathlib import PurePosixPath
from typing import Any


def resource_path(relative_path: str) -> Any:
    """Return a validated path to one packaged documentation asset."""
    path = PurePosixPath(relative_path)
    if not relative_path or path.is_absolute() or ".." in path.parts:
        raise ValueError("Asset paths must be relative and remain inside the package.")
    return files("GoogleCloudPlatformAPI.assets").joinpath(*path.parts)


def read_text_resource(relative_path: str, encoding: str = "utf-8") -> str:
    """Read one packaged text asset."""
    return resource_path(relative_path).read_text(encoding=encoding)


__all__ = ["read_text_resource", "resource_path"]
