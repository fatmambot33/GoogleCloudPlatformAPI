"""Stable opaque cursors for bounded discovery operations."""

import base64
import json
from typing import Dict, Optional

_CURSOR_VERSION = 1


class CursorError(ValueError):
    """Report an invalid or context-mismatched pagination cursor."""


def encode_cursor(
    service: str,
    operation: str,
    page_token: Optional[str],
    context: Dict[str, str],
) -> Optional[str]:
    """Encode a provider page token with operation context."""
    if not page_token:
        return None
    payload = {
        "version": _CURSOR_VERSION,
        "service": service,
        "operation": operation,
        "page_token": page_token,
        "context": dict(sorted(context.items())),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(
    cursor: Optional[str],
    service: str,
    operation: str,
    context: Dict[str, str],
) -> Optional[str]:
    """Decode and validate a cursor for one operation and resource context."""
    if not cursor:
        return None
    try:
        padding = "=" * (-len(cursor) % 4)
        payload = json.loads(
            base64.urlsafe_b64decode((cursor + padding).encode("ascii")).decode("utf-8")
        )
    except (ValueError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
        raise CursorError("Cursor is not valid opaque pagination state.") from exc
    expected_context = dict(sorted(context.items()))
    if (
        payload.get("version") != _CURSOR_VERSION
        or payload.get("service") != service
        or payload.get("operation") != operation
        or payload.get("context") != expected_context
        or not isinstance(payload.get("page_token"), str)
    ):
        raise CursorError("Cursor does not match this capability request.")
    return payload["page_token"]
