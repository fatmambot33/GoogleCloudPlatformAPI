"""Conservative SQL validation for billable BigQuery reads."""

import re
from typing import List

_ALLOWED_PREFIXES = {"select", "with", "explain"}
_DENIED_KEYWORDS = {
    "alter",
    "call",
    "create",
    "delete",
    "drop",
    "execute",
    "export",
    "grant",
    "insert",
    "load",
    "merge",
    "revoke",
    "truncate",
    "update",
}


class ReadOnlyQueryError(ValueError):
    """Report SQL that is not a single conservative read statement."""


def _code_characters(query: str) -> str:
    """Replace quoted strings and comments while preserving executable code."""
    output: List[str] = []
    index = 0
    state = "code"
    quote = ""
    while index < len(query):
        char = query[index]
        next_char = query[index + 1] if index + 1 < len(query) else ""
        if state == "line_comment":
            if char == "\n":
                state = "code"
                output.append(char)
            else:
                output.append(" ")
        elif state == "block_comment":
            if char == "*" and next_char == "/":
                output.extend((" ", " "))
                index += 1
                state = "code"
            else:
                output.append(" ")
        elif state == "quote":
            output.append(" ")
            if char == "\\" and next_char:
                output.append(" ")
                index += 1
            elif char == quote:
                if next_char == quote:
                    output.append(" ")
                    index += 1
                else:
                    state = "code"
        elif char == "-" and next_char == "-":
            output.extend((" ", " "))
            index += 1
            state = "line_comment"
        elif char == "/" and next_char == "*":
            output.extend((" ", " "))
            index += 1
            state = "block_comment"
        elif char in {"'", '"', "`"}:
            quote = char
            output.append(" ")
            state = "quote"
        else:
            output.append(char)
        index += 1
    if state in {"block_comment", "quote"}:
        raise ReadOnlyQueryError("Query contains an unterminated quote or comment.")
    return "".join(output)


def validate_single_read_query(query: str) -> str:
    """Validate one conservative BigQuery read statement and return it stripped."""
    if not isinstance(query, str) or not query.strip():
        raise ReadOnlyQueryError("Query must be a non-empty SQL string.")
    code = _code_characters(query)
    semicolons = [index for index, char in enumerate(code) if char == ";"]
    if len(semicolons) > 1:
        raise ReadOnlyQueryError(
            "BigQuery scripts and multiple statements are disabled."
        )
    if semicolons:
        remainder = code[semicolons[0] + 1 :]
        if remainder.strip():
            raise ReadOnlyQueryError(
                "BigQuery scripts and multiple statements are disabled."
            )
        code = code[: semicolons[0]]
    words = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", code.lower())
    if not words or words[0] not in _ALLOWED_PREFIXES:
        raise ReadOnlyQueryError("Only SELECT, WITH, and EXPLAIN reads are allowed.")
    denied = sorted(set(words) & _DENIED_KEYWORDS)
    if denied:
        raise ReadOnlyQueryError(
            "Query contains disabled statement keywords: {0}.".format(", ".join(denied))
        )
    return query.strip().rstrip(";").rstrip()
