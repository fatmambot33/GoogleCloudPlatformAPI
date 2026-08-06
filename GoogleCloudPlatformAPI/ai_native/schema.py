"""Dependency-free validation for the supported JSON Schema subset."""

from typing import Any, Dict, Iterable, List, Union

SchemaType = Union[str, List[str]]


class SchemaValidationError(ValueError):
    """Report a deterministic schema validation failure."""

    def __init__(self, path: str, message: str) -> None:
        self.path = path
        self.message = message
        super().__init__(f"{path}: {message}")


def _matches_type(value: Any, expected: str) -> bool:
    """Return whether a value matches one JSON Schema type."""
    checks = {
        "array": lambda item: isinstance(item, list),
        "boolean": lambda item: isinstance(item, bool),
        "integer": lambda item: isinstance(item, int) and not isinstance(item, bool),
        "null": lambda item: item is None,
        "number": lambda item: isinstance(item, (int, float))
        and not isinstance(item, bool),
        "object": lambda item: isinstance(item, dict),
        "string": lambda item: isinstance(item, str),
    }
    check = checks.get(expected)
    if check is None:
        raise ValueError(f"Unsupported JSON Schema type: {expected}")
    return bool(check(value))


def _expected_types(schema_type: SchemaType) -> Iterable[str]:
    """Normalize a JSON Schema type declaration."""
    if isinstance(schema_type, str):
        return (schema_type,)
    return tuple(schema_type)


def validate_json_schema(value: Any, schema: Dict[str, Any], path: str = "$") -> None:
    """Validate a value against the package's supported JSON Schema subset.

    Parameters
    ----------
    value : Any
        JSON-compatible value to validate.
    schema : dict
        JSON Schema using object, array, string, integer, number, boolean, or
        null types plus common structural bounds.
    path : str, optional
        Human-readable location used in validation errors.

    Raises
    ------
    SchemaValidationError
        If the value does not satisfy the schema.
    ValueError
        If the schema uses an unsupported type declaration.
    """
    if "enum" in schema and value not in schema["enum"]:
        raise SchemaValidationError(path, "value is not in the allowed enum")

    schema_type = schema.get("type")
    if schema_type is not None:
        expected = tuple(_expected_types(schema_type))
        if not any(_matches_type(value, item) for item in expected):
            raise SchemaValidationError(
                path, "expected type {0}".format(" or ".join(expected))
            )

    if value is None:
        return

    if isinstance(value, dict):
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        for name in required:
            if name not in value:
                raise SchemaValidationError(path, f"missing required property '{name}'")
        additional = schema.get("additionalProperties", True)
        for name, item in value.items():
            child_path = f"{path}.{name}"
            if name in properties:
                validate_json_schema(item, properties[name], child_path)
            elif additional is False:
                raise SchemaValidationError(path, f"unexpected property '{name}'")
            elif isinstance(additional, dict):
                validate_json_schema(item, additional, child_path)
        return

    if isinstance(value, list):
        minimum = schema.get("minItems")
        maximum = schema.get("maxItems")
        if minimum is not None and len(value) < minimum:
            raise SchemaValidationError(path, f"expected at least {minimum} items")
        if maximum is not None and len(value) > maximum:
            raise SchemaValidationError(path, f"expected at most {maximum} items")
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, item in enumerate(value):
                validate_json_schema(item, item_schema, f"{path}[{index}]")
        return

    if isinstance(value, str):
        minimum = schema.get("minLength")
        maximum = schema.get("maxLength")
        if minimum is not None and len(value) < minimum:
            raise SchemaValidationError(path, f"minimum length is {minimum}")
        if maximum is not None and len(value) > maximum:
            raise SchemaValidationError(path, f"maximum length is {maximum}")
        return

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if minimum is not None and value < minimum:
            raise SchemaValidationError(path, f"minimum value is {minimum}")
        if maximum is not None and value > maximum:
            raise SchemaValidationError(path, f"maximum value is {maximum}")
