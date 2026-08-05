"""Built-in capability definitions for the existing read-only tool surface."""

from GoogleCloudPlatformAPI.ai_native.contracts import SafetyLevel
from GoogleCloudPlatformAPI.ai_native.registry import Capability, capability_registry

_OBJECT = {"type": "object", "additionalProperties": True}


def register_default_capabilities() -> None:
    """Register the package's stable, read-only AI capabilities once."""
    definitions = [
        Capability(
            name="gcp_context",
            service="gcp",
            operation="context",
            description="Inspect the active local Google Cloud configuration.",
            input_schema={"type": "object", "properties": {}, "additionalProperties": False},
            output_schema=_OBJECT,
            safety=SafetyLevel.READ_ONLY,
            permissions=[],
            timeout_seconds=10,
        ),
        Capability(
            name="bigquery_query",
            service="bigquery",
            operation="query",
            description="Run a bounded read-only BigQuery statement.",
            input_schema={
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {"type": "string", "minLength": 1},
                    "max_rows": {"type": "integer", "minimum": 1, "maximum": 10000},
                },
                "additionalProperties": False,
            },
            output_schema=_OBJECT,
            safety=SafetyLevel.READ_ONLY,
            permissions=["bigquery.jobs.create"],
            timeout_seconds=60,
        ),
        Capability(
            name="gcs_list_objects",
            service="cloud_storage",
            operation="list_objects",
            description="List a bounded set of objects in a Cloud Storage bucket.",
            input_schema={
                "type": "object",
                "required": ["bucket_name"],
                "properties": {
                    "bucket_name": {"type": "string", "minLength": 1},
                    "prefix": {"type": "string"},
                    "max_results": {"type": "integer", "minimum": 1, "maximum": 1000},
                },
                "additionalProperties": False,
            },
            output_schema=_OBJECT,
            safety=SafetyLevel.READ_ONLY,
            permissions=["storage.objects.list"],
            timeout_seconds=30,
        ),
        Capability(
            name="gcs_read_text",
            service="cloud_storage",
            operation="read_text",
            description="Read a bounded UTF-8 object from Cloud Storage.",
            input_schema={
                "type": "object",
                "required": ["bucket_name", "object_name"],
                "properties": {
                    "bucket_name": {"type": "string", "minLength": 1},
                    "object_name": {"type": "string", "minLength": 1},
                    "max_bytes": {"type": "integer", "minimum": 1, "maximum": 1000000},
                },
                "additionalProperties": False,
            },
            output_schema=_OBJECT,
            safety=SafetyLevel.READ_ONLY,
            permissions=["storage.objects.get"],
            timeout_seconds=30,
        ),
    ]
    existing = {item.name for item in capability_registry.list()}
    for definition in definitions:
        if definition.name not in existing:
            capability_registry.register(definition)


register_default_capabilities()
