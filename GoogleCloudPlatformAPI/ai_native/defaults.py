"""Built-in capability definitions for the read-only tool surface."""

from typing import Any, Dict

from GoogleCloudPlatformAPI.ai_native.contracts import SafetyLevel
from GoogleCloudPlatformAPI.ai_native.registry import Capability, capability_registry

_OBJECT = {"type": "object", "additionalProperties": True}


def _limit_schema(maximum: int, default: int) -> Dict[str, Any]:
    """Build a reusable bounded integer schema."""
    return {
        "type": "integer",
        "minimum": 1,
        "maximum": maximum,
        "default": default,
    }


def register_default_capabilities() -> None:
    """Register the package's stable, read-only AI capabilities once."""
    definitions = [
        Capability(
            name="gcp_context",
            service="gcp",
            operation="context",
            description="Inspect active GCP defaults and credential presence safely.",
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
            output_schema=_OBJECT,
            safety=SafetyLevel.READ_ONLY,
            permissions=[],
            timeout_seconds=10,
        ),
        Capability(
            name="bigquery_list_datasets",
            service="bigquery",
            operation="list_datasets",
            description="Discover BigQuery datasets visible to the active project.",
            input_schema={
                "type": "object",
                "properties": {"max_results": _limit_schema(1000, 100)},
                "additionalProperties": False,
            },
            output_schema=_OBJECT,
            safety=SafetyLevel.READ_ONLY,
            permissions=["bigquery.datasets.get"],
            timeout_seconds=30,
        ),
        Capability(
            name="bigquery_list_tables",
            service="bigquery",
            operation="list_tables",
            description="Discover tables and views in a BigQuery dataset.",
            input_schema={
                "type": "object",
                "required": ["dataset_id"],
                "properties": {
                    "dataset_id": {"type": "string", "minLength": 1},
                    "max_results": _limit_schema(1000, 100),
                },
                "additionalProperties": False,
            },
            output_schema=_OBJECT,
            safety=SafetyLevel.READ_ONLY,
            permissions=["bigquery.tables.list"],
            timeout_seconds=30,
        ),
        Capability(
            name="bigquery_table_schema",
            service="bigquery",
            operation="table_schema",
            description="Inspect a BigQuery table schema and lightweight metadata.",
            input_schema={
                "type": "object",
                "required": ["table_id"],
                "properties": {
                    "table_id": {"type": "string", "minLength": 1}
                },
                "additionalProperties": False,
            },
            output_schema=_OBJECT,
            safety=SafetyLevel.READ_ONLY,
            permissions=["bigquery.tables.get"],
            timeout_seconds=30,
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
                    "max_rows": _limit_schema(1000, 100),
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
                    "prefix": {"type": "string", "default": ""},
                    "max_results": _limit_schema(1000, 100),
                },
                "additionalProperties": False,
            },
            output_schema=_OBJECT,
            safety=SafetyLevel.READ_ONLY,
            permissions=["storage.objects.list"],
            timeout_seconds=30,
        ),
        Capability(
            name="gcs_object_metadata",
            service="cloud_storage",
            operation="object_metadata",
            description="Inspect metadata for one Cloud Storage object.",
            input_schema={
                "type": "object",
                "required": ["bucket_name", "object_name"],
                "properties": {
                    "bucket_name": {"type": "string", "minLength": 1},
                    "object_name": {"type": "string", "minLength": 1},
                },
                "additionalProperties": False,
            },
            output_schema=_OBJECT,
            safety=SafetyLevel.READ_ONLY,
            permissions=["storage.objects.get"],
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
                    "max_bytes": _limit_schema(1000000, 100000),
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
