"""Built-in capability definitions for the read-only tool surface."""

from typing import Any, Dict, List, Union

from GoogleCloudPlatformAPI.ai_native.contracts import SafetyLevel
from GoogleCloudPlatformAPI.ai_native.registry import Capability, capability_registry

SchemaTypes = Union[str, List[str]]


def _typed(schema_type: SchemaTypes, **constraints: Any) -> Dict[str, Any]:
    """Build a typed JSON Schema fragment."""
    return {"type": schema_type, **constraints}


def _object(
    properties: Dict[str, Any], required: List[str]
) -> Dict[str, Any]:
    """Build a strict object schema."""
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def _limit_schema(maximum: int, default: int) -> Dict[str, Any]:
    """Build a reusable bounded integer schema."""
    return _typed("integer", minimum=1, maximum=maximum, default=default)


def _dataset_schema() -> Dict[str, Any]:
    """Return one dataset discovery record schema."""
    return _object(
        {
            "dataset_id": _typed("string", minLength=1),
            "project": _typed(["string", "null"]),
            "full_id": _typed(["string", "null"]),
        },
        ["dataset_id", "project", "full_id"],
    )


def _table_schema() -> Dict[str, Any]:
    """Return one table discovery record schema."""
    return _object(
        {
            "table_id": _typed("string", minLength=1),
            "table_type": _typed(["string", "null"]),
            "full_id": _typed(["string", "null"]),
        },
        ["table_id", "table_type", "full_id"],
    )


def _field_schema() -> Dict[str, Any]:
    """Return one BigQuery field schema."""
    return _object(
        {
            "name": _typed("string", minLength=1),
            "type": _typed("string", minLength=1),
            "mode": _typed(["string", "null"]),
            "description": _typed(["string", "null"]),
        },
        ["name", "type", "mode", "description"],
    )


def register_default_capabilities() -> None:
    """Register the package's stable, read-only AI capabilities once."""
    definitions = [
        Capability(
            name="gcp_context",
            service="gcp",
            operation="context",
            description="Inspect active GCP defaults and credential presence safely.",
            input_schema=_object({}, []),
            output_schema=_object(
                {
                    "project_id": _typed(["string", "null"]),
                    "credentials_configured": _typed("boolean"),
                    "credentials_file": _typed(["string", "null"]),
                    "write_tools_enabled": _typed("boolean"),
                },
                [
                    "project_id",
                    "credentials_configured",
                    "credentials_file",
                    "write_tools_enabled",
                ],
            ),
            safety=SafetyLevel.READ_ONLY,
            permissions=[],
            timeout_seconds=10,
            adapter_method="context",
        ),
        Capability(
            name="bigquery_list_datasets",
            service="bigquery",
            operation="list_datasets",
            description="Discover BigQuery datasets visible to the active project.",
            input_schema=_object(
                {"max_results": _limit_schema(1000, 100)}, []
            ),
            output_schema=_object(
                {
                    "datasets": _typed(
                        "array", items=_dataset_schema(), maxItems=1000
                    ),
                    "returned_datasets": _typed(
                        "integer", minimum=0, maximum=1000
                    ),
                    "truncated": _typed("boolean"),
                },
                ["datasets", "returned_datasets", "truncated"],
            ),
            safety=SafetyLevel.READ_ONLY,
            permissions=["bigquery.datasets.get"],
            timeout_seconds=30,
            adapter_method="bigquery_list_datasets",
        ),
        Capability(
            name="bigquery_list_tables",
            service="bigquery",
            operation="list_tables",
            description="Discover tables and views in a BigQuery dataset.",
            input_schema=_object(
                {
                    "dataset_id": _typed("string", minLength=1),
                    "max_results": _limit_schema(1000, 100),
                },
                ["dataset_id"],
            ),
            output_schema=_object(
                {
                    "dataset_id": _typed("string", minLength=1),
                    "tables": _typed(
                        "array", items=_table_schema(), maxItems=1000
                    ),
                    "returned_tables": _typed(
                        "integer", minimum=0, maximum=1000
                    ),
                    "truncated": _typed("boolean"),
                },
                ["dataset_id", "tables", "returned_tables", "truncated"],
            ),
            safety=SafetyLevel.READ_ONLY,
            permissions=["bigquery.tables.list"],
            timeout_seconds=30,
            adapter_method="bigquery_list_tables",
        ),
        Capability(
            name="bigquery_table_schema",
            service="bigquery",
            operation="table_schema",
            description="Inspect a BigQuery table schema and lightweight metadata.",
            input_schema=_object(
                {"table_id": _typed("string", minLength=1)}, ["table_id"]
            ),
            output_schema=_object(
                {
                    "table_id": _typed("string", minLength=1),
                    "table_type": _typed(["string", "null"]),
                    "description": _typed(["string", "null"]),
                    "num_rows": _typed(["integer", "null"], minimum=0),
                    "num_bytes": _typed(["integer", "null"], minimum=0),
                    "partitioning": _typed(["string", "null"]),
                    "fields": _typed(
                        "array", items=_field_schema(), maxItems=10000
                    ),
                },
                [
                    "table_id",
                    "table_type",
                    "description",
                    "num_rows",
                    "num_bytes",
                    "partitioning",
                    "fields",
                ],
            ),
            safety=SafetyLevel.READ_ONLY,
            permissions=["bigquery.tables.get"],
            timeout_seconds=30,
            adapter_method="bigquery_table_schema",
        ),
        Capability(
            name="bigquery_query",
            service="bigquery",
            operation="query",
            description="Run a bounded read-only BigQuery statement.",
            input_schema=_object(
                {
                    "query": _typed("string", minLength=1),
                    "max_rows": _limit_schema(1000, 100),
                },
                ["query"],
            ),
            output_schema=_object(
                {
                    "rows": _typed(
                        "array",
                        items={"type": "object", "additionalProperties": True},
                        maxItems=1000,
                    ),
                    "returned_rows": _typed(
                        "integer", minimum=0, maximum=1000
                    ),
                    "truncated": _typed("boolean"),
                },
                ["rows", "returned_rows", "truncated"],
            ),
            safety=SafetyLevel.READ_ONLY,
            permissions=["bigquery.jobs.create"],
            timeout_seconds=60,
            adapter_method="bigquery_query",
        ),
        Capability(
            name="gcs_list_objects",
            service="cloud_storage",
            operation="list_objects",
            description="List a bounded set of objects in a Cloud Storage bucket.",
            input_schema=_object(
                {
                    "bucket_name": _typed("string", minLength=1),
                    "prefix": _typed("string", default=""),
                    "max_results": _limit_schema(1000, 100),
                },
                ["bucket_name"],
            ),
            output_schema=_object(
                {
                    "objects": _typed(
                        "array", items=_typed("string"), maxItems=1000
                    ),
                    "returned_objects": _typed(
                        "integer", minimum=0, maximum=1000
                    ),
                    "truncated": _typed("boolean"),
                },
                ["objects", "returned_objects", "truncated"],
            ),
            safety=SafetyLevel.READ_ONLY,
            permissions=["storage.objects.list"],
            timeout_seconds=30,
            adapter_method="storage_list",
        ),
        Capability(
            name="gcs_object_metadata",
            service="cloud_storage",
            operation="object_metadata",
            description="Inspect metadata for one Cloud Storage object.",
            input_schema=_object(
                {
                    "bucket_name": _typed("string", minLength=1),
                    "object_name": _typed("string", minLength=1),
                },
                ["bucket_name", "object_name"],
            ),
            output_schema=_object(
                {
                    "bucket_name": _typed("string", minLength=1),
                    "object_name": _typed("string", minLength=1),
                    "size": _typed(["integer", "null"], minimum=0),
                    "content_type": _typed(["string", "null"]),
                    "generation": _typed(["integer", "string", "null"]),
                    "updated": _typed(["string", "null"]),
                    "md5_hash": _typed(["string", "null"]),
                },
                [
                    "bucket_name",
                    "object_name",
                    "size",
                    "content_type",
                    "generation",
                    "updated",
                    "md5_hash",
                ],
            ),
            safety=SafetyLevel.READ_ONLY,
            permissions=["storage.objects.get"],
            timeout_seconds=30,
            adapter_method="storage_object_metadata",
        ),
        Capability(
            name="gcs_read_text",
            service="cloud_storage",
            operation="read_text",
            description="Read a bounded UTF-8 object from Cloud Storage.",
            input_schema=_object(
                {
                    "bucket_name": _typed("string", minLength=1),
                    "object_name": _typed("string", minLength=1),
                    "max_bytes": _limit_schema(1000000, 100000),
                },
                ["bucket_name", "object_name"],
            ),
            output_schema=_object(
                {
                    "text": _typed("string", maxLength=1000000),
                    "bytes_returned": _typed(
                        "integer", minimum=0, maximum=1000000
                    ),
                    "truncated": _typed("boolean"),
                },
                ["text", "bytes_returned", "truncated"],
            ),
            safety=SafetyLevel.READ_ONLY,
            permissions=["storage.objects.get"],
            timeout_seconds=30,
            adapter_method="storage_read_text",
        ),
    ]
    existing = {item.name for item in capability_registry.list()}
    for definition in definitions:
        if definition.name not in existing:
            capability_registry.register(definition)


register_default_capabilities()
