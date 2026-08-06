"""Generate adapter metadata and compatibility evidence from the registry."""

from typing import Any, Dict, Iterable, List, Optional, Sequence

from GoogleCloudPlatformAPI.ai_native.registry import Capability, CapabilityRegistry


def mcp_tool_definitions(
    registry: CapabilityRegistry, order: Optional[Sequence[str]] = None
) -> List[Dict[str, Any]]:
    """Generate MCP tool definitions from canonical capability contracts."""
    capabilities: Iterable[Capability]
    if order is None:
        capabilities = registry.list()
    else:
        capabilities = (registry.get(name) for name in order)
    return [
        {
            "name": capability.name,
            "description": capability.description,
            "inputSchema": capability.input_schema,
            "outputSchema": capability.output_schema,
            "annotations": {
                "readOnlyHint": capability.safety.value != "mutating",
                "destructiveHint": capability.safety.value == "mutating",
                "idempotentHint": capability.safety.value != "mutating",
                "openWorldHint": False,
            },
        }
        for capability in capabilities
    ]


def compatibility_snapshot(registry: CapabilityRegistry) -> Dict[str, Any]:
    """Return a compact deterministic contract snapshot for API diffing."""
    capabilities = []
    for capability in registry.list():
        capabilities.append(
            {
                "name": capability.name,
                "version": capability.version,
                "input_schema": capability.input_schema,
                "output_schema": capability.output_schema,
                "safety": capability.safety.value,
                "timeout_seconds": capability.timeout_seconds,
                "deprecated": capability.deprecated,
                "replaced_by": capability.replaced_by,
            }
        )
    return {"snapshot_version": "1.1.0", "capabilities": capabilities}


def _schema_breaks(
    before: Dict[str, Any], after: Dict[str, Any], input_schema: bool
) -> List[str]:
    """Return breaking differences between two object schemas."""
    changes = []
    before_properties = before.get("properties", {})
    after_properties = after.get("properties", {})
    removed = sorted(set(before_properties) - set(after_properties))
    if removed:
        changes.append("removed properties: {0}".format(", ".join(removed)))
    for name in sorted(set(before_properties) & set(after_properties)):
        if before_properties[name].get("type") != after_properties[name].get("type"):
            changes.append(f"changed type for property: {name}")
    added_required = sorted(
        set(after.get("required", [])) - set(before.get("required", []))
    )
    if added_required:
        label = "inputs" if input_schema else "outputs"
        changes.append(
            "added required {0}: {1}".format(label, ", ".join(added_required))
        )
    return changes


def compare_compatibility_snapshots(
    before: Dict[str, Any], after: Dict[str, Any]
) -> Dict[str, Any]:
    """Classify registry changes as compatible, additive, or breaking."""
    previous = {item["name"]: item for item in before.get("capabilities", [])}
    current = {item["name"]: item for item in after.get("capabilities", [])}
    changes = []
    breaking = False
    additive = False

    for name in sorted(set(previous) - set(current)):
        breaking = True
        changes.append({"capability": name, "kind": "removed"})
    for name in sorted(set(current) - set(previous)):
        additive = True
        changes.append({"capability": name, "kind": "added"})
    for name in sorted(set(previous) & set(current)):
        before_item = previous[name]
        after_item = current[name]
        reasons = _schema_breaks(
            before_item["input_schema"], after_item["input_schema"], True
        ) + _schema_breaks(
            before_item["output_schema"], after_item["output_schema"], False
        )
        if before_item.get("safety") != after_item.get("safety"):
            reasons.append("changed safety classification")
        if reasons:
            breaking = True
            changes.append({"capability": name, "kind": "breaking", "reasons": reasons})
        elif before_item != after_item:
            additive = True
            changes.append({"capability": name, "kind": "changed"})

    classification = (
        "breaking" if breaking else "additive" if additive else "compatible"
    )
    return {"classification": classification, "changes": changes}


def capability_reference_markdown(registry: CapabilityRegistry) -> str:
    """Generate a compact Markdown capability reference."""
    lines = [
        "# Capability reference",
        "",
        "Generated from `capability_registry`; do not maintain a second tool list.",
        "",
        "| Name | Version | Service | Safety | Timeout |",
        "| --- | --- | --- | --- | ---: |",
    ]
    for capability in registry.list():
        lines.append(
            "| `{0}` | {1} | {2} | {3} | {4}s |".format(
                capability.name,
                capability.version,
                capability.service,
                capability.safety.value,
                capability.timeout_seconds,
            )
        )
    return "\n".join(lines) + "\n"
