"""Deterministic evaluations for the AI-facing capability surface."""

from dataclasses import dataclass
from typing import Dict, List, Optional

from GoogleCloudPlatformAPI.ai_native.registry import CapabilityRegistry


@dataclass(frozen=True)
class EvaluationResult:
    """Represent one deterministic capability evaluation."""

    name: str
    passed: bool
    message: str
    category: str = "contract"
    measured: Optional[float] = None
    threshold: Optional[float] = None

    def to_dict(self) -> Dict[str, object]:
        """Return a JSON-compatible evaluation result."""
        return {
            "name": self.name,
            "passed": self.passed,
            "message": self.message,
            "category": self.category,
            "measured": self.measured,
            "threshold": self.threshold,
        }


def evaluate_registry(registry: CapabilityRegistry) -> List[EvaluationResult]:
    """Evaluate schema, safety, adapter, and boundedness invariants."""
    results = []
    capabilities = registry.list()
    results.append(
        EvaluationResult(
            name="non_empty_registry",
            passed=bool(capabilities),
            message="At least one capability must be registered.",
            category="schema",
        )
    )
    for capability in capabilities:
        results.extend(
            [
                EvaluationResult(
                    name="{0}:input_schema".format(capability.name),
                    passed=capability.input_schema.get("type") == "object"
                    and capability.input_schema.get("additionalProperties") is False,
                    message="Tool inputs must use a strict object JSON Schema.",
                    category="schema",
                ),
                EvaluationResult(
                    name="{0}:output_schema".format(capability.name),
                    passed=capability.output_schema.get("type") == "object"
                    and capability.output_schema.get("additionalProperties") is False,
                    message="Tool outputs must use a strict object JSON Schema.",
                    category="schema",
                ),
                EvaluationResult(
                    name="{0}:adapter".format(capability.name),
                    passed=bool(capability.adapter_method or capability.handler),
                    message="Capabilities must identify an execution adapter.",
                    category="behavior",
                ),
                EvaluationResult(
                    name="{0}:bounded_timeout".format(capability.name),
                    passed=0 < capability.timeout_seconds <= 300,
                    message="Capabilities must declare a bounded timeout.",
                    category="latency",
                    measured=float(capability.timeout_seconds),
                    threshold=300.0,
                ),
                EvaluationResult(
                    name="{0}:version".format(capability.name),
                    passed=len(capability.version.split(".")) == 3,
                    message="Capabilities must use semantic versions.",
                    category="compatibility",
                ),
            ]
        )
    return results


def readiness_score(registry: CapabilityRegistry) -> Dict[str, object]:
    """Return a compact release-ready AI surface scorecard."""
    results = evaluate_registry(registry)
    passed = sum(1 for result in results if result.passed)
    total = len(results)
    return {
        "passed": passed,
        "total": total,
        "score": round((passed / total) * 100, 1) if total else 0.0,
        "ready": passed == total,
        "results": [result.to_dict() for result in results],
    }
