# AI Readiness Release Evidence

GoogleCloudPlatformAPI treats AI readiness as a release gate backed by
deterministic evidence, not as a manually maintained checklist.

## Run the gate

```bash
gcp-api-eval --output build/ai-readiness --fail-under 100
```

The command runs without credentials, provider calls, or network access. It
writes:

- `scorecard.json`: machine-readable results and metric pass rates;
- `scorecard.md`: a concise human scorecard;
- `evaluations.xml`: JUnit evidence for CI systems.

A non-zero exit status means a schema, behavioral, safety, protocol, latency, or
token-footprint gate failed. Pass `--baseline previous-scorecard.json` to reject
category or overall-score regressions.

## Golden scenarios

The canonical corpus contains golden scenarios for every shipped capability.
Scenarios check:

- tool selection and sequencing;
- strict argument generation;
- bounded reads and pagination behavior;
- refusal of mutating BigQuery SQL;
- explicit truncation and recoverable errors;
- prompt-injection containment for BigQuery rows and Cloud Storage text;
- latency and approximate token-footprint budgets.

MCP conformance checks cover initialization, ping, tool discovery, strict input
and output schemas, JSON-RPC method-not-found behavior, and notifications.

Provider-controlled values are treated as untrusted data. Instruction-like text
inside rows or objects must never create derived tool calls or override the
agent's governing instructions.

## Optional OpenAI Agents SDK

Install the optional adapter only when needed:

```bash
pip install "GoogleCloudPlatformAPI[openai-agents]"
```

`openai_tool_specs`, `build_openai_tools`, and `build_openai_agent` generate the
framework surface from the canonical capability registry. The base package does
not import or require the OpenAI Agents SDK.

## Release integration

The normal CI workflow uploads the generated evidence artifact after the full
Python matrix, minimum-dependency test, package build, clean-wheel install, and
MCP smoke test pass. The release workflow runs the evidence command from the
installed wheel and attaches the evidence beside the distributions, SBOM, and
provenance attestation.
