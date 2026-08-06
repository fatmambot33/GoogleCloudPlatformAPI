# Roadmap

## Completed foundation

- [x] Product vision and repository standards
- [x] Typed Python packaging and public type marker
- [x] Stable package-root imports and package exception hierarchy
- [x] Quality, test, 90% coverage, package, and release automation
- [x] Discovery-first local Codex/MCP server
- [x] BigQuery dataset, table, schema, and bounded query workflow
- [x] Cloud Storage listing, metadata, and bounded text-read workflow
- [x] Framework-neutral agent runtime and CLI
- [x] Canonical AI-native capability registry
- [x] Strict generated input and output contracts
- [x] Registry-driven MCP and CLI surfaces
- [x] Structured result and error envelopes
- [x] Provider-enforced cost, timeout, range, pagination, and error controls
- [x] Request IDs, duration metadata, optional telemetry, and secret redaction
- [x] Behavioral golden scenarios and prompt-injection containment tests
- [x] MCP protocol conformance tests
- [x] Generated JSON, Markdown, and JUnit AI readiness evidence
- [x] Optional OpenAI Agents SDK adapters generated from shared contracts
- [x] Human and machine-readable AI platform documentation

## Execution roadmap

1. [x] #56 Release 2.7.0 as a truthful installable AI-native package.
2. [x] #57 Make capability contracts strict and generated across surfaces.
3. [x] #58 Enforce safe observable execution for billable and remote operations.
4. [x] #59 Add behavioral agent evaluations and release evidence.

Every roadmap milestone is implemented behind a green release gate. The 2.8.0
release carries the behavioral scorecard, MCP conformance evidence, SBOM, and
provenance beside the installable distributions.

## Deliberately gated

Mutation tools, remote HTTP deployment, OpenAPI, TypeScript, and graphical UI
are not default roadmap requirements. Add them only when concrete usage
requires them and after authorization, audit, compatibility, and evaluation
contracts remain satisfied.
