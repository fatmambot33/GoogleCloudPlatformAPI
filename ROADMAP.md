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
- [x] Request IDs, duration metadata, logging hooks, and secret redaction
- [x] Deterministic AI readiness evaluation and release scorecard
- [x] Human and machine-readable AI platform documentation

## Execution roadmap

1. [x] #56 Release 2.7.0 as a truthful installable AI-native package.
2. [x] #57 Make capability contracts strict and generated across surfaces.
3. [ ] #58 Enforce safe observable execution for billable and remote operations.
4. [ ] #59 Add behavioral agent evaluations and release evidence.

Issue #58 is implemented in PR #62 and remains incomplete until every CI gate
passes and the pull request is merged. Issue #59 starts only from that green
baseline.

Each milestone must merge with a green main branch before work starts on the
next dependency. Release expansion follows evidence, not framework count.

## Deliberately gated

Mutation tools, remote HTTP deployment, OpenAPI, TypeScript, graphical UI, and
framework-specific adapters are not default roadmap requirements. Add them only
when concrete usage requires them and after the shared registry, safety,
authorization, audit, compatibility, and evaluation contracts remain satisfied.
