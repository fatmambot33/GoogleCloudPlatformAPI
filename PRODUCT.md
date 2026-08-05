# Product

## Mission

Build a simple, reliable, typed, and production-ready Python interface for
common Google Cloud workflows that is equally usable by developers, automation,
and AI agents.

## Product promise

Every supported capability should be:

- easy to discover and understand;
- predictable and strongly typed;
- safe by default;
- bounded and observable;
- documented for humans and machines;
- backward compatible within a major release;
- represented once and reused across Python, CLI, MCP, and agent surfaces.

## Target users

Primary users are Python developers and data teams using BigQuery, Cloud
Storage, Analytics, or Ad Manager who want less boilerplate without hiding the
underlying Google services.

Secondary users are automation systems and AI agents that need deterministic,
machine-readable, permission-aware tools.

The project is not intended to replace the complete official Google Cloud SDKs,
provide an unrestricted remote control plane, or expose broad mutation tools by
default.

## Product principles

1. Keep the package Python-first and lean.
2. Prefer explicit contracts over magic.
3. Generate adapters from one canonical capability registry.
4. Keep credentials local and redact secrets before logging.
5. Bound rows, bytes, pages, and execution time.
6. Ship read-only agent tools before considering mutations.
7. Treat compatibility, security, documentation, and evaluations as release
   gates.
8. Add framework-specific integrations only when usage justifies them.
