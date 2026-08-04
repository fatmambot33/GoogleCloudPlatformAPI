# Agents and plugins

GoogleCloudPlatformAPI includes a small deterministic runtime for composing reusable GCP workflows without coupling the package to a specific model provider.

## Create a plugin

```python
from GoogleCloudPlatformAPI.agents import AgentContext, AgentPlugin


class ProjectPlugin(AgentPlugin):
    name = "project"
    description = "Resolve the active project."

    def run(self, context: AgentContext):
        return {"project_id": context.values.get("project_id")}
```

Plugins must have a unique non-empty `name`, avoid import-time side effects, and return a mapping. Values returned by each plugin are merged into the shared context before the next plugin runs.

## Run in Python

```python
from GoogleCloudPlatformAPI.agents import Agent

result = Agent([ProjectPlugin()]).run({"project_id": "demo"})
```

## Load external plugins

Use explicit `package.module:attribute` references:

```python
from GoogleCloudPlatformAPI.agents import load_plugin

plugin = load_plugin("my_package.plugins:ProjectPlugin")
```

The attribute can be a plugin instance or a plugin class with a zero-argument constructor.

## Command line

```bash
gcp-api-agent \
  --plugin my_package.plugins:ProjectPlugin \
  --context '{"project_id":"demo"}'
```

References may also be supplied as a comma-separated environment variable:

```bash
export GCP_API_PLUGINS="my_package.plugins:ProjectPlugin"
gcp-api-agent --context '{"project_id":"demo"}'
```

Use repeated `--only NAME` options to execute a selected ordered subset.

## Safety model

The runtime performs no discovery, network access, or credential handling by itself. Plugins are loaded only from explicit references. GCP operations should inject clients, bound remote results, avoid credential persistence, and default to read-only behavior where practical.
