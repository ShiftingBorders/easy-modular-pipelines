# Documentation

Start with a complete example, then use the guides for your own modules and
experiments. Commands in these guides use the source checkout and its
`uv` environment.

## Start here

1. [Install from source](../README.md#installation).
2. [Run the quickstart](quickstart.md).
3. [Create a module](instructions/modules.md).
4. [Configure and run an experiment](basic_dag.md).
5. [Debug execution](debugging.md).

## Guides and references

| Document | Contents |
| --- | --- |
| [Experiment template](experiment_template.md) | Required YAML fields, module and service references, settings, policies, and paths. |
| [Service authoring](instructions/python_bridges.md) | ParticipantServer handlers, external processes, readiness, shutdown, and snapshots. |
| [Participant API and protocol](instructions/participant_protocol.md) | StageClient, shared service API, identities, versioning, and result semantics. |
| [Module storage](storage.md) | Maintenance mode, registration, validation, versions, and SeaweedFS. |
| [Logging](logging.md) | Module events, errors, artifacts, read APIs, and journal generations. |
| [Resource monitoring](resources.md) | Metrics, configuration, history, and limitations. |
| [Dashboard](dashboard.md) | Setup, screens, commands, alerts, and data sources. |
| [Dashboard API](../dashboard/API_CONTRACT.md) | Dashboard HTTP routes and their relationship to the system API. |
| [System HTTP API](http_api.md) | Server routes, command receipts, authentication, and client errors. |
| [Testing](testing.md) | Existing test commands and environment requirements. |

## Examples

- [Weather experiment](../examples/weather_dag/README.md): a complete project
  with a service, sequential stages, and an output artifact.
- [Counter stage](../examples/basic_dag/modules/counter/1.0/README.md):
  progress, cancellation, and a JSON artifact.
- [Command proxy](../examples/service_dag/modules/command_proxy/1.0/README.md):
  a service that owns external start/stop commands.

Current runtime limitations are listed in the [experiment guide](basic_dag.md#current-limits).
Repository contribution rules are in [AGENTS.md](../AGENTS.md).
