# Creating services and process proxies

A service runs before and alongside the DAG. It stays alive during pauses and
can be called from several stage nodes. Use a service for a database, model
server, background worker, or another resource whose lifetime spans attempts.

Start with [module authoring](modules.md) for directory, configuration, logging,
and artifact rules. Use the shared `ParticipantServer` instead of implementing
another transport.

## Declare the service

```yaml
schema_version: 2
name: my_service
version: "1.0"
role: service
implementation: full
commands:
  start: [python, -B, main.py]
defaults: {}
```

Use `implementation: action` for a command-oriented service/proxy.
Both variants expose the same participant operations. The manifest contains
only `commands.start`; your proxy owns the external start and stop commands.

Two complete implementations are available:

- [Weather service](../../examples/weather_dag/modules/weather_service/1.0/main.py):
  background updates, DAG requests, state export and restoration.
- [Command proxy](../../examples/service_dag/modules/command_proxy/1.0/main.py):
  internal start/stop commands and a stateless snapshot contract.

Register the module through [maintenance mode](../storage.md), then declare
it in the template's [services list](../experiment_template.md#services).

## Connect the shared server

Read the JSON path supplied by `--emp-context`. Construct your process's
OperationLogger from `context["logging_config_path"]`. The server's constructor
takes:

```python
from pathlib import Path

from core.runner_utils.participant_server import ParticipantServer

# context, logger, and handle_request belong to your service implementation.
server = ParticipantServer(
    Path(context["endpoint_path"]),
    context["context"],
    logger,
    handle_request,
)
```

`handle_request` is an async callable receiving a validated request dictionary.
Return `{"result": "success", "data": ...}` or
`{"result": "fail", "data": ...}`; the shared server adds the transport envelope
and original request ID.

The owner explicitly opens the logger, awaits `server.start()`, runs its
application lifecycle, then awaits `server.close()` and closes the logger.
Constructor calls do not replace lifecycle management.

Do not call `server.close()` from inside its request handler. Signal the owner
to stop and return the shutdown response first. See the examples for cleanup
that also handles partial startup.

## Implement the operations

| Operation | Your responsibility |
| --- | --- |
| `heartbeat` | Return success only when the actual service is ready. |
| `execute` | Consume `args["input_data"]` and per-call `args["settings"]`; return the application's result. |
| `interrupt` | Stop the work identified by `args["request_id"]` before reporting success. |
| `shutdown` | Stop owned work and external resources, then signal the owner to exit. |
| `freeze_writes` | Stop all ordinary writes to state that will enter the snapshot. Keep heartbeat responsive. |
| `save_state` | Write complete restorable files inside `args["output_directory"]`; return their experiment-relative `state_path`. |
| `load_state` | Restore the required state from the supplied path before reporting success. |
| `unfreeze_writes` | Resume ordinary writes after snapshot completion or cancellation. |

The shared server implements `command_state` for its own current and pending
calls. It records working command results and interrupt/shutdown outcomes in
the journal. Do not record a second result for the same request ID in a handler.
Use your logger for application events, progress, and errors.

Long work must yield to the event loop. A synchronous compute loop or blocking
subprocess wait in an async handler prevents heartbeat, cancellation, and
shutdown. Track owned work explicitly so interruption stops real effects,
including child processes, rather than merely cancelling an awaiting task.

## Readiness and timeouts

The first successful heartbeat means full readiness, including external startup.
An open socket, completed handshake, or successful launcher command alone is
insufficient. Prepare before admitting successful heartbeats.

| Setting | Meaning |
| --- | --- |
| `start_timeout` | Service startup through first successful heartbeat; also the separate shutdown deadline. |
| `heartbeat.interval_seconds` | Probe interval after readiness. |
| `heartbeat.grace_seconds` | Time allowed for a heartbeat response after readiness. |
| `command_timeout_seconds` | Timeout policy for service-manager work outside a DAG attempt. |
| Stage `timeout_seconds` | Entire DAG attempt, including queueing and response. |
| `runner_timeout_margin_seconds` | Cooperative cancellation/termination margin; it does not extend a successful attempt's deadline. |

A failed application request does not automatically mean the service needs a
restart. The runner owns DAG-request retries; the service manager owns service
restarts. A late success does not reverse an accepted timeout.

Losing the runner connection must not automatically destroy the service.
A live instance accepts reconnection. A restart creates a new instance identity.

Operators can use `service stop POSITION` and `service start POSITION` at an idle
experiment pause. Manual stop uses the existing `shutdown` operation; manual
start launches a new instance and waits for a successful heartbeat. No additional
participant operations are required. The runner persists manual-stop intent and
does not automatically restart that service. Resume, step and snapshot creation
are unavailable until it is explicitly started again. Rollback follows the
selected snapshot's service state.

## Settings and state

Startup settings merge module defaults with `services[].settings`.
A DAG node that references the service supplies its own settings separately;
startup settings are not automatically merged into each request.

Write only to assigned runtime directories. For an external process or container,
redirect its caches and data too. Observe the real resource's readiness and
ownership, not just the PID of its launcher.

A snapshot must include every file required for restoration. Freeze background
threads, subprocess writers, and other sources of state changes before confirming
`freeze_writes`. Publish state files before success and keep them available after
shutdown.

For a stateless service, set `state_required: false` in the template and return
`{"result": "success", "data": {"state_path": null}}` from `save_state`.
Do not claim that partial state is a complete snapshot. In particular, document
whether random-generator state and timers are restored.

The runner owns experiment-wide snapshot and rollback decisions. Services report
actual effects and failures. Unconfirmed shutdown or unfreeze must remain an
explicit failure, not a fabricated success.

See the [participant reference](participant_protocol.md) for protocol versions,
identities, result ownership, and transport details.
