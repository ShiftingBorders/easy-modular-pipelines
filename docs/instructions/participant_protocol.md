# Participant API and protocol

This reference covers the shared API used by ordinary stages and long-lived
services. Start with [module authoring](modules.md) or
[service authoring](python_bridges.md) for implementation examples.

## Versions and roles

| Document or channel | Current version |
| --- | --- |
| Module manifest and experiment template | `schema_version: 2` |
| Participant TCP and module launch context | `protocol_version: 2` |
| Runner state | `schema_version: 3` |
| Journal events and storage | Version 2 |
| Experiment snapshot, restoration transaction, exchange archive | `schema_version: 2` |

These versions do not change the outer HTTP API or logger connection-settings
schema. Incompatible older experiments and archives are rejected, not migrated.

Schema-3 service records include the boolean `manually_stopped` intent. A record
without this field defaults to `false`. Recovery preserves explicit manual stops;
this flag does not replace observed process termination or readiness.

An ordinary stage is a subprocess executing one attempt. The library's
StageExecutor owns its process and runner connection. A module does not need
to implement the TCP server.

A service owns a ParticipantServer and application handlers. It starts before
the DAG, stays alive on pause, and can serve several DAG nodes. The runner
owns DAG retries; the service manager owns restarts.

## StageClient

```python
from core.runner_utils.stage_client import StageClient

# context_path is the absolute Path supplied by --emp-context.
with StageClient(context_path) as client:
    if client.cancel_requested():
        client.fail({"reason": "cancelled"})
    else:
        client.report_progress(0.5, "Processing")
        client.report_state({"phase": "processing"})
        client.succeed({"input": client.input_data})
```

| Member | Contract |
| --- | --- |
| `open()`, `close()`, context manager | Explicitly manage the executor connection and background communication. Construction alone performs no I/O. |
| `input_data` | Previous accepted stage data, when available. |
| `settings` | Effective settings after opening. |
| `context` | Runner-provided paths, logging configuration, and identities. |
| `report_progress(value, message=None)` | Number from 0 to 1; optional message must be nonempty. |
| `report_state(data)` | JSON object for observation, not a memory checkpoint. |
| `cancel_requested()` | Whether cooperative cancellation has been requested. Check at safe points. |
| `succeed(data)`, `fail(data)` | Write exactly one result JSON; do not terminate the process automatically. |

Closing does not invent a result. Connection failures are reported to the
caller, not converted into cancellation. The public API is synchronous.

## Launch context

The executor appends `--emp-context <absolute-path>` to `commands.start`.
The context is a JSON file, rather than a potentially oversized command-line
payload. Relevant fields are:

| Field | Meaning |
| --- | --- |
| `experiment_directory` | Root for experiment-relative result paths. |
| `resources_directory` | Copied static resources. |
| `settings_directory` | Runtime settings directory. |
| `module_data_directory` | Mutable data for this definition. |
| `artifacts_directory` | Assigned output directory for this attempt/call. |
| `logging_config_path` | This process's journal-client configuration. |
| `endpoint_path` | Assigned participant endpoint. |
| `control_timeout_seconds` | Module control-connection timeout. |
| `context` | Experiment, participant, attempt, and related logging identifiers. |
| `settings`, `input_data` | Application configuration and input. |

Use supplied absolute directories. Do not derive them from cwd or invent
attempt numbers. Code directories remain immutable.

## ParticipantServer

`ParticipantServer(endpoint_path, context, logger, handler)` accepts an async
`handler(request)`. The logger belongs to the service process. Optional
`describe` supplements command-state observations; ordinary service authors
do not need the executor's module-client handler.

Handlers return `result` and `data`; the server adds the response envelope.
Working operations run sequentially, while heartbeat, command-state inspection,
interrupt, and shutdown remain independently serviceable.
Do not block the event loop with synchronous long-running work.

| Operation | Handler or library responsibility |
| --- | --- |
| `heartbeat` | Handler reports real readiness. |
| `command_state` | Shared server reports its current/pending calls. |
| `execute` and application commands | Handler performs work; server journals the result before replying. |
| `interrupt` | Handler stops the named work; this does not restart the service. |
| `shutdown` | Handler stops owned resources; owner closes the server after the handler returns. |
| `freeze_writes/save_state/load_state/unfreeze_writes` | Handler implements actual state consistency and restoration. |

The shared server records working results and interrupt/shutdown outcomes.
Do not record another competing result for the same request ID.

The first successful heartbeat confirms complete readiness, including internal
startup. Handshake alone is insufficient. Shutdown acknowledgement and actual
process termination are separate observations. Connection loss does not
automatically stop a service.

## Service nodes in a DAG

Declare the service in `services` with its own explicit `service_id`,
module reference, startup settings, heartbeat and error policies.
A DAG node references that ID instead of repeating `module`:

```yaml
- stage_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
  service_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
  settings: {}
  timeout_seconds: 30
  errors:
    retries: 2
    retry_delay_seconds: 1
    on_exhausted: pause
```

This is a fragment, not a complete template.
Startup settings merge defaults with `services[].settings`. A node's settings
are passed separately with `input_data`; startup settings are not merged into
each call. See the [template reference](../experiment_template.md).

The attempt deadline starts at queue admission. Queueing, transmission, and
response all consume it. Retrying creates a new request ID; restarting a service
does not silently resend a DAG request.

## Wire protocol

Most Python modules should use the library classes above. For integrations
that implement the participant channel directly:

- Participants listen on loopback `127.0.0.1` with a port selected by the OS.
- Frames contain an 8-byte unsigned big-endian payload length followed by
  exactly that many bytes of a UTF-8 JSON object.
- Readers must handle partial reads and multiple frames. Serialize writes.
  A malformed frame closes the connection; payloads are not silently truncated.
- The endpoint identifies `experiment_id`, `participant_id`,
  `participant_instance_id`, OS process identity, address, and `token_file`.
- Handshake `hello` verifies identity, version, role, and token. `runner`
  and `module` are distinct client roles.
- Requests use `protocol_version`, `message_type: request`, identity fields,
  `request_id`, `command`, `args`, and `deadline_monotonic`.
- Responses use `message_type: response`, the original `request_id`,
  `result`, and `data`. There are no separate `status` or `command_result`
  transport envelopes.
- Cooperative cancellation is a `notification` with `command: cancel`;
  it is not the final result.

The executor endpoint belongs to the attempt's `executor.lock.json`.
A service endpoint is `runner/endpoints/<service_id>.json`.
Token files are instance-specific and are not included in logged commands.
A saved PID alone does not prove ownership.

The protocol coordinates trusted processes under the experiment owner's account.
It is not a sandbox for untrusted module code.

## Results and restoration

Stage stdout still carries `{"result": "success"|"fail", "data": ...}`.
StageExecutor validates stdout and exit code, then records the result in
`journals/events.sqlite`. There is no separate result JSON file.
Artifacts stay on disk with experiment-relative links in result data.

`request_id` and participant identity associate each result with its work.
`author: participant` records observed completion; `author: runner` records
the accepted outcome and takes priority. Late evidence does not turn an
accepted timeout into success.

Recovery considers saved state, journal outcomes, and live participant
observations. Absence from `current/pending` does not prove that a command
never ran. Before repeating unresolved stage work, the runner requires proof
that the old process stopped.

Snapshots include a consistent journal and validate referenced results.
Service state exports must be complete and available after shutdown.
See [logging](../logging.md) and [experiment recovery](../basic_dag.md#snapshots-recovery-and-exchange).
