# System HTTP API

`webserver.py` owns the runtime for one project directory.
`cli.py` and the dashboard are clients. Start the server independently using
the [experiment guide](basic_dag.md).

The default API base is `http://127.0.0.1:8000/api`.
FastAPI's interactive route documentation is at `/docs` on the same server.

## Routes

| Method and path under `/api` | Purpose |
| --- | --- |
| `GET /health` | Server/controller health and server mode. Wait for readiness before sending work. |
| `GET /state?experiment_id=...` | Current runner state; an explicit ID must match the selected experiment. |
| `GET /experiments` | Registered experiments and their published state summaries. |
| `GET /experiments/inspect?experiment_id=...` | A registered experiment's published state and directory. |
| `GET /snapshots?experiment_id=...` | Published snapshot metadata for the specified or selected experiment. |
| `GET /snapshots/inspect?snapshot_id=...&experiment_id=...` | Snapshot metadata and manifest; does not validate file integrity. |
| `GET /resources` | Current resource-collector observations. |
| `GET /resources/history?after=0&limit=100` | Page of RAM resource history. |
| `GET /experiments/{experiment_id}/events?limit=100&cursor=...` | Journal page; cursor is an encoded JSON checkpoint. |
| `POST /commands` | Admit one control command. |
| `POST /chains` | Admit a sequence of commands, each with its own outcome. |
| `GET /commands/{command_id}` | Read one retained command outcome without executing it again. |
| `GET /commands?limit=100&after=...&state=...&command=...` | Retained receipt summaries in submission order; filters and UUID cursor. |
| `GET /modules` | Registered module name/version/hash references. |
| `GET /modules/inspect?name=...&version=...` | Registration, archive availability and local installation presence. |
| `POST /templates/validate` | Validate template structure and registered references; JSON body: `{"template_path":"<absolute-server-path>"}`. |
| `GET /artifacts?experiment_id=...` | Recorded artifacts with current file availability. |
| `GET /artifacts/download?experiment_id=...&artifact_id=...` | Stream a recorded file as `application/octet-stream`. |

Read endpoints expose data directly; command outcomes use a receipt/result
envelope. A successful health request alone does not prove that initialization
is complete.

Resource history uses nonnegative `after` and `limit` from 1 to 1000.
Event pages also use a 1–1000 limit. Keep journal checkpoints opaque and retain
their generation information.

## Saved experiment and snapshot metadata

Experiment and snapshot metadata reads work in both modes. Maintenance mode
requires `experiment_id` for snapshot reads; run mode defaults to the selected
experiment. Inspecting an experiment does not select it. Saved experiment data
has `source: saved_state`; snapshot data has `integrity: not_checked`. Neither
is proof of live process state or successful restoration.

List responses contain `items`; unreadable entries have `available: false` and
an `error` string. A missing registry produces an empty experiment list, while
a malformed registry fails the request. Inspect responses wrap saved state in
`state` or snapshot metadata in `manifest`. Unknown IDs return 404; malformed
IDs or incompatible metadata return 400. Existing response-size limits apply.

## Module, template, receipt and artifact reads

Module discovery, template validation, artifact reads and receipt listing work
in both modes. They do not admit a control command or return a receipt. Template
validation does not verify archive contents, installed code or resource files;
the response names its scope explicitly.

Maintenance mode runs up to four background reads concurrently and queues up
to 64 more. A full read queue returns HTTP 429 (`too_many_reads`). State queries
and shutdown do not wait for those reads. Shutdown cancels read workers and
waits for any in-flight archive checks before closing storage. Module mutations
remain serial; concurrent reads are observations, not a transactional snapshot.

Receipt lists return `items`, `has_more`, `next_after` and `server_instance_id`.
The limit is 1–1000. Filters match state and exact controller command name.
Reuse filters with the next cursor. An expired/unknown cursor returns 404;
pagination is over mutable, bounded retention rather than a historical snapshot.

Artifact lists scan published journal events through their initial boundary.
They do not depend on dashboard caches. Downloads return the first matching
recorded artifact, after checking its attempt-relative path. Unavailable files
return 404. JSON responses use existing size limits; binary downloads stream
without that JSON limit. A file removed during transfer can interrupt the stream.

## Submit and observe a command

Send JSON with `Content-Type: application/json`:

```json
{
  "api_version": 1,
  "command_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "command": "run",
  "args": {
    "template_path": "<absolute-template-path-on-server>",
    "delayed_start": true
  }
}
```

Generate a new UUID for a new command. `api_version` defaults to 1 and
`command_id` can be assigned by the server. Some commands accept a
`target: {"kind": "stage"|"service", "position": 1}` instead of placing target
information in args; use the CLI to construct supported command forms.

HTTP 202 confirms admission. Poll the returned command ID until the result
leaves `pending`. Completed responses carry `state`, `result`, `data`, and
`error`. Success of `run` signals launch; it does not mean the DAG completed.
Use `/state` to observe the experiment.

An identical explicit ID can retrieve a retained identical submission.
A conflicting reuse is rejected. This is not a permanent exactly-once guarantee:
receipt retention is bounded, and loss of a response does not prove absence of
external effects. Query the result, state, and journal before retrying.

A chain body has `commands: [...]` and an optional `chain_id`. A failed command
prevents later commands in that chain from proceeding. Client wait timeouts
do not cancel admitted work.

## Modes and commands

Maintenance mode accepts:

| Command | Args |
| --- | --- |
| `module.add` | `{"folder": "<absolute-source-folder>"}` |
| `module.validate` | Either `{"folder": "<absolute-source-folder>"}` or `{"name": "...", "version": "..."}`. |
| `module.remove` | `{"name": "...", "version": "..."}` |

Successful add/stored validation returns `data.module` with name/version/hash.
Module operations are rejected in run mode.

Run mode accepts experiment lifecycle, stage/service controls, snapshots,
recovery, and archive operations described in the [experiment guide](basic_dag.md).
The runtime checks the current phase; an exposed route does not make every
operation valid in every state.

Individual service control uses the existing `POST /api/commands` endpoint:

```json
{
  "api_version": 1,
  "command": "service.stop",
  "args": {},
  "target": {"kind": "service", "position": 1}
}
```

Use `service.start` with the same target to start that service. Positions are
one-based. These commands require an idle paused experiment and empty `args`;
their target must have `kind: service`. Maintenance mode rejects admission with
HTTP 409 (`invalid_mode`). They use ordinary command receipts, chain ordering
and `--wait` completion. A priority experiment `stop` cancels in-flight service
control and still completes participant cleanup.

Successful results contain `service_id`, `service_instance_id`,
`manually_stopped`, and either `ready` for start or `stopped` for stop. Repeated
calls do not replace a ready instance or re-stop an already stopped instance.
The state response also exposes `services[].manually_stopped`; recovery and
reconciliation preserve this intent. Resume, step and snapshot creation require
those services to be explicitly started. Rollback restores the earlier snapshot's
services. See [service commands](cli.md#start-and-stop-individual-services).

## Reload the selected template

Use the existing command endpoint in run mode:

```json
{
  "command": "reload_template",
  "args": {"template_path": "<absolute-server-path>/updated.yaml"}
}
```

Empty args select the experiment's `experiment.yaml`. No target is accepted.
The experiment must be idle and paused, with no unresolved attempt, maintenance
operation, or blocked/manually stopped service. Only `stages` and `services`
may change. Normal receipts and chain ordering apply; success means the reload
has committed and its services are ready. The experiment remains paused.

Results identify the old/new template revisions, protective snapshot, cursor,
and change summary. An identical normalized template returns `changed: false`
and `snapshot_id: null`. State responses expose `run_id`, `template_revision_id`,
and `pending_rebuild`. An interrupted rebuild requires `recover` before further
DAG control; priority `stop` stops participants and retains that requirement.

New reload commands in maintenance mode are rejected with `invalid_mode`.
Invalid templates/unsupported field changes return `invalid_request`; an
unavailable execution boundary returns `invalid_state`. As with all commands,
HTTP admission is distinct from the eventual command outcome.

## Runtime lifecycle

`POST /api/commands` accepts `server.restart` with empty `args`, and
`server.mode` with `{"mode": "run"}` or `{"mode": "maintenance"}`. These operations
run in the HTTP owner rather than its controller. They accept no target and
cannot appear in command chains. Existing authentication and receipt limits
apply. For example:

```json
{"api_version": 1, "command": "server.mode", "args": {"mode": "maintenance"}}
```

Admission returns 202 and the normal retained command ID. Poll its existing
result endpoint. Success means that shutdown completed and the replacement
controller reported readiness; selecting the current healthy mode is a no-op.
The result includes `previous_runtime_id`, `runtime_id`, `server_mode`, `changed`
and `controller` OS identity. Mode changes are session-local and do not rewrite
the configuration file. Active experiments are stopped and are not automatically
resumed or selected by the new runtime.

HTTP remains running with the same `server_instance_id` and receipt cache.
`GET /api/health` additionally reports `runtime_id` and `restart_blocked`.
While a lifecycle command is pending, health reports `state: restarting` and
HTTP 503; new controller work returns `controller_unavailable`. Concurrent
lifecycle admission returns 409 `restart_pending`. Reusing an identical retained
command ID retrieves its existing receipt. Previous command outcomes remain
available subject to ordinary retention limits; unresolved controller work may
be marked `unknown` on shutdown.
Re-submitting an identical retained service/module command also returns its
receipt after a mode change, without executing it again. Mode restrictions apply
to new commands; a retained ID with a different request remains a conflict.

Forced, nonzero or incomplete shutdown fails the operation with
`runtime_restart_failed` and blocks replacement. Further lifecycle mutations
return 409 `restart_blocked`; inspect diagnostics and restart the HTTP process
explicitly. Controller journals are separate for each `runtime_id` under
`controller/server`. Late messages from a previous generation cannot change
the replacement's readiness or receipts.

`server.shutdown` with empty `args` stops the runtime and requests graceful HTTP
exit. It works in either mode, accepts no target and cannot appear in a chain.
Concurrent lifecycle operations are rejected with 409 (`shutdown_pending` or
`restart_pending`). Repeating the same retained request ID retrieves its receipt.

```text
POST /api/commands?wait=true
Content-Type: application/json

{"command": "server.shutdown", "args": {}}
```

Without `wait=true`, admission returns 202. `wait=true` is exclusive to shutdown:
the request remains open until its outcome and returns HTTP 200 with the command
receipt. The graceful HTTP owner drains this request before exiting. Authentication
and input validation run before admission. A client disconnect does not cancel
the independent operation.

Successful data includes `runtime_id`, `runtime_stopped: true` and
`http_shutdown_requested: true`. This acknowledges cleanup and the exit request;
it is not remote proof of process exit. Failure returns a failed receipt with
`server_shutdown_failed` and leaves HTTP available for diagnostics. Health reports
`shutting_down`/503 while the operation is pending. Receipts cease to be available
once the HTTP process exits.

The `webserver.py` entry point supplies the shutdown hook. An external ASGI owner
can set `app.state.stop_http` to a synchronous callable which requests graceful
shutdown and drains accepted HTTP requests. Without this capability the command
returns 501 `unsupported_feature` without stopping the controller. The runtime
does not send OS signals to an arbitrary host process.

## Configuration and authentication

Server settings are in [webserver.json](../default_settings/webserver.json).
Configure `project_root` and the desired `server_mode`; relative paths resolve
from the JSON file. CLI overrides are resolved at the CLI boundary.

For bearer authentication, set `token_env` to an environment-variable name
and populate that variable in the server process. Clients use their own
`--token-env` setting to read the same token and send
`Authorization: Bearer <token>`. Never store the token value in project JSON.

A non-loopback listener requires a configured token. For remote use, provide
an appropriate protected transport. Filesystem paths in requests are always
paths on the server, not files uploaded by the HTTP client.

## Errors and limits

Errors preserve a diagnostic `code` and `message`. Invalid input, unavailable
dependencies, unsupported commands, stale state, and capacity limits are not
converted to empty success responses. Read errors may use HTTP 400/404/409/501/503;
an admitted command can instead finish with a failed outcome in its result body.

Server settings bound pending commands, reads, retained receipts, request and
response sizes, and cached result bytes. The default result retention is one
hour; do not use receipts as the permanent experiment journal.

The [dashboard API](../dashboard/API_CONTRACT.md) is a separate interface.
Dashboard history routes use locally readable project files; the system API URL
alone does not provide those files.
