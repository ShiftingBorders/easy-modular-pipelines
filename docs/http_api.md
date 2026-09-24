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
