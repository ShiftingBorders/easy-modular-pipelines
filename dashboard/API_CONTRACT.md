# System API connection contract

Dashboard reads local history through `OperationLogger(..., read_only=True)`
and uses the existing system HTTP API for live state, resources and commands.
Starting dashboard does not start `webserver.py`.

See the [dashboard guide](../docs/dashboard.md) for setup and the
[system HTTP API reference](../docs/http_api.md) for runtime routes and modes.

The actual upstream routes are `GET state`, `GET resources`,
`GET resources/history?after=...&limit=...`, `POST commands`, and
`GET commands/{command_id}`, relative to `system_api_url`.
Historical screen routes below belong to dashboard. They require a configured,
locally readable `project_root` for the same runtime.

`system_api_url` is a configured base URL. Requests are server-to-server HTTP calls
from dashboard to that base URL. The browser communicates with dashboard's own
origin. Credentials and redirects are not accepted in the base URL; redirects
are not followed. Environment proxy settings are not inherited implicitly.
The optional `system_api_token_env` setting names the environment variable
containing the system server's bearer token. The dashboard HTTP client sends it
server-to-server; the browser never receives the secret. Error responses preserve
the system's message and expose its diagnostic code as `error.upstream_code`.

## Common response rules

- Successful responses are JSON objects. Lists use `items: [...]`, including an
  explicit empty list; an absent list is an invalid response, not zero records.
- Every experiment response includes `experiment_id`, exactly matching the
  requested experiment. Run-scoped requests use the optional `run_id` query.
- Times are ISO 8601 with timezone. Durations are seconds. Unknown numeric
  values are `null`, never invented zeros; non-finite numbers are invalid.
- `observed_at` records observation freshness. A live phase requires a current
  observation from the runtime; persisted state alone does not prove liveness.
- Journal responses carry `journal: {journal_id, generation}`, `observed_at`,
  `items` and optional `next_cursor`. Cursor is opaque to the dashboard and must
  identify the publication/generation where required by the logger.
- A cursor from replaced history is rejected. Dashboard resets its selection
  on a changed generation. Effective publications must reconcile confirmations,
  ignored evidence and changed outcomes without duplicating records.
- History accepts `limit` (1–1000), opaque `cursor`, `view=effective|raw` and
  `run_id`. Template accepts `revision`; compute accepts `since`/`until`.
  Table search and module/metric selectors filter loaded records in the browser.
- 404/501 means unavailable resource/capability. Other errors do not become empty
  success responses. HTTP time and decoded response size are bounded locally.

Dashboard combines the public logger read/change feeds with recorded runner
metadata and live API observations. It does not query event tables directly or
instantiate runtime/controller classes. These views do not change the event schema.

## Root resources

| Dashboard GET path under `/api/system/` | Used fields |
| --- | --- |
| `overview` | `metrics` with active/completed/failed_experiments and error_events; `attention`; `compute`; `active_alerts`. |
| `experiments` | `items`: experiment_id, name, status, run_id, template_revision_id, completed_cycles, total_cycles, started_at. |
| `modules` | `items`: module_id, name, version, experiment_name, runs, error_count, restarts, p50_seconds, p95_seconds, recent_attempts. |
| `services` | `items`: instance_id, name, version, state, observed_at, uptime_seconds, queue_length, process_metrics. |
| `compute` | `metrics`, `history`, observation/source metadata. `since` / `until` select actual resource history. |
| `alerts` | `items` with name/type, status, started_at and details; `active_count` for system incidents. |

System Alerts exclude dashboard-local ICMP incidents, which are counted separately.
An unknown system count remains unknown in the header even if local ICMP is healthy.

`compute.metrics.cpu/ram/disk` are `{value, fresh, exceeded}` objects with
host-normalized percentages. `internet` has `receive_mbps` and `transmit_mbps`.
The selected internet path belongs to the system collector, not an arbitrary
browser interface. `history` maps cpu/ram/disk to arrays of
`{observed_at, value}`; null samples break chart lines.

`process_metrics` has `rss_bytes`, `cpu_percent` and `history` containing both
fields with `observed_at`. Process CPU uses 100% per logical CPU; RSS is physical
resident memory / Windows working set. The system must attach the actual
observed process identity and never substitute proxy-process utilization for
the service behind it.

## Experiment resources

Paths are relative to `/api/system/experiments/{experiment_id}/` on dashboard.

| GET suffix | Used fields beyond common metadata |
| --- | --- |
| `summary` | name, status, run_id, template_revision_id, observed_at. |
| `runs` | items: run_id, template_revision_id, status in lineage order. |
| `operations` | items: operation_id, parent_operation_id, name/operation_type, started_at, finished_at, status and contextual details. |
| `events` | items: event_id, event_type, occurred_at, context, data, confirmation, ignored. Original payload/author are preserved. |
| `errors` | items: error_id, type, message, module_name, stage_id, phase, occurred_at, traceback and context. One item represents one error event. |
| `measurements` | items: module_name, module_version, metric, unit, cycle_number, value, template_revision_id, complete, estimated. See aggregation rules below. |
| `template` | template_yaml, template (normalized JSON), nodes and explicit edges `{from,to,condition}`. Nodes have stage_id, name, module, optional observed status. |
| `parameters` | items: attempt_id, module_name, cycle_number and complete recorded effective parameters. |
| `commands` | items: command/name, target (display name), kind, status/outcome, run_id, sent_at, request_id and original observations. |
| `snapshots` | items: snapshot_id, status, template_revision_id, cycle_number, created_at, validation and recovery details. |
| `artifacts` | items: path/name, purpose, module_name, attempt_id, size_bytes and recorded metadata. |
| `forecast` | completed_cycles, total_cycles, eta_seconds, paused, sample_mean_seconds, sample_cycles, eta_low_seconds, eta_high_seconds, measurements, component_durations. |

`measurements` contains **one aggregate per completed DAG cycle / module version /
metric / revision**, prepared using logger resource semantics. It is not an array
of raw deltas: the browser must not accidentally add cumulative totals, gauges
or parent/child usage. The API retains coverage and estimation flags and returns
a comparable revision cohort. Mixed units/revisions, duplicated cycle entries
and missing values prevent a single numeric mean in the UI. Negative metric
values remain signed in labels; bar length represents magnitude.

`component_durations` contains `{module_name, mean_seconds}` per comparable DAG
cycle. Forecast requires a known remaining workload. Open-ended termination
conditions or insufficient observations return null estimates.

IDs and source data remain distinct: experiment, logical run, template revision,
stage, attempt, operation, cycle, service instance and observed process are not
interchangeable. DAG edges come from the template; operation parents belong to
the timeline and do not synthesize DAG dependencies.

Artifact `path_base` distinguishes `experiment` for stage result files from
`attempt` for `record_artifact` paths. Downloads require enough recorded context
to locate the attempt and remain within that directory. Unknown attempt context
or a cleaned-up file returns 404.

## Dashboard-owned ICMP API

These routes are implemented in this app and do not call the system API:

| Route | Behavior |
| --- | --- |
| `GET /api/application` | Application identity, dashboard hostname, connection configuration state and UI refresh default. |
| `GET /api/icmp` | Settings, freshness, latest observation, bounded history, local incidents and storage status. |
| `PUT /api/icmp/settings` | Validate and atomically save enabled, host, timeout_seconds, interval_seconds. |
| `POST /api/icmp/probe` | One immediate server-side probe, coalesced with an already-running probe. |

Writes require `X-Dashboard-Request: 1`; configuration uses JSON. Cross-origin
browser writes are rejected. These checks are not user authentication.

Every observation reports `source: dashboard_host` and `probe_host`. A browser
opened on machine B while dashboard runs on machine A measures connectivity
from **A**. The system runtime can be on a third machine without changing this.
Stopping browser polling does not stop the FastAPI-owned monitoring loop.

## Commands, artifacts and Alert configuration

| Dashboard route | Behavior |
| --- | --- |
| `POST /api/commands` | Submit `{command, args?, target?, command_id?, expected_experiment_id?}`. HTTP 202 carries the upstream receipt. |
| `GET /api/commands/{command_id}` | Query actual outcome; pending admission is not success. |
| `GET /api/experiments/{experiment_id}/artifacts/{artifact_id}/download` | Download a recorded local artifact; paths outside the experiment are rejected. |
| `GET /api/alerts` | Rules, notifications, all ICMP/system incidents and active count. |
| `POST /api/alerts/rules` | Create/update resource or error-frequency rule. |
| `DELETE /api/alerts/rules/{id}` | Delete rule and close its incident as configuration changed. |
| `PUT /api/alerts/notifications` | Save desktop, sound, on_recovery and repeat_seconds. |
| `POST /api/alerts/notifications/test` | Deliver through enabled channels on the dashboard host. |

Commands use runtime operations: run, pause, resume, step, stop, rerun,
retry, move, reset_retries, snapshot, rollback, recover.
Support and valid phases are enforced by runtime. Paths in command arguments
refer to the runtime machine and should be absolute. Unknown outcomes do not
cause automatic resubmission. `expected_experiment_id` is checked by dashboard
and removed before forwarding; it cannot prevent another system client from
changing the selection after this preflight check.

A resource rule has optional id, name, enabled, kind=resource, metric,
operator=above|below, threshold and duration_seconds. Metrics: cpu, ram, disk
(percent), disk_free_gib, internet_receive and internet_transmit (Mbps).
An error rule uses kind=errors, threshold (positive event count), window_seconds,
duration_seconds and optional experiment_id. An unavailable source cannot
resolve an active incident. GPU/VRAM are unfinished and unused in sampling,
views and rules; their prototype code is retained.

Journal caches default to 100000 events / 64 MiB per experiment. Publication
cursors expire after five minutes or journal generation replacement and return
409 `history_changed`. At most 16 publications share the history byte budget.
Oversized histories/records return 413. The live resource cache retains at most
50000 samples and reports gaps; upstream retention defaults to 15 minutes.

Writes require same-origin validation and `X-Dashboard-Request: 1`; JSON bodies
use `Content-Type: application/json`. These checks are not authentication.
