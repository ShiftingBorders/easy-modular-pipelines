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

The original journal is authoritative. Dashboard opens it read-only and makes
no changes to its contents, schema, indexes, or event format. All cache writes
target a separate, rebuildable database in the dashboard state directory.

`GET /api/application` includes `cache_activity`: `active` worker entries
(experiment ID, initial-build flag, known event count and cached boundary),
`queued` requested histories (including final updates), and `building` experiment
IDs whose initial build is unfinished. The UI displays these builds and their possible
latency impact. This metadata is read from the scheduler, without journal I/O.
Serve schedules running/paused experiments automatically. Terminal or idle
histories start only when explicitly opened; project-wide views and alerts
do not request their construction. Precache explicitly includes all experiments.
An experiment leaving automatic caching receives a final update against a fresh
journal boundary. An older in-flight task cannot consume this request; bounded
worker batches complete it and publish the final module statistics.
Opened histories continue background checks of the source identity and both
cursors. External changes trigger cache updates without requiring a new browser
session; unchanged histories reuse their projections and active window.

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
- Journal responses additionally expose `cached_through` (journal identity,
  completed event/change cursors), `window_start_cursor`, and `cache_gap`.
  A gap is `{after: cached_event_cursor, before: window_start_cursor}` with
  exclusive bounds, or `null`. `target_boundary` identifies the finite source
  boundary requested by this read. Pending disk projections without a complete
  RAM-window representation, or a gap before the RAM window, keep `complete: false`; a source boundary is never
  substituted for a completed cache boundary. Appends after the requested
  boundary are handled by subsequent refreshes.
- Experiment responses include `summary` for the heading and controls, avoiding
  a second summary request. Normal compact page reads use only published cache
  metadata and indexed projections; they neither reopen the original journal
  nor wait for cache construction. RAM-window maintenance and source validation
  run independently in the background. An unavailable raw window is reported
  as `window_error` without inventing history records.
- A complete small history (at most 500 events and within the configured RAM
  window/byte budget) can be returned from its read-only source window during
  the first disk build: `source: ram_window`, `complete: true`,
  `cache_complete: false`, `cache_pending: true`. No historical data is omitted;
  `cached_through` is not advanced to claim unfinished disk work. A subsequent
  ready disk publication replaces the window projection. Incomplete pending
  histories can be retried promptly without waiting for the regular UI interval.
  Initial RAM-window responses contain complete in-window records; measurement
  cards receive the complete bounded collection independently of page size.
- A cursor from replaced history is rejected. Dashboard resets its selection
  on a changed generation. Effective publications must reconcile confirmations,
  ignored evidence and changed outcomes without duplicating records.
- History accepts `limit` (1–1000), opaque `cursor`, `view=effective|raw` and
  `run_id`. Template accepts `revision`; compute accepts `since`/`until`.
  Table search and module/metric selectors filter loaded records in the browser.
- `compact=1` requests screen rows with `detail_ref` instead of embedded large
  source payloads. `GET experiments/{experiment_id}/detail?ref=<JSON detail_ref>`
  loads original records and validates journal identity and generation. Treat
  the reference as opaque. Details return `{...metadata, record: {...}}`;
  derived records include `source_events`, preserving complete original JSON.
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

`modules` is a materialized worker publication. Its response includes `complete`,
`published_at`, and `sources` with per-experiment cache versions, journal
identities and completed boundaries. HTTP reads only this publication and
performs no source-journal scans or aggregate SQL queries. Counts and exact
nearest-rank p50/p95 values cover the full published history; `recent_attempts`
is limited to the latest 100 entries per module. Uninitialized or partially
available histories return an explicit incomplete result, not complete zero counts.

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
| `timeline` | Compact, paginated operation rows with ancestors and a full-history `timeline` overview. Optional `since` / `until` select intersecting operations. |
| `events` | items: event_id, event_type, occurred_at, context, data, confirmation, ignored. Original payload/author are preserved. |
| `errors` | items: error_id, type, message, module_name, stage_id, phase, occurred_at, traceback and context. One item represents one error event. |
| `measurements` | items: module_name, module_version, metric, unit, cycle_number, value, template_revision_id, complete, estimated. See aggregation rules below. |
| `template` | template_yaml, template (normalized JSON), nodes and explicit edges `{from,to,condition}`. Nodes have stage_id, name, module, optional observed status. |
| `parameters` | items: attempt_id, module_name, cycle_number and complete recorded effective parameters. |
| `commands` | items: command/name, target (display name), kind, status/outcome, run_id, sent_at, request_id and original observations. |
| `snapshots` | items: snapshot_id, status, template_revision_id, cycle_number, created_at, validation and recovery details. |
| `artifacts` | items: path/name, purpose, module_name, attempt_id, size_bytes and recorded metadata. |

### Timeline range navigation

`timeline` is independent of `operations`; existing operation pagination and
statistics are unchanged. Use `run_id` to select a run and `limit` (1–1000,
default 200) to bound the number of matching operations per page. Parent
operations and logical-run rows are included for hierarchy and may be repeated
between pages. `total` counts matching operations, excluding these extra rows.
Use operation IDs to merge subsequent pages. Details retain full original times.

`since` and `until` must be supplied together as valid timestamps with
`since < until`. Operations intersecting either boundary are included; an open
operation extends to the latest available observation, not an invented finish.
The range applies to the complete published cache, including operations outside
the raw RAM window. Small complete RAM previews use the same overlap semantics.
An incomplete cache remains explicitly incomplete.

`timeline.start` and `timeline.end` are UTC Unix milliseconds for all recorded
operations in the selected run (or experiment), independent of the requested
range and page. Empty history returns null bounds. `histogram` contains 64 counts
of operation starts between `start` and `histogram_end`; it is an overview, not
a resource metric. Its recorded domain is fixed for one publication. Open
operations can extend `end` with each live observation without moving or
recomputing the histogram buckets. The UI scales the histogram to its own
domain within the full axis.
The reader retains up to 32 small overviews, keyed by cache reader, publication
version and run. Range changes and pagination reuse these summaries; replacement
or a new publication invalidates reuse. Historical payloads are not retained in
this overview cache.
`operation_count` counts all timed operations in that scope. Cursors are bound
to the journal generation, cache publication, run and requested range; changing
the range starts at its first page. Extremely large ancestor sets return an
explicit limit error instead of silently dropping rows.

In the UI, drag either edge of the overview selection to zoom, or drag its body
to pan. Arrow keys adjust the focused edge or selection (Shift increases the
step); Home/End move it to a boundary. `All history` restores automatic scaling.
Manual selections keep absolute times during refresh and appends. Changing the
experiment/run or replacing its journal resets the selection. Striped bar ends
indicate continuation beyond the visible range. Pending requests are cancelled
when a newer range is selected; dragging itself performs no history requests.
| `forecast` | completed_cycles, total_cycles, eta_seconds, paused, sample_mean_seconds, sample_cycles, eta_low_seconds, eta_high_seconds, measurements, component_durations. |

Forecast and measurement responses also include `metric_summaries`,
`measurement_cycles`, and the latest 20 `measurements` per module/version/metric.
`measurement_cycles` is an integer count of distinct cycles, including in a
RAM-window response; it is never a list of cycle numbers.
These summaries cover the entire comparable run/revision cohort, irrespective
of pagination or RAM-window size. The paginated `items` endpoint remains
available for every historical cycle value. Mixed units, duplicate cycles,
unknown values, overlap, and estimation flags remain explicit.

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

Cache construction runs in independent spawned processes (`cache_workers`,
default 2), with one writer per experiment and consistent SQLite read snapshots
for HTTP queries. `--mode precache` builds captured per-experiment boundaries
and exits without starting HTTP or other monitoring services. Both modes reuse
the same persistent checkpoints and fill any gap before the RAM window.

The active source-payload window defaults to 1000 events in RAM per experiment,
ordered by source cursor. Pausing or stopping does not age it out. Exact older
projections and source locators persist on disk; checkpoints and dirty projection
scopes are committed together. A projection working set defaults to at most
100000 compact records / 64 MiB; these are not total-history limits.
Publication cursors expire after five minutes, a cache publication change, or
journal replacement and return 409 `history_changed`. At most 16 small history
publication descriptors are retained. Oversized working sets/records return 413.
The live resource cache retains at most
50000 samples and reports gaps; upstream retention defaults to 15 minutes.

Writes require same-origin validation and `X-Dashboard-Request: 1`; JSON bodies
use `Content-Type: application/json`. These checks are not authentication.
