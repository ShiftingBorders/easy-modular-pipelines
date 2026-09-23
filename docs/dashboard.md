# Dashboard

The dashboard is a separate FastAPI application. It reads local experiment
history through the logger's read-only API and uses the system HTTP API for
live state, resources, and commands. Starting it does not start the runtime.

**The journal is the source of truth.** Dashboard history reads never change
journal contents, event formats, tables, indexes, or schema versions. The
dashboard writes its derived cache only in its own state directory. Removing
that cache loses no journal information. Explicit runtime control commands
remain separate: the runtime may record their effects in its journal.

## Start the dashboard

From the framework checkout:

```text
uv run --locked python -B -m dashboard
uv run --locked python -B -m dashboard --project-root examples/weather_dag --system-api-url http://127.0.0.1:8010/api/
```

Open <http://127.0.0.1:8765/>. The first command opens an unconfigured dashboard;
the second connects it to the weather example. Run one command at a time.

Use one HTTP worker. Cache construction runs in separate processes;
`--cache-workers` controls their number (default: 2, range: 1–32).
CLI options are `--config`, `--host`, `--port`, `--project-root`,
`--system-api-url`, `--mode`, and `--cache-workers`.
Library callers use `dashboard.application.create_app(config_path)` with an
absolute configuration path.

## Precache and exit

Use the same configuration and project root as the dashboard that will consume
the cache:

```text
uv run --locked python -B -m dashboard --config path/to/dashboard.json --mode precache --cache-workers 4
```

`--project-root path/to/runtime` can override the configured project. This mode
starts only cache processes: no HTTP server, ICMP monitor, runtime polling or
command submission. Experiments from the initial registry snapshot are
distributed among independent spawned processes, with one writer per
experiment. Each bounded task releases its resources and commits its progress.
The ordinary `serve` mode uses the same process-based cache workers while the
HTTP process maintains the active RAM windows and reads projections.
Ordinary page reads do not wait for cache workers. They read the latest
published snapshot, with incomplete history shown explicitly while a rebuild
continues. Startup can prime one bounded batch per worker when no published
reader context exists. Successful control commands refresh their history in
workers so subsequent navigation can observe their effects, including rollback.

Serve automatically caches only running or paused experiments, including their
active startup, snapshot and restoration phases. Stopped, completed, failed and
idle experiments are cached when explicitly opened. Overview, the experiment
list, Modules and alert monitoring do not initiate their cache construction.
Precache explicitly processes every registered experiment regardless of phase.
When an automatically cached experiment leaves its active phase, serve queues
one final update with a fresh journal boundary, even if an older cache task is
still running. That update finishes in bounded worker batches and publishes the
final projections and module statistics before background caching stops.
Once a history has been opened, background checks continue to verify its source
identity and both event/change cursors, including while it is stopped. Changes
made by another client or CLI schedule an incremental update or a rebuild after
replacement. Unchanged sources reuse their RAM windows and cached projections;
Histories already stopped when discovered are not probed or cached automatically
unless opened or selected by precache.
An initial build displays a status banner naming the experiments and explaining
that pages may respond more slowly until caching finishes.

On first opening a stopped experiment with at most 500 events, dashboard can
render its complete active RAM window immediately while the disk cache is built.
This applies only when the whole observed history fits both the configured
event window and payload budget. The source is read-only, command confirmations
are reconciled, and a changing source boundary prevents this shortcut. Subsequent
reads reuse that window until the corresponding disk publication is ready.
The response reports `source: ram_window`, accurate `complete` data coverage,
and a separate `cache_complete` flag; `cached_through` still describes only the
completed disk prefix. Larger or partially available histories stay explicitly
incomplete and refresh promptly while caching is pending.

For each experiment, precache captures a target journal boundary at its first
successful task. It exits once every target's projections are complete, even
if the runtime keeps appending events. Later appends are handled on the next
precache or dashboard refresh. JSON-line progress reports contain experiment
IDs, worker PIDs, target boundaries and completed cache boundaries. Exit code
0 means all targets completed, 1 means an experiment failed, and 2 indicates a
configuration or pool-level failure. Interruption exits with 130; committed
progress remains available for resumption.

Use the same `state_directory` to reuse precached data. Multiple writers for one
cache are prevented by an OS file lock in that directory, independent of the
source journal. SQLite read snapshots allow the dashboard to read a consistent
publication while a worker advances it.

## Connect data sources

- `project_root` points to a locally readable project with `experiments.json`,
  runner state, journals, and artifacts.
- `system_api_url` points to that same project's runtime.
- The dashboard creates reader configurations in its own state directory.
- History can be available while the runtime is offline; it does not establish
  that processes are currently alive.
- A remote API URL alone does not make historical files available locally.
- Without a data source, the interface shows unavailable or empty data rather
  than fabricated examples.

## Settings

Base settings are in [dashboard/settings.json](../dashboard/settings.json).

| Field | Purpose / default |
| --- | --- |
| `host`, `port` | Dashboard address: `127.0.0.1:8765`. |
| `system_api_url` | System API base URL; `null` means no connection. |
| `request_timeout_seconds` | HTTP request deadline: 8 seconds. |
| `max_response_bytes` | HTTP response/page limit: 8 MiB. |
| `state_directory` | Dashboard state: `.state` beside its configuration. |
| `refresh_seconds` | Initial UI refresh: 5 seconds. |
| `project_root` | Optional local project directory, initially `null`. |
| `system_api_token_env` | Optional environment-variable name containing the system bearer token. |
| `history_window_events` | Complete source events retained in RAM per experiment: 1000. |
| `cache_workers` | Independent cache processes: 2 (1–32). |
| `history_max_events` | Maximum compact input records for one affected execution scope: 100000; not a limit on total history. |
| `history_max_bytes` | Active payload window and per-scope projection working budget: 64 MiB. |

Relative JSON paths resolve from the configuration file; absolute paths remain
absolute. CLI paths are resolved at the CLI boundary.

The system token is sent server-to-server; the browser does not receive it.
Do not put the token value in JSON. Redirects and implicit HTTP proxies are not
used. The dashboard itself has no user authentication and is intended for a
trusted local environment. Binding to `0.0.0.0` does not add authentication;
remote access requires a suitably protected external proxy.

## Screens

| Screen | Use |
| --- | --- |
| Overview | Current metrics, attention items, CPU/RAM/disk, and ICMP. |
| Experiments | Choose an experiment, browse logical runs, submit Run. |
| Execution / DAG | Inspect stages, attempts, nested operations, and cycles. |
| Errors | Group failures and inspect original events and tracebacks. |
| Events | Browse raw/effective journal events and their JSON. |
| Resources | Compare numerical module metrics across completed cycles. |
| Artifacts | Browse recorded results and download available local files. |
| Run settings | Inspect recorded YAML/JSON revisions and attempt parameters. |
| Commands | Inspect receipts, pending work, and actual outcomes. |
| Snapshots | Inspect saved states and request restoration. |
| Iterations and forecast | View progress, ETA, and component durations. |
| Modules / Services | Inspect attempts, restarts, service instances, and process metrics. |
| Compute resources | View CPU, RAM, disk, and network history. |
| Alerts | Configure thresholds, error-rate rules, ICMP, and notifications. |

Search and filters operate on loaded records. History has pagination and JSON
inspection. The interface is English and uses local fonts.

Background refresh updates existing rows and blocks by stable identifiers.
Unchanged DOM nodes, focus, input values, expanded details, and scroll containers
are retained. A pending refresh keeps the preceding observations visible;
failures identify them as previous observations. Opening a record's details
loads its original source events when they are outside the active RAM window.

Experiment tabs receive their heading and page data in one response. Switching
tabs within the same experiment keeps its heading and navigation visible while
the new content loads. Number/date formatters and unchanged DOM nodes are reused.

Modules reads a precomputed project-wide publication in
`state_directory/readers/modules.json`. Cache workers build it from consistent
read snapshots of the experiment caches and replace it atomically. Its source
versions and journal boundaries are included in the API response. Unchanged
versions reuse the existing publication, including across dashboard restarts;
precache also finishes this publication before exiting.

Opening Modules does not read original journals or calculate statistics.
Counts and exact p50/p95 values cover all included attempts, not just the RAM
window or recent-attempt list. Percentiles combine ordered duration samples
across experiments; per-experiment percentiles are never averaged. Pending or
unavailable histories are shown as incomplete statistics. Source checks remain
in the background cache workers, so changes become visible on their next refresh.

## Commands and restoration

Pause, Resume, Step, Stop, Snapshot, and Recover submit commands to the runtime,
which validates the operation and current state. HTTP 202 is admission, not
completion. The dashboard stores the command ID before sending and polls its
outcome. Unknown outcomes do not trigger automatic resubmission.

Snapshot restoration requires confirmation because it replaces state/history.
The dashboard resets old history pages when the journal generation changes.
A recorded artifact may no longer exist after attempt cleanup; downloads then
return 404.

## Statistics and forecasts

Statistics compare completed DAG cycles of the same revision.
`delta` is summed, `total` uses the last value per stream, `peak` uses the
maximum, and `gauge` uses the mean. Overlapping nested operations and
incompatible measurement kinds are not added together.

ETA uses comparable completed cycles and a known remaining workload.
Paused cycles are excluded from the timing sample. Additive metrics can estimate
remaining and total consumption; gauges are not converted to total consumption.
An unfinished cycle counts as remaining work in full. Missing observations or
insufficient history produce incomplete/unknown estimates, not zero.

## ICMP and notifications

ICMP runs on the **dashboard server's machine**, not the browser or necessarily
the runtime machine. Monitoring is opt-in; the initial target is
`www.google.com`. IPv4 and DNS names with IPv4 records are supported.
Windows uses `IcmpSendEcho`; Linux uses `iputils ping`.

Resource metrics come from the runtime collector.
GPU/VRAM monitoring is unfinished and disabled.

Alerts can use sustained thresholds, free disk space, error counts in a time
window, and ICMP failures. Repeated observations do not create duplicate
incidents. An unavailable source is not evidence of recovery.

Desktop notifications and sound are delivered on the dashboard host and are
disabled initially. Delivery depends on the OS session and notification
settings. Alerts and ICMP continue working without an open browser.

## History and limits

Dashboard state uses an OS lock, so two instances cannot share one state
directory. Corrupted state is reported rather than silently replaced.

The live collector's default RAM retention is 15 minutes. Dashboard retains up
to 50000 received resource samples; selecting a longer time range cannot
reconstruct missing observations. Experiment journals remain the persistent
source for cycle statistics.

The active RAM window consists of the latest `history_window_events` source
events in cursor order, including ignored or superseded observations. It is
independent of wall-clock time. Paused and completed experiments retain their
window; historical detail reads do not displace recent events.

Older history is represented by indexed, exact projection inputs and screen
records in `state_directory/readers/*.cache.sqlite`. Source event IDs and
cursors remain attached to derived records. Large original parameters,
templates, tracebacks and command observations are fetched through the logger
using the journal's existing indexes. Full old payloads are not retained in RAM.

Initial cache construction reads history in bounded batches and reports
`complete: false` until ingestion and projections catch up. Background refresh
continues without an open browser. Later refreshes consume only new journal
changes and recalculate affected execution scopes; unchanged completed scopes
are reused. Restarting resumes persisted checkpoints and reconstructs only the
active payload window. Live runtime observations do not rebuild history.

Measurement scopes inherit missing coordinates from recorded attempt parameters.
Projection inputs use those same coordinates for run filtering; raw event pages
and source details retain the original event context.
Indexed ancestor lookups include parent operations outside the measurement's
scope, so nested totals are not counted twice. Later ancestor observations also
invalidate dependent scopes. Attempt statuses are overlaid with runtime freshness
when pages are read; recorded completion outcomes remain unchanged.

Ordinary history pages read only the derived database and published reader
metadata. Original journal checks, runner-state reads and raw-window updates
run in the background. An unchanged raw tail is reused; small appends extend
the window incrementally. HTTP readers use separate locks, so source I/O cannot
block switching tabs. The cache also holds the exact current template document;
explicit inspection of older revisions still loads their original source events.
All new metadata and indexes belong to the disposable cache, not the journal.

The cache always persists `cached_through`: journal ID, generation, event
`cursor` and `change_cursor` through the last completed projection boundary.
`ingested_through` and the change checkpoint separately track committed input
whose projections may still be pending. Workers resume from these positions;
they never jump their checkpoint forward to the RAM window.

If uncached events precede the start of the latest N-event window, dashboard
reports the gap and prioritizes caching the missing prefix. History stays
explicitly incomplete until caught up. Late confirmations are also consumed
through the change cursor even when the event cursor has not advanced.

The cache is disposable. Stop dashboard before removing its `*.cache.sqlite`
files; the next start reconstructs them from the original journals. Cache
version or journal generation/file identity changes invalidate derived data.
An unavailable or corrupt source is reported, never repaired by dashboard.
SQLite read-only access may use WAL coordination files; it does not write
journal events or alter the journal schema.

Indexed history pages retain only small publication descriptors in RAM. They
expire after five minutes, a cache publication change, or journal replacement;
at most 16 descriptors are retained. Expired pages require restarting pagination.
Limits apply to the active window, one projection working set, and responses,
not to the total historical event count. Oversized working sets are reported
explicitly without truncating events. The disk cache grows with the amount of
exact historical information retained.

See the [dashboard API contract](../dashboard/API_CONTRACT.md),
[system API](http_api.md), and [test requirements](testing.md#dashboard).
