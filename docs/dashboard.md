# Dashboard

The dashboard is a separate FastAPI application. It reads local experiment
history through the logger's read-only API and uses the system HTTP API for
live state, resources, and commands. Starting it does not start the runtime.

## Start the dashboard

From the framework checkout:

```text
uv run --locked python -B -m dashboard
uv run --locked python -B -m dashboard --project-root examples/weather_dag --system-api-url http://127.0.0.1:8010/api/
```

Open <http://127.0.0.1:8765/>. The first command opens an unconfigured dashboard;
the second connects it to the weather example. Run one command at a time.

Use one worker. CLI options are `--config`, `--host`, `--port`,
`--project-root`, and `--system-api-url`.
Library callers use `dashboard.application.create_app(config_path)` with an
absolute configuration path.

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
| `history_max_events` | Optional event limit per experiment: 100000. |
| `history_max_bytes` | Optional loaded-history/page budget: 64 MiB. |

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

Published history pages expire after five minutes or a journal generation
change. Up to 16 publications share the history byte budget. Expired pages
require restarting pagination. Increase the configured history limits when
needed; oversized history is reported explicitly.

See the [dashboard API contract](../dashboard/API_CONTRACT.md),
[system API](http_api.md), and [test requirements](testing.md#dashboard).
