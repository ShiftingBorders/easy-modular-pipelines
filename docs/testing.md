# Testing

Use Python 3.12, the project's `uv` environment, and standard-library
`unittest`. Run commands from the repository root.

## Existing suite

```text
uv sync --locked
uv run python scripts/download_seaweedfs.py
uv run python -m unittest discover -s tests -p "test_*.py" -v
uv run python -m compileall core cli.py webserver.py dashboard
uv run --with ruff ruff check core tests dashboard
```

The full suite includes filesystem, SQLite, subprocess, local HTTP/TCP, and
resource-collector scenarios. Some integration cases take several minutes.
Temporary outputs belong under `.artifacts/`.

The repository's VS Code configuration uses `unittest`, the `tests` directory,
and the `test_*.py` discovery pattern.

## Continuous integration

GitHub Actions runs Ruff and Python compilation in a separate `Static checks`
job. Test jobs run independently using a matrix of Ubuntu and Windows with
nine groups: `storage`, `modules`, `experiments`, `journal`, `participants`,
`resources`, `server`, `dashboard_tests`, and `root`.

Each test job installs the locked environment and pinned SeaweedFS binary.
Directory groups use discovery with the repository as the import root:

```text
uv run python -m unittest discover -s tests/experiments -t . -p "test_*.py" -v
```

The `root` group explicitly lists the test modules directly under `tests/`.
When adding a root test module or a new test directory, update the workflow
to keep all tests included. The matrix uses `fail-fast: false`, so a failed
group does not cancel other groups. Failure diagnostics are uploaded from
`.artifacts/ci-failures/` with the group and OS in the artifact name.

Browser tests still require the prerequisites described below and skip when
they are unavailable. Splitting the suite does not change platform-specific
or opt-in test behavior.

## Selected suites

The compact checkpoint-history checks use five focused scenarios:

```text
uv run python -m unittest tests.journal.test_checkpoint_request_ids -v
```

They cover exact history and physical savings, bounded chains, legacy-format
compatibility, transactional replay, corrupted dependencies, and snapshot/export/
restore/projection behavior. The fixed format-2 SQL fixture is version-controlled;
no private experiment journal is required. Existing journal and experiment
recovery suites exercise the same format through real writer/runner processes.

The standalone converter has three focused scenarios:

```text
uv run python -m unittest tests.journal.test_journal_converter tests.journal.test_checkpoint_request_ids -v
```

They cover committed WAL data, exact original JSON and auxiliary rows, cursor
gaps and sequence allocation, an active source's capture boundary, failures and
process interruption before publication, and normal replay/snapshot/restore of
the converted database. The CLI is also exercised from another working directory.
Fixtures are self-contained; no private runtime journal is required.

| Area | Command |
| --- | --- |
| Module storage | `uv run python -m unittest tests.storage.test_hash_db tests.modules.test_manager tests.storage.test_seaweed -v` |
| CLI discovery and downloads (direct library calls) | `uv run python -m unittest tests.experiments.test_reader tests.test_cli_library_reads tests.test_cli_discovery -v` |
| Runtime restart, mode changes and retained receipts | `uv run python -m unittest tests.server.test_runtime_restart tests.server.test_lifecycle tests.server.test_results -v` |
| Graceful server shutdown and response delivery | `uv run python -m unittest tests.server.test_shutdown -v` |
| Service control review regressions | `uv run python -m unittest tests.server.test_service_control_review -v` |
| Template reload | `uv run python -m unittest tests.experiments.test_template_reload tests.experiments.test_template_reload_services tests.experiments.test_template_reload_failures tests.experiments.test_template_reload_recovery tests.test_template_reload_cli tests.test_cli -v` |
| Participant protocol and modules | `uv run python -m unittest tests.participants.test_participant_protocol tests.participants.test_stage_client tests.experiments.test_service_dag_requests tests.participants.test_command_proxy -v` |
| Conditional stages | `uv run python -m unittest tests.experiments.test_conditional_stages tests.experiments.test_conditional_transitions tests.experiments.test_conditional_recovery -v` |
| Weather experiment | `uv run python -m unittest tests.test_weather_dag -v` |
| Resource collector | `uv run python -m unittest discover -s tests/resources -t . -p "test_*.py" -v` |
| Dashboard | `uv run python -m unittest discover -s tests/dashboard_tests -t . -p "test_*.py" -v` |

The weather timing smoke retains the real 40-second stage and 30-second
forecast timer. Its other scenarios use acknowledged fixture gates while
retaining real stage/service processes, TCP and SQLite. They release held work
after observing the relevant pause, recovery or accepted timeout. The next
forecast is still generated by the live service.

## Timing and focused validation

Functional concurrency checks synchronize on entry, readiness, accepted results
and completion. A fixed sleep is not evidence that another task has reached a
particular phase. Test guards bound hangs independently of the deadline being
verified; they do not change production timeouts.

Service deadline boundaries and recovery accounting also have isolated checks
with controlled observations. Real startup, heartbeat and command watchdog
cases remain. Clock substitutions are local to the tested module/operation and
do not replace the event loop's monotonic clock. Deadline expiry alone does not
authorize terminating a process whose ownership is unconfirmed.

Run changed areas separately and use unittest's duration report to identify
expensive cases:

```text
uv run python -m unittest tests.experiments.test_service_timeouts tests.experiments.test_service_recovery -v --durations 10
uv run python -m unittest tests.experiments.test_archiver_validation tests.experiments.test_snapshot_validation -v --durations 10
uv run python -m unittest tests.dashboard_tests.test_api_client tests.dashboard_tests.test_icmp tests.dashboard_tests.test_icmp_state tests.dashboard_tests.test_views -v --durations 10
uv run python -m unittest tests.test_weather_dag -v --durations 10
```

Durations include fixture preparation and cleanup. Archive and snapshot
validation reuse closed source images with independent copies per test;
publication, retention and rollback cases retain their own live runtimes.
Do not share mutable journals, processes or event loops between cases.

Keep failed runs when repeating race-sensitive cases. A later success does not
erase an earlier failure. Browser functional checks use acknowledged responses
and refresh callbacks; the actual periodic timer and the documented latency
limits remain separate observable requirements.

Conditional tests use real stage/service processes and the SQLite journal.
The single-writer tests also terminate or stall real writer processes at COMMIT
boundaries, check replay without duplicate events, exercise bounded queues and
heartbeat during recovery, and verify rollback, replacement storage, and
standalone idle shutdown. Fault instrumentation lives in test fixtures inside
the writer process. Temporary-directory cleanup stops only fixture-owned
standalone writers before removing their files on Windows.
They cover result errors and retries, exact move inputs, backwards/self visits,
pause/stop/step boundaries, service-call targets, template reload, protected
artifact reads, and snapshot/rollback/continuation. Owner-crash fixtures exercise
accepted results, committed moves, and already running targets. Fault injection
is confined to test helpers; production code has no test fault switches.

For conditional DAG display and refresh behavior:

```text
uv run python -m unittest tests.dashboard_tests.test_conditional_dag tests.dashboard_tests.test_conditional_browser -v
```

These checks cover current node badges versus historical attempt outcomes,
raw/RAM/disk-cache agreement, run/cycle isolation, cache format 1, and actual
runner progress fields. The focused browser check uses the production DAG
renderer and DOM updater in headless Chromium. It requires the same browser and
Node tooling as the existing browser suite and writes its screenshot under
`.artifacts/logs/dashboard-conditional-move.png`.

Reload tests exercise stable IDs, cursor and input changes, selective service
state transfer, journal preservation, receipts, and actual owner-process crashes.
Fault injection stays in test fixtures. The recovery matrix includes publication
and rollback boundaries; it can take several minutes. File-symlink cases skip
explicitly when the host cannot create them. Run the same suite in a native
Linux environment to validate Linux behavior; a Windows pass is not a Linux pass.

CLI discovery regressions reuse the storage, DAG and journal fixtures with
direct library calls. They cover optional service IDs, normalized module
references, event-loop progress during template loading, cancellation of
archive checks, inherited artifact history, and maintenance read saturation
without blocking state queries or shutdown. The same tests run on Windows
and Linux using native filesystem paths.

Runtime restart tests reuse the HTTP/process and receipt fixtures. They check
real controller replacement, active-stage/service shutdown, mode transitions,
retained receipts, client wait timeouts, failed shutdown/startup and stale IPC
callbacks. Fault injection is confined to tests; no production fault switches
or extra dependencies are required.

Service control regressions cover stopping during automatic retry delay,
readiness after partial service startup, and replaying retained commands/chains
across mode changes. They reuse the service/DAG, receipt and HTTP fixtures and
run on Windows and Linux without additional dependencies.

The HTTP/CLI and full-system DAG integration cases are part of normal discovery.
To run those suites separately:

```text
uv run python -m unittest tests.test_cli tests.test_system_dag -v
```

These checks start isolated local HTTP/CLI and participant processes with their
own temporary data. Do not point integration checks at valuable runtime data.

## Dashboard

Dashboard backend tests are included in the normal suite.
`tests/dashboard_tests/test_browser.py` uses headless Edge on Windows or a
Chromium-compatible browser (`google-chrome`, `chromium`, or `chromium-browser`
on PATH) on Linux, controlled through CDP with Node's built-in WebSocket API.
It explicitly skips when prerequisites are unavailable.
It does not add an external browser-test framework.

Tests select platform behavior from the current OS. They do not start Docker
or Docker Desktop. A Linux container may be started manually to provide the
test environment; install uv, Python 3.12, Node and the browser there, then
run `uv sync --locked` and the same unittest commands as on the host. Put uv
on PATH because integration fixtures launch child processes through it.
Use a Linux-local checkout/environment instead of reusing a Windows `.venv`.

The Heilbronn regression is self-contained: compressed extracted events and
fixed reference results live in `tests/dashboard_tests/fixtures/heilbronn`.
No original experiment directory or network access is needed to run it.

Browser checks use a 500-event history and require each sampled warm-page
navigation to finish within 200 ms. The first opening of a stopped uncached
history is measured for diagnostics without a latency assertion.
Each interval starts before navigation and ends after
the matching successful DOM update and two animation frames; loading and
error pages do not count as ready. A mutation observer detects readiness
without polling. The test samples eight warm pages five times each and one
cold opening. The normal periodic refresh stays active during measurement;
the forced refresh that checks row preservation starts after the cold sample.

Run latency checks without unrelated host benchmarks. Separate profiling runs
add overhead and should not be used as latency baselines. Raw and performance
reports are saved before assertions in a unique
`.artifacts/logs/dashboard-browser-<run-id>/` directory, with latest copies at
`dashboard-browser-results-<platform>.json` and
`dashboard-browser-performance-<platform>.json` in the logs directory.
Reports include navigation, DOM and frame timestamps, full HTTP resource
timings, refresh markers, environment details, and memory usage.

Resource tests include the
dashboard process and its cache subprocesses: 2,000,000,000 bytes with zero or
one caching experiment, 4,000,000,000 bytes with multiple concurrent builds.
A delay beyond 300 ms is allowed only while a large, previously uncached
experiment is actively being built,
with the initial-build notice visible in dashboard.

The live external ICMP scenario is opt-in. In PowerShell:

```powershell
$env:EMP_DASHBOARD_LIVE_ICMP = "1"
try {
    uv run --locked python -B -m unittest discover -s tests/dashboard_tests -t . -p "test_*.py" -v
} finally {
    Remove-Item Env:EMP_DASHBOARD_LIVE_ICMP
}
```

This scenario needs an actual Echo Reply from `www.google.com`.
Automated tests do not display desktop notifications or play audio.
For manual UI inspection, use a separate runtime project and configure both
`project_root` and `system_api_url`.

## Platform and environment requirements

Platform-specific cases select the actual host OS; a Windows run does not
validate Linux or WSL. Symbolic-link tests may skip on Windows without the
required privilege; junction checks do not replace file-symlink checks.
External ICMP availability depends on the network.

Linux process-termination fixtures require a Python build exposing
`os.pidfd_open`. If the selected build lacks it, recreate the project environment
with an installed Python 3.12 that provides it, for example
`uv sync --locked --python /usr/bin/python3.12` on Ubuntu 24.04. Continue running
tests through `uv run`; do not weaken process-identity checks to bypass this
environment requirement.

A successful process test or SQLite commit is not proof of durability under
power loss or storage failure. Report the command, OS, skips, and dependencies
alongside any validation result rather than relying on an old test count.

## Changing tests

Follow [AGENTS.md](../AGENTS.md). Before writing tests or implementation code,
create the feature's Markdown test plan.
Describe affected files, observable behavior, relevant boundaries and errors,
and open questions. Write test code only after the maintainer explicitly
confirms the plan. Existing approval does not authorize new cases automatically.

Write and run the approved tests before implementing the feature or behavior
change. Confirm that new behavior or regression tests fail for the expected
reason, then implement the behavior and run the tests again together with
relevant existing regression tests. For behavior-preserving refactoring,
establish passing coverage before changing production code. Do not start
implementation while test approval is pending.
