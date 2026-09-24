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

## Selected suites

| Area | Command |
| --- | --- |
| Module storage | `uv run python -m unittest tests.test_hashdb tests.test_modulemanager tests.test_seaweed -v` |
| CLI discovery and downloads (direct library calls) | `uv run python -m unittest tests.test_experimentreader tests.test_cli_library_reads tests.test_cli_discovery -v` |
| Runtime restart, mode changes and retained receipts | `uv run python -m unittest tests.test_runtime_restart tests.test_server_lifecycle tests.test_server_results -v` |
| Service control review regressions | `uv run python -m unittest tests.test_service_control_review -v` |
| Participant protocol and modules | `uv run python -m unittest tests.runner_utils.test_participant_protocol tests.runner_utils.test_stage_client tests.runner_utils.test_service_dag_requests tests.runner_utils.test_command_proxy -v` |
| Weather experiment | `uv run python -m unittest tests.test_weather_dag -v` |
| Resource collector | `uv run python -m unittest discover -s tests/resource_utils -t . -p "test_*.py" -v` |
| Dashboard | `uv run python -m unittest discover -s tests/dashboard_tests -t . -p "test_*.py" -v` |

The weather suite includes real timing intervals, processes, and journal
behavior; do not assume that all scenarios finish immediately.

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

An additional integration entry point is outside normal `test_*.py` discovery:

```text
uv run python -m unittest tests.integration_cli -v
```

It requires an available SeaweedFS executable and permission to start a local
service. Do not point integration checks at valuable runtime data.

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

Browser checks use a 500-event history and require each sampled ready-page
navigation, including the first opening of a stopped uncached history, to
finish within 200 ms; loading placeholders do not count. Run
latency checks without unrelated host benchmarks. Resource tests include the
dashboard process and its cache subprocesses: 2,000,000,000 bytes with zero or
one caching experiment, 4,000,000,000 bytes with multiple concurrent builds.
Reports are written under `.artifacts/logs/`. A delay beyond 300 ms is allowed
only while a large, previously uncached experiment is actively being built,
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

Follow [AGENTS.md](../AGENTS.md). Before adding or modifying tests, finalize the
feature and create its Markdown plan under `.artifacts/test-plans/`.
Describe affected files, observable behavior, relevant boundaries and errors,
and open questions. Write test code only after the maintainer explicitly
confirms the plan. Existing approval does not authorize new cases automatically.
