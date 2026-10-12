<p align="center">
  <img src="assets/logo-observe-dark.svg" alt="Execution history and observation" width="420">
</p>

# Execution history and observation

Follow current execution and inspect its recorded history: operations, accepted
results, errors, settings, resources, and artifact metadata. Each experiment
uses a shared journal, with a separate logger client for each process.

## Observe through the CLI

With a ready run-mode server:

```text
uv run python -B cli.py status --watch 1
uv run python -B cli.py logs --follow
uv run python -B cli.py resources --watch 1
```

Run each continuous view in a separate terminal. Use `--config <cli-json>` or
`--url <api-url>` before the subcommand for a non-default server.
See [state, resources, and log commands](cli.md#read-state-resources-and-logs).

Command receipts identify submitted work; inspect the command result to learn
its outcome and experiment status to learn whether the run has finished.

## Explore through the dashboard

The [dashboard](dashboard.md) provides execution history, live state, artifacts,
resource charts, settings, command outcomes, snapshots, and alerts. Start it
separately and connect its journal and API sources; starting the dashboard does
not start the runtime.

The journal is the source of truth. Dashboard history reads do not modify it;
explicit control commands go to the runtime. Artifact metadata can outlive a
file removed by retention, so a recorded artifact may no longer be downloadable.

## Record and interpret evidence

- [Logging](logging.md) explains operations, errors, progress, artifacts, and
  journal readers. Module stdout is reserved for the final stage result.
- [Resource monitoring](resources.md) explains CPU, RAM, disk, and network
  measurements, configuration, and missing values.
- [Dashboard alerts](dashboard.md#icmp-and-notifications) cover thresholds,
  error rates, ICMP, and notifications.
- [Debugging](debugging.md#find-the-relevant-evidence) connects commands,
  attempts, errors, and artifacts when investigating a failed run.

Historical records and saved state do not prove that a process is currently
alive. Missing measurements remain unknown rather than becoming zero.
After a [rollback](restoration.md), readers must refresh their journal
generation instead of combining old and restored history.

Return to [project features](../README.md#what-it-does) or the
[documentation index](README.md).
