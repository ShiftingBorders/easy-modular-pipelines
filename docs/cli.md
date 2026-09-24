# CLI reference

`cli.py` is an HTTP client for an independently running `webserver.py`.
It does not start the server. The `template create` command works locally
without a server connection.

Run commands through the repository's `uv` environment:

```text
uv run python -B cli.py --help
uv run python -B cli.py run --help
uv run python -B cli.py module add --help
```

See the [quickstart](quickstart.md) for initial setup and the
[weather example](../examples/weather_dag/README.md) for a complete project.

## Connection and server modes

Place global options before the command:

```text
uv run python -B cli.py --config examples/weather_dag/cli.json health
uv run python -B cli.py --url http://127.0.0.1:8010/api --json status
```

The default API URL is `http://127.0.0.1:8000/api`. The weather example uses
port `8010`; its dashboard is a separate application on port `8765`.
Point the CLI at the system API, not the dashboard.

| Server mode | Available workflow |
| --- | --- |
| `maintenance` | Register, validate and remove modules. No experiment runner is started. |
| `run` | Execute and control experiments, read runtime resources and logs, work with snapshots and exchange archives. Module mutations and `module validate` are rejected. |

Module list/inspect, template validation, saved metadata, artifacts and retained
command receipts are readable in both modes.

Start the appropriate mode in another terminal:

```text
uv run python -B webserver.py --config examples/weather_dag/server.json --mode maintenance
```

To switch modes, stop that server with Ctrl+C, wait for exit, and start it
again with `--mode run`. After each start, poll `health` until `state: ready`.
An HTTP response or CLI exit code 0 alone does not establish readiness.

`stop` stops the selected experiment and its services. It does not shut down
webserver or dashboard. Terminate those applications in their own terminals.

## Settings and global options

Defaults come from [default_settings/cli.json](../default_settings/cli.json).
A file supplied with `--config` overrides those defaults; explicit connection
options override the file. Unknown settings are rejected.

| JSON setting | Default | CLI override / meaning |
| --- | --- | --- |
| `schema_version` | `1` | Supported configuration schema. |
| `server_url` | `http://127.0.0.1:8000/api` | `--url URL`; include `/api`. |
| `token_env` | `null` | `--token-env NAME`; environment variable containing the bearer token. |
| `request_timeout_seconds` | `15` | `--request-timeout SECONDS`; deadline for one HTTP request. |
| `wait_timeout_seconds` | `60` | `--wait-timeout SECONDS` on execution commands; total client wait for the command outcome. |
| `poll_interval_seconds` | `0.25` | Interval between result polls and idle `logs --follow` requests. |
| `max_response_bytes` | `33554432` | Maximum response body size, 32 MiB. |

Timeouts and polling intervals must be positive. `--json` selects machine-readable
output; `--help` describes the current parser. Store the environment-variable
name in configuration, not the token value. The client reads the token from its
own environment; see [HTTP authentication](http_api.md#configuration-and-authentication).

## Filesystem paths

| Argument | Where the file exists |
| --- | --- |
| `--config`, `chain FILE`, JSON `@file`, `template create DESTINATION`, `artifact get --output` | On the CLI machine; relative paths resolve from its current directory. |
| `module ... --folder`, `run --template`, `template validate PATH`, archive paths and installation destination | On the server machine; pass absolute paths. The CLI does not upload these files. |

For a local PowerShell session:

```powershell
$template = (Resolve-Path examples/weather_dag/experiment.yaml).Path
uv run python -B cli.py --config examples/weather_dag/cli.json run --template $template --wait
```

In Bash, from the repository root:

```bash
uv run python -B cli.py --config examples/weather_dag/cli.json run \
  --template "$(pwd)/examples/weather_dag/experiment.yaml" --wait
```

The generated `experiment.yaml` must already contain registered hashes.
Do not run the example's source template with unresolved hash markers.

## Waiting, command IDs and output

Submission commands accept `--wait`, `--no-wait`, `--wait-timeout SECONDS`,
and `--command-id UUID`. `chain` uses `--chain-id UUID` instead, and `result`
looks up its positional command UUID.

- A normal one-shot submission waits by default. Inside `shell`, submissions
  return after admission by default. Explicit `--wait` or `--no-wait` overrides this.
- `result UUID` reads once by default; add `--wait` to poll a pending command.
- `--wait` waits for the command response, not for the entire DAG. For `run`,
  inspect `status` until the experiment reaches its intended phase.
- `--wait-timeout` and Ctrl+C interrupt client waiting without cancelling
  accepted server work. Use `stop` when you intend to stop the experiment.
- The CLI prints submission IDs to stderr before sending. Keep these IDs if
  the connection fails. It does not automatically resend commands.

```text
uv run python -B cli.py run --template "<absolute-template-path>" --no-wait
uv run python -B cli.py result <command-uuid> --wait --wait-timeout 120
```

Successful admission is not successful execution. Responses distinguish
`pending`, `succeeded`, `failed`, `cancelled`, `unknown` and `unavailable`.
An identical explicit command ID may retrieve a retained identical submission;
it is not a permanent exactly-once guarantee. After uncertainty, inspect the
result, state and journal before issuing new work.

With `--json`, documents are written to stdout. Watch/follow output uses JSON
Lines. Errors and command-ID notices go to stderr, so keep the streams separate
when parsing JSON. Without `--json`, status uses a compact summary and other
responses may still be displayed as formatted JSON.

## Local template creation

```text
uv run python -B cli.py template create ./my-experiment.yaml --name my-experiment
```

Creates a new draft from the bundled defaults, creates missing parent directories,
and refuses to overwrite an existing destination. Fill its stages/services and
registered module hashes before running it. This command neither registers
modules nor fills their hashes automatically.

## Template validation and module discovery

These operations work in both server modes and do not create experiments:

```text
uv run python -B cli.py template validate "<absolute-template-path-on-server>"
uv run python -B cli.py module list
uv run python -B cli.py module inspect --name weather_stage --version 1.1
```

`template validate` checks the existing template schema, registered module hashes
and archive presence. Its scope is `structure_and_registered_references`: it does
not execute modules, inspect archive contents, verify installed code, or verify
resource files. Relative resource paths resolve from the template directory.
Use `module validate` in maintenance mode to check stored package integrity.
The result includes `warnings`: services without an explicit `service_id`
produce a warning while validation remains successful. Assembly generates
their IDs; services referenced by DAG nodes still require explicit IDs.

`module list` returns registered name/version/hash references, ordered by name
and version. `module inspect` returns the reference, archive availability and
local installation location/presence; `integrity: not_checked` distinguishes this
from package validation. Storage outages fail the request instead of reporting
a missing archive. These reads do not accept command-wait options.

## Module maintenance

These commands require a maintenance server:

| Command | Behavior |
| --- | --- |
| `module add --folder PATH` | Read name/version from `module.yaml`, register the archive and hash, and install the local module copy. |
| `module validate --folder PATH` | Validate a source manifest; this does not test execution or validate a registered archive. |
| `module validate --name NAME --version VERSION` | Check the registered hash and stored archive; return the module reference. |
| `module remove --name NAME --version VERSION` | Remove the registered archive and hash; local source files remain. |

`--folder` cannot be combined with `--version`; `--name` requires `--version`.
Successful add/stored validation returns `data.module` with `name`, `version`
and `hash` for the experiment template. Re-registering an identical package
returns `already_registered`. Changed package contents require a new version.

```text
uv run python -B cli.py --config examples/weather_dag/cli.json module validate --name weather_stage --version 1.1 --wait
```

Recover and stop unfinished experiments before changing modules. For storage
setup and version rules, see [module storage](storage.md).

## Read state, resources and logs

| Command | Options and behavior |
| --- | --- |
| `health` | Read server mode, storage initialization and controller health. |
| `status` / `state` | Read selected runner state; `--experiment-id ID` must match its ID. `--watch [SECONDS]` repeats, default interval 1 second. Use `experiment inspect ID` for historical saved state. |
| `resources` | Read current resource observations. Supports `--watch [SECONDS]`. |
| `resource-history` | `--after N` (default 0, nonnegative), `--limit N` (default 100, range 1–1000). |
| `logs` | `--experiment-id ID`, `--cursor JSON_OR_@FILE`, `--limit N` (default 100, range 1–1000), `--follow`. |

Without `--experiment-id`, `logs` resolves the selected experiment when it starts.
Without `--follow`, it returns one page. Follow mode advances the returned
checkpoint and waits between requests when caught up. Treat checkpoints as opaque
JSON, including their journal generation; do not construct them from timestamps.

```text
uv run python -B cli.py --json status --watch 1
uv run python -B cli.py --json logs --follow
uv run python -B cli.py logs --cursor "@checkpoint.json" --limit 500
```

These examples use the default server. Add `--config examples/weather_dag/cli.json`
before the command for the weather server.

## Browse saved experiments and snapshots

These read-only commands work in both server modes:

| Command | Behavior |
| --- | --- |
| `experiment list` | List registered experiments, saved name/phase/mode, directory, availability and whether the runner currently selects them. |
| `experiment inspect ID` | Read the published state and directory of a registered experiment, including historical experiments. |
| `snapshot list [--experiment-id ID]` | List published snapshot metadata for an experiment. |
| `snapshot inspect UUID [--experiment-id ID]` | Read a snapshot manifest and its saved cycle/stage position. |

Without `--experiment-id`, snapshot reads use the selected experiment in run
mode. Maintenance mode has no selection and requires an explicit ID.
These commands neither select nor restore an experiment. Saved state is labelled
`source: saved_state` and does not prove that processes are currently alive.
Use `status` for the runner's current state.

Snapshot reads check metadata identity and supported schema, not file hashes or
restorability. Their `integrity: not_checked` is not a validation result.
Lists retain unreadable entries with `available: false` and an error; inspection
of such an entry fails. An empty snapshot list means no published manifests
were found. Oversized responses are rejected by the existing server limits.

```text
uv run python -B cli.py experiment list
uv run python -B cli.py experiment inspect <experiment-id>
uv run python -B cli.py snapshot list --experiment-id <experiment-id>
uv run python -B cli.py snapshot inspect <snapshot-uuid> --experiment-id <experiment-id>
```

The existing `snapshot --label TEXT --wait` still creates a snapshot. Snapshot
`list` and `inspect` do not accept creation or command-wait options.

## Execute and control experiments

These commands require run mode. Stage and service positions are **1-based**
and refer to their respective arrays in the template. Most controls act on
the currently selected experiment; they do not accept `--experiment-id`.

| Command | Behavior and constraints |
| --- | --- |
| `run --template PATH` | Create a new experiment. Optional `--experiment-id ID` supplies its unique ID. |
| `run --continue-from ID` | Create a continuation from a valid saved snapshot. Mutually exclusive with `--template` and cannot use `--experiment-id`. |
| `run ... --delayed-start` | Prepare services, then wait before executing DAG stages. |
| `pause` | Finish the current attempt and pause progression. Services keep running. |
| `resume` | Resume a paused experiment. |
| `step` | Execute one stage from the paused position. |
| `stop` | Interrupt active work, stop services and finalize the experiment. |
| `rerun stage --position N` | Rerun the stage at the paused, idle pointer. Does not accept `--experiment-id`. |
| `rerun experiment [--experiment-id ID]` | Create a new delayed-start run from the selected/specified experiment's saved template. Does not accept `--position`. |
| `retry N` | Manually restart service N in a paused experiment. This is distinct from rerunning a stage. |
| `move N` | Move the paused, idle stage pointer. |
| `reset-retries stage N` / `reset-retries service N` | Reset the respective retry counter while paused. |
| `recover ID` | Reconcile an existing experiment after loss of its owning runtime, including surviving participants. |

`recover` reconciles existing work; `--continue-from` creates a continuation.
Commands can fail when the experiment phase does not permit them. See the
[experiment guide](basic_dag.md) and [debugging guide](debugging.md).

## Snapshots and archives

| Command | Behavior |
| --- | --- |
| `snapshot [--label TEXT]` | Create a restorable snapshot at a paused boundary with no active attempt. |
| `rollback SNAPSHOT_UUID` | Restore the selected experiment from a valid snapshot. |
| `archive create PATH [--experiment-id ID]` | Create a `tar.xz` exchange archive from a stopped/completed experiment. |
| `archive inspect PATH` | Validate an exchange archive without installing it. |
| `archive install PATH DESTINATION` | Register required packages and install template/resources into a new directory. |

Archive paths belong to the server. Snapshots contain runtime state for restoration;
exchange archives package code, the applied template and static resources for
another run. They serve different purposes.

## Retained commands and artifacts

```text
uv run python -B cli.py commands list --state failed --limit 50
uv run python -B cli.py commands list --command run --after <command-uuid>
uv run python -B cli.py artifact list --experiment-id <experiment-id>
uv run python -B cli.py artifact get <artifact-id> --experiment-id <experiment-id> --output ./result.bin
```

`commands list` returns receipt summaries in submission order. Filters are
`--state` and exact controller name `--command`; `--limit` defaults to 100 and
accepts 1–1000. Continue with `--after` set to `next_after`, keeping the filters.
An expired cursor requires restarting pagination. Receipts belong to the current
server instance and are not the permanent history. Use `result ID` for details.

`artifact list` reads `artifact.recorded` events and their attempt context from
the experiment journal. Entries report current file availability and path errors;
recorded sizes/hashes are metadata, not a new integrity check. It scans the
journal through the boundary observed at the start of the request. Large lists
remain subject to the existing read timeout and JSON response limits.

Continuations include inherited artifact records with their original experiment
IDs in the event context. File availability and downloads use the continuation's
own experiment directory, including files restored from its source snapshot.

`artifact get` downloads the first recorded matching artifact ID. Only recorded
files within their attempt directory can be downloaded. The destination is local,
its parent directory must exist, and an existing destination is never replaced.
The client streams into a temporary directory beside the destination, then
publishes the complete file using a hard link; the destination filesystem must
support hard links. Failed/interrupted transfers remove temporary data.
`--request-timeout` also bounds downloads; the JSON size limit does not limit
binary artifact size. No command receipt is created for these reads.

## Generic commands and chains

`command NAME` sends a supported controller command using `--args JSON_OR_@FILE`
(default `{}`) and optional `--target JSON_OR_@FILE`.
Using a file avoids shell-specific JSON quoting. For example, save this target
as `target.json` on the CLI machine:

```json
{"kind": "stage", "position": 1}
```

Then, while paused:

```text
uv run python -B cli.py command reset_retries --target "@target.json" --wait
```

Generic controller names use underscores where applicable. The generic command
does not bypass mode or state checks. In particular, `replace` and
`reload_template` currently return `unsupported_feature`.

`chain FILE` reads a nonempty JSON command array or an envelope with `commands`
and optional `chain_id`. For an active run, a checkpoint chain can contain:

```json
[
  {"command": "pause", "args": {}},
  {"command": "snapshot", "args": {"label": "inspection checkpoint"}}
]
```

```text
uv run python -B cli.py chain checkpoint-chain.json --wait --wait-timeout 120
```

The CLI supplies missing command IDs. `--chain-id UUID` overrides the envelope's
chain ID. The server executes the commands in sequence; a failure cancels later
commands in the chain. This is not a transaction that undoes earlier effects.
The client wait deadline covers the whole chain. Query individual command IDs
with `result`; a chain ID is not a command-result lookup ID.

## Interactive shell

```text
uv run python -B cli.py --config examples/weather_dag/cli.json shell
```

At `emp>`, enter subcommands without `uv run python cli.py`:

```text
status
pause --wait
snapshot --label "manual checkpoint" --wait
resume
result <command-uuid> --wait
quit
```

Submissions are asynchronous by default in the shell. `help` shows available
commands; `quit`, `exit` and EOF leave the client without stopping server work.
Connection settings are fixed when entering the shell; nested shells are rejected.
Ctrl+C during a wait interrupts the client operation, not the accepted command.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Successful read/local action, successful command, or accepted/pending submission when not waiting. |
| `1` | Server rejection or failed/cancelled command outcome. |
| `2` | Invalid CLI arguments, configuration or local input. |
| `3` | Connection/protocol failure, HTTP timeout, oversized response, server-instance change, or unknown/unavailable outcome. |
| `4` | Client command-wait deadline expired; server execution was not cancelled. |
| `130` | Ctrl+C interrupted a one-shot CLI operation. |

Inspect `$LASTEXITCODE` in PowerShell or `$?` in Bash immediately after a command.
The interactive shell reports individual errors and remains open; do not use its
final exit status to aggregate prior command outcomes. For wire-level details,
see the [system HTTP API](http_api.md).
