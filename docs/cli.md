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
| `maintenance` | Register, validate and remove modules with `module ...`. No experiment runner is started. |
| `run` | Execute and control experiments, read runtime resources and logs, work with snapshots and exchange archives. Module maintenance commands are rejected. |

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
| `--config`, `chain FILE`, JSON `@file`, `template create DESTINATION` | On the CLI machine; relative paths resolve from its current directory. |
| `module ... --folder`, `run --template`, archive paths and installation destination | On the server machine; pass absolute paths. The CLI does not upload these files. |

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
| `status` / `state` | Read selected state; `--experiment-id ID` selects saved state explicitly. `--watch [SECONDS]` repeats, default interval 1 second. |
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
