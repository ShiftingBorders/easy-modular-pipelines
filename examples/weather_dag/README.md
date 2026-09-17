# Weather experiment: a complete project directory

This folder is a runtime project (`project_root`) with module source, server
and CLI settings, and an experiment template. It uses the framework checkout
and its `uv` environment; no additional Python dependencies are required.

The forecast is synthetic and needs no weather API or internet connection.
The server manages a local SeaweedFS store using `core/seaweedfs/weed.exe`
on Windows or `core/seaweedfs/weed` on Linux.

## What runs

The `weather_service` creates its first forecast before reporting readiness,
then updates it every 30 seconds. The DAG performs four steps:

1. `weather_stage` with `operation: wait`: wait 40 seconds, allowing cancellation.
2. A node referencing the live service: request its current JSON forecast.
3. `weather_stage` with `operation: format`: turn the forecast into text.
4. `weather_stage` with `operation: write`: write UTF-8 `weather.txt` and
   register the artifact.

Three stage nodes reuse one module with different settings. The example's data
and formatted text use Russian city/condition strings. This does not change
the interface language or JSON field names.

The complete run takes a little over 40 seconds and stops the service when done.

## Files

```text
weather_dag/
  modules/
    weather_service/1.1/
    weather_stage/1.1/
  experiment.template.yaml  # Source template with hash markers
  server.json
  cli.json
  hash_db/                  # Generated configuration, schema, and SQLite hashes
  seaweedfs/                # Generated module archive store
  experiments/              # Generated experiment directories
  snapshots/                # Generated snapshots
  controller/               # Generated controller state and journal
  experiments.json          # Generated experiment registry
  experiment.yaml           # Template filled with actual registered hashes
```

The source template and commands select version 1.1. Version 1.0 is also retained in modules/ for earlier registrations.

Generated state is excluded by this example's [.gitignore](.gitignore).

## Register the modules

Run all commands from the framework repository root. Install the environment:

```text
uv sync --locked
```

In terminal 1, start maintenance mode:

```text
uv run python -B webserver.py --config examples/weather_dag/server.json --mode maintenance
```

The server initializes missing HashDB settings and starts SeaweedFS.
In terminal 2, poll `health` until `state: ready`. Startup can take several
seconds. If the API is not yet reachable, repeat only the health check.

Use the commands for your shell below. They register both modules and replace
the template's hash markers with `data.module.hash` from the responses.
`installed: false` is normal because the modules already occupy their local
installation paths.

### PowerShell

```powershell
uv run python -B cli.py --config examples/weather_dag/cli.json health
$serviceFolder = (Resolve-Path examples/weather_dag/modules/weather_service/1.1).Path
$stageFolder = (Resolve-Path examples/weather_dag/modules/weather_stage/1.1).Path
$serviceJson = uv run python -B cli.py --config examples/weather_dag/cli.json --json module add --folder $serviceFolder --wait
if ($LASTEXITCODE -ne 0) { throw 'Service registration failed' }
$stageJson = uv run python -B cli.py --config examples/weather_dag/cli.json --json module add --folder $stageFolder --wait
if ($LASTEXITCODE -ne 0) { throw 'Stage registration failed' }
$serviceReply = ($serviceJson -join "`n") | ConvertFrom-Json
$stageReply = ($stageJson -join "`n") | ConvertFrom-Json
if ($serviceReply.result -ne 'success' -or $stageReply.result -ne 'success') {
    throw 'Module registration did not complete successfully'
}
$draft = Get-Content -Raw -Encoding UTF8 examples/weather_dag/experiment.template.yaml
$draft = $draft.Replace('__WEATHER_SERVICE_HASH__', $serviceReply.data.module.hash)
$draft = $draft.Replace('__WEATHER_STAGE_HASH__', $stageReply.data.module.hash)
$weatherTemplate = Join-Path (Resolve-Path examples/weather_dag).Path 'experiment.yaml'
[IO.File]::WriteAllText($weatherTemplate, $draft, [Text.UTF8Encoding]::new($false))
```

### Bash

```bash
uv run python -B cli.py --config examples/weather_dag/cli.json health
set -o pipefail
service_hash=$(uv run python -B cli.py --config examples/weather_dag/cli.json --json module add \
  --folder "$(pwd)/examples/weather_dag/modules/weather_service/1.1" --wait \
  | uv run python -c 'import json, sys; print(json.load(sys.stdin)["data"]["module"]["hash"])') || exit 1
stage_hash=$(uv run python -B cli.py --config examples/weather_dag/cli.json --json module add \
  --folder "$(pwd)/examples/weather_dag/modules/weather_stage/1.1" --wait \
  | uv run python -c 'import json, sys; print(json.load(sys.stdin)["data"]["module"]["hash"])') || exit 1
weather_template="$(pwd)/examples/weather_dag/experiment.yaml"
sed -e "s/__WEATHER_SERVICE_HASH__/$service_hash/g" \
    -e "s/__WEATHER_STAGE_HASH__/$stage_hash/g" \
    examples/weather_dag/experiment.template.yaml > "$weather_template"
```

Before leaving maintenance mode, validate the stored packages:

```text
uv run python -B cli.py --config examples/weather_dag/cli.json module validate --name weather_service --version 1.1 --wait
uv run python -B cli.py --config examples/weather_dag/cli.json module validate --name weather_stage --version 1.1 --wait
```

## Run the experiment

Stop the maintenance server with Ctrl+C and wait for exit. In terminal 1:

```text
uv run python -B webserver.py --config examples/weather_dag/server.json --mode run
```

In terminal 2, wait for `health` readiness again. Then, in PowerShell:

```powershell
$weatherTemplate = (Resolve-Path examples/weather_dag/experiment.yaml).Path
uv run python -B cli.py --config examples/weather_dag/cli.json run --template $weatherTemplate --wait
uv run python -B cli.py --config examples/weather_dag/cli.json status --watch 1
```

In Bash:

```bash
uv run python -B cli.py --config examples/weather_dag/cli.json run --template "$weather_template" --wait
uv run python -B cli.py --config examples/weather_dag/cli.json status --watch 1
```

`--wait` waits for the launch command, not the whole DAG. Look for
`phase: completed` in status. Ctrl+C in the watch client only stops observation.
Each new `run` creates a separate experiment ID.

The server uses `127.0.0.1:8010`. If changing the port, update both
`server.json` and `cli.json`. Paths stored in configuration are relative
to the containing file. Command template/module paths must be absolute paths
on the server.

From another directory, use `uv run --project <framework-checkout>` with
absolute paths to the entry points and configuration files.

## Inspect and control

The generated registry maps the ID to its experiment directory.
The last step returns `data.path` for:

```text
experiments/<run-folder>/shared_artifacts/epoch_1/weather_stage/
  f7cd487d-26f8-4142-974e-9029fbb30b14/attempt_1/weather.txt
```

The forecast travels through the DAG and its results are recorded in the
journal. The service's mutable `forecast.json` is under its `module_data`.

```text
uv run python -B cli.py --config examples/weather_dag/cli.json logs
uv run python -B cli.py --config examples/weather_dag/cli.json pause --wait
uv run python -B cli.py --config examples/weather_dag/cli.json step --wait
uv run python -B cli.py --config examples/weather_dag/cli.json resume --wait
uv run python -B cli.py --config examples/weather_dag/cli.json stop --wait
```

Use these controls while the experiment is active, rather than after completion.
For step-by-step execution, add `--delayed-start` to a new `run`.
Services become ready before the DAG waits for `step` or `resume`.
Pause lets the current attempt finish and keeps the service alive.
Stop interrupts work and stops the service.

Intermediate automatic snapshots are disabled. Finalization creates a final
snapshot. The service also supports manual snapshot and rollback operations.
Failures pause execution for inspection; service restart and DAG-request retries
have separate limits.

For the web interface:

```text
uv run python -B -m dashboard --project-root examples/weather_dag --system-api-url http://127.0.0.1:8010/api/
```

See [debugging](../../docs/debugging.md) and [dashboard](../../docs/dashboard.md).
After completion or stop, terminate the server in its terminal.

## Reusing the example

Unchanged `module add` returns `already_registered`. Any packaged content
change, including a README translation, changes its hash. Use new module versions
or a separate clean copy of this example to evaluate changed content.

Older example data prepared with a hash-only script is not a complete
registration. Preserve that old runtime separately and use clean storage for
the current workflow. Do not pair its old HashDB with a new empty SeaweedFS.

`template create` creates an empty draft for your own DAG; this example uses
its existing template. Neither it nor `module add` rewrites arbitrary YAML
hash references automatically.

The Bash instructions follow the same workflow; a Windows validation does not
establish that the Linux environment and executable have also been verified.
