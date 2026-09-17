# Quickstart

Use the [source installation](../README.md#installation). The complete
[weather example](../examples/weather_dag/README.md) includes the configuration,
modules, template, and shell commands needed below. It generates synthetic
weather locally; it does not call a weather API.

## 1. Register the example modules

From the repository root, start the example's maintenance server:

```text
uv run python -B webserver.py --config examples/weather_dag/server.json --mode maintenance
```

Keep this terminal open. In another terminal:

```text
uv run python -B cli.py --config examples/weather_dag/cli.json health
```

Wait for `state: ready`. The server initializes its HashDB and managed SeaweedFS
store; startup may take several seconds. Follow the example's
[registration commands](../examples/weather_dag/README.md#register-the-modules)
for PowerShell or Bash. They register both modules, read their returned hashes,
and create `examples/weather_dag/experiment.yaml`.

An unchanged module may already be registered. A changed module with the same
name/version is a conflict; give changed code a new version. Do not edit the
hash database to bypass this check.

## 2. Start the execution server

Stop the maintenance server with Ctrl+C and wait for it to exit. Start the same
project in run mode:

```text
uv run python -B webserver.py --config examples/weather_dag/server.json --mode run
```

Check `health` again and wait for readiness. Only one server may own this
project directory at a time.

## 3. Run the experiment

PowerShell:

```powershell
$weatherTemplate = (Resolve-Path examples/weather_dag/experiment.yaml).Path
uv run python -B cli.py --config examples/weather_dag/cli.json run --template $weatherTemplate --wait
uv run python -B cli.py --config examples/weather_dag/cli.json status --watch 1
```

Bash:

```bash
weather_template="$(pwd)/examples/weather_dag/experiment.yaml"
uv run python -B cli.py --config examples/weather_dag/cli.json run --template "$weather_template" --wait
uv run python -B cli.py --config examples/weather_dag/cli.json status --watch 1
```

The template path must be absolute and readable on the **server's machine**.
`--wait` waits for the launch command's outcome, not completion of the whole DAG.
Wait until status reports `phase: completed`. The example takes a little over
40 seconds. Ctrl+C in `status --watch` stops observation, not the experiment.

## 4. Inspect the result

`examples/weather_dag/experiments.json` maps the experiment ID to its directory.
The final stage returns `data.path` pointing to `weather.txt` under
`shared_artifacts/epoch_1/weather_stage/<stage-id>/attempt_1/`.

```text
uv run python -B cli.py --config examples/weather_dag/cli.json logs
```

To open the dashboard:

```text
uv run python -B -m dashboard --project-root examples/weather_dag --system-api-url http://127.0.0.1:8010/api/
```

Open <http://127.0.0.1:8765/>. The dashboard needs both the local project directory
for history and the matching API address for live state.

## 5. Stop or explore

To interrupt an active experiment:

```text
uv run python -B cli.py --config examples/weather_dag/cli.json stop --wait
```

After the experiment completes or stops, terminate the server with Ctrl+C.
For a controlled walkthrough, launch a new experiment with `--delayed-start`,
then use `step --wait`. See [debugging](debugging.md) and
[creating your own experiment](basic_dag.md).
