# Creating and running experiments

An experiment template selects versioned modules, their settings, and the
execution policies. Stages run in order. Services start before the DAG and stay
alive across its stages and pauses.

For a complete first run, use the [quickstart](quickstart.md). This guide
describes the same workflow for your own project.

## Project directory and server

Keep the framework checkout separate from the project directory that stores
your modules and runs. Create a project folder and a `server.json` in it:

```json
{
  "schema_version": 1,
  "project_root": ".",
  "server_mode": "maintenance",
  "host": "127.0.0.1",
  "port": 8000
}
```

Custom server settings extend [the defaults](../default_settings/webserver.json).
Relative paths in a configuration file are resolved from that file, not the
caller's current directory. Absolute configured paths are preserved.

From the framework checkout, start the server with your actual configuration:

```text
uv run python -B webserver.py --config "<absolute-project-path>/server.json" --mode maintenance
uv run python -B cli.py --url http://127.0.0.1:8000/api health
```

Use two terminals; the server stays running in the first. Wait for
`state: ready`. By default it prepares `hash_db/`, `seaweedfs/`, `modules/`,
and runtime directories. See [storage](storage.md) for the SeaweedFS executable
and external Filer option.

Maintenance mode handles module registration and validation. Run mode handles
experiments. Stop the server and restart it with the other `--mode` to switch.
Concurrent servers cannot own the same project root.

## Prepare modules and a template

1. Write a module following [module authoring](instructions/modules.md), or use
   an existing example.
2. [Register and validate it](storage.md#register-and-validate-a-module) on the
   maintenance server. Record `data.module.name/version/hash` from the result.
3. Create a local template draft:

```text
uv run python -B cli.py template create "<absolute-project-path>/experiment.yaml" --name my-experiment
```

This command does not contact the server. It refuses to overwrite an existing
file and creates an **incomplete draft** with an empty `stages` list.
Fill it using the [template reference](experiment_template.md). Neither
`template create` nor `module add` substitutes module hashes in your YAML.

A source module is registered once per name/version. Do not modify its files
during registration or change the code copied into a running experiment.

## Start and observe

Restart the server with the same configuration and `--mode run`, wait for
readiness, then run:

```text
uv run python -B cli.py run --template "<absolute-project-path>/experiment.yaml" --wait
uv run python -B cli.py status --watch 1
uv run python -B cli.py logs --follow
```

These commands use the default API address, `http://127.0.0.1:8000/api`.
For another server, put `--url <api-url>` or `--config <cli-json>` **before**
the subcommand. See [CLI settings](../default_settings/cli.json).

Each new `run --template` creates an experiment. The server owns execution;
closing the CLI does not stop it. `--wait` waits for the command result.
Inspect `status` to determine whether the experiment itself has completed.

Commands initially receive a receipt with a `command_id`. If a command is still
pending, query `cli.py result <command-id> --wait` rather than submitting it
again. A lost HTTP response does not prove the action was never performed.

## Control execution

To change server mode remotely, use `cli.py server mode maintenance --wait` or
`cli.py server mode run --wait`. To restart the runtime in its current mode, use
`cli.py server restart --wait`. These operations stop the selected experiment
and replace the controller and its owned resources while HTTP remains available.
They do not automatically resume an experiment or modify the configuration file.
See [runtime lifecycle](http_api.md#runtime-lifecycle).

| CLI command | Behavior |
| --- | --- |
| `run --template <path> --delayed-start --wait` | Prepare the experiment and services, then wait before the first stage. |
| `pause --wait` | Wait for the active attempt to finish; services remain alive. |
| `step --wait` | Execute the next stage from an idle pause, then pause again. |
| `resume --wait` | Continue normal execution. |
| `stop --wait` | Interrupt active work and stop owned services. |
| `rerun stage --position 1 --wait` | Rerun a stage from a permitted paused state. |
| `retry 1 --wait` | Restart service 1 from a permitted paused state. |
| `move 2 --wait` | Move the paused stage pointer to position 2. |
| `reset-retries stage 1 --wait` | Reset that stage's retry counter while paused. |

Positions are one-based. A service retry and a DAG-stage rerun are different
operations. The runtime validates the current phase and may reject a command.
Moving or rerunning can change available input; each module must validate it.
See [debugging](debugging.md) for a step-by-step workflow.

## Update the DAG while paused

Use `reload_template` to update the selected experiment without creating another
experiment directory. Pause at a stage-free boundary, then apply a template:

```text
uv run python -B cli.py pause --wait
uv run python -B cli.py template reload --template "<absolute-project-path>/updated.yaml" --wait
uv run python -B cli.py status
uv run python -B cli.py resume --wait
```

Without `--template`, reload reads the selected experiment's `experiment.yaml`.
Only `stages` and `services` may change. Keep the UUIDs of existing definitions;
missing IDs identify new definitions and receive generated UUIDs. Module versions
must already be registered and installed before entering run mode.

Reload creates a protective snapshot even when automatic snapshots are off.
It retains unchanged-prefix results and invalidates result references from the
first affected node onward. It rewinds executed work when necessary, preserving
an explicitly earlier cursor. It stays paused; `--wait` confirms the reload,
including service readiness, rather than completion of the remaining DAG.
Reloading an identical normalized template performs no rebuild.

Unchanged services keep running. For a restarted service with the same ID and
module name, saved state is loaded even across versions; the service checks
compatibility. A different module name starts with clean module data. A new ID
starts independently. A failed state load fails reload and attempts restoration
of the protective snapshot. See [reload recovery](debugging.md#reload-failures).

The journal records full templates, field changes, cursor/result decisions,
service actions, and rollback evidence. Prior attempt results remain historical
facts. See [template identity](experiment_template.md#identity-during-reload).

## Results and paths

The project's `experiments.json` maps IDs to experiment directories. Each
experiment has copied module code, runtime data, a journal, and stage artifacts.
A stage receives its paths through `--emp-context`; it must not derive them
from the current working directory.

Artifacts for a stage attempt live under:

```text
shared_artifacts/epoch_<n>/<module_name>/<stage_id>/attempt_<n>/
```

File references in stage result `data` are relative to the experiment directory.
Command results are stored in `journals/events.sqlite`; there is no
`execution_result.json`. Older artifact files may be removed by `keep_attempts`
even though their journal records remain.

## Snapshots, recovery, and exchange

Create a manual snapshot at an idle pause:

```text
uv run python -B cli.py pause --wait
uv run python -B cli.py snapshot --label before-change --wait
```

Use the returned snapshot ID with `rollback <snapshot-id> --wait` to restore
the paused experiment. Rollback replaces experiment state and journal history;
clients must refresh their journal generation.

After a server interruption, inspect `health` and `status` for recovery
requirements, then use `recover <experiment-id> --wait`. Recovery reconciles the
existing experiment and any surviving participants. Do not treat a saved PID
or missing endpoint as proof that the old work has stopped.

`run --continue-from <experiment-id> --wait` creates a continuation from a valid
saved snapshot. It is different from reconnecting to a live participant.

For a stopped experiment, the CLI also provides:

```text
uv run python -B cli.py archive create "<absolute-archive-path>" --wait
uv run python -B cli.py archive inspect "<absolute-archive-path>" --wait
uv run python -B cli.py archive install "<absolute-archive-path>" "<new-absolute-directory>" --wait
```

Archive paths and installation destinations belong to the server. The runtime
checks archive validity, space, and experiment state. A snapshot must be fully
valid before continuation or rollback. Automatic snapshot settings and final
snapshots are described in the [template reference](experiment_template.md).

## Current limits

- Stages are sequential; branching and parallel stage execution are unsupported.
- `replace` is not supported yet.
- StageClient currently has a synchronous public API.
- GPU/VRAM monitoring is unfinished and disabled.
- Old incompatible experiment, journal, and archive formats are rejected;
  automatic migration is not provided.
