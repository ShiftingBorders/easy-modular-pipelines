# Creating modules

A module is an immutable, versioned folder containing `module.yaml`, its
implementation, and a README describing its contract. Start with a stage:
it accepts one input, performs work, returns one result, and exits.
Use a [service](python_bridges.md) for work that stays alive across stages.

## 1. Create a module folder

For example, create `modules/hello/1.0/` in your project:

```text
modules/hello/1.0/
  module.yaml
  main.py
  README.md
```

The server can also register an external source folder and install it into
`<project_root>/modules/<name>/<version>/`. Paths must not traverse symlinks
or junctions, and module files must not contain them.

Write this `module.yaml`:

```yaml
schema_version: 2
name: hello
version: "1.0"
role: stage
implementation: full
commands:
  start: [python, -B, main.py]
defaults:
  message: Hello from EMP
```

These seven top-level fields are required. A conditional stage additionally
declares `stage_kind: conditional`; other unknown fields are rejected.
`name` and `version` must be valid portable folder names.
Keep versions quoted so YAML reads them as strings.

| Field | Choices |
| --- | --- |
| `role` | `stage`: one attempt and an exit; `service`: a long-lived participant. |
| `implementation` | `full`: module implementation; `action`: a command-oriented implementation or proxy. |
| `commands` | Exactly one `start` argument array. No implicit shell. |
| `defaults` | A JSON-compatible settings object. |

Both full and action modules use the same role contract. A service's internal
start/stop actions belong to its implementation; do not add `commands.stop`
or `service_interface`.

## 2. Implement the stage

Put this in `main.py`:

```python
import argparse
from pathlib import Path

from core.journal.logger import OperationLogger
from core.participants.stage_client import StageClient


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emp-context", type=Path, required=True)
    arguments = parser.parse_args()

    with StageClient(arguments.emp_context) as client:
        message = client.settings["message"]
        if not isinstance(message, str) or not message.strip():
            raise ValueError("message must be a nonempty string.")
        if client.cancel_requested():
            client.fail({"reason": "cancelled"})
            return

        client.report_progress(0.5, "Writing the greeting")
        context = client.context
        artifact = Path(context["artifacts_directory"]) / "greeting.txt"
        artifact.write_text(message + "\n", encoding="utf-8")
        relative = artifact.relative_to(context["experiment_directory"]).as_posix()

        with OperationLogger(Path(context["logging_config_path"])) as logger:
            logger.record_artifact(
                "greeting.txt",
                purpose="greeting",
                size_bytes=artifact.stat().st_size,
            )
        client.succeed({"message": message, "path": relative})


if __name__ == "__main__":
    main()
```

The executor appends `--emp-context <absolute-json-path>` and supplies the core
SDK through `PYTHONPATH`. Python examples use the project's `uv` environment;
`-B` prevents bytecode writes into the immutable module directory.

Do not run this entry point without a runner-created context. StageClient
connects to its assigned executor; an arbitrary JSON file is not a standalone
execution environment.

The complete [counter example](../../examples/basic_dag/modules/counter/1.0/main.py)
also demonstrates repeated progress updates, state reporting, and cancellation.

## 3. Understand inputs, settings, and output

After opening StageClient:

- `client.settings` contains the effective module defaults and template overrides.
  Dictionaries merge recursively; lists and scalar values are replaced.
  YAML `null` becomes Python `None`.
- `client.input_data` contains the previous stage's accepted data, when available.
  The first stage should load its initial inputs from settings/resources.
- `client.context` supplies absolute experiment, resource, settings, module-data,
  and artifact directories, identifiers, and logging configuration.

Validate inputs, including missing data after a skip, move, or rerun. Saved
working files must not silently override explicitly supplied settings.

A successful attempt requires **exit code 0 and exactly one stdout JSON**:

```json
{"result": "success", "data": {"message": "Hello from EMP"}}
```

`client.succeed(data)` and `client.fail(data)` write this result once. They do
not exit the process; return from your code afterward. A failure result,
nonzero exit code, missing JSON, or invalid JSON fails the attempt.
Failure data is not passed to the next stage.

### Conditional stages

A conditional module keeps `role: stage` and adds `stage_kind: conditional`
to its manifest. Each template node using it must declare the boolean
`returns_data` outside `settings`. Ordinary stages and services omit both
conditional-specific fields.

Use the same StageClient and successful process-exit contract. The following
are alternative final results; publish exactly one and return:

```python
client.succeed(None)  # returns_data=false: continue with the original input.
client.succeed({"command": "pause"})  # returns_data=false
client.succeed({"command": "stop"})  # returns_data=false
client.succeed({
    "command": "move",
    "stage_id": client.settings["target_stage_id"],
    "data": {"value": 42},
})
```

`data` passed to `succeed` is a decision object, or `None` for no command when
`returns_data` is false.
The nested `data` is the application output. With `returns_data: true` it is
required, including in a decision without a command. An explicit `null` counts
as supplied data. A missing field fails the attempt, uses the configured retries,
and stops the DAG after they are exhausted, even if `on_exhausted` is `pause` or
`skip`. With a command, supplied data replaces the input.
With `returns_data: false`, nested data is ignored and the input is forwarded.
Supplying that disabled payload records a `conditional_unexpected_data` error
in the journal without failing the attempt or suppressing a valid command.
A valid decision without a command advances normally and forwards the input, even
if nested data was supplied. An empty stdout remains a failed attempt.

Only `pause`, `stop`, and `move` are supported. The experiment is implicit.
Only `move` accepts `stage_id`, which names an existing DAG node, including a
service-call node, an earlier node, or the conditional node itself. Unknown
commands, fields, and invalid targets fail the attempt under its error policy.
Commands in failed or timed-out attempts never take effect.

The runner accepts application output separately from the participant's raw
decision and applies it at the completed-stage boundary. See
[conditional execution](../basic_dag.md#conditional-execution) for pause, step,
retries, loops, and recovery behavior.

Send diagnostics to stderr or the [library logger](../logging.md). Do not print
banners, dependency installation output, or intermediate results to stdout.
For non-Python stages, implement the same context and final JSON contract;
the library executor still owns process control.

## 4. Keep code and runtime files separate

Never modify the module's source folder during execution. Environments, caches,
downloads, temporary files, generated settings, and outputs belong in the
runtime directories supplied by the runner. This includes files created by
third-party tools.

The stage's artifact directory is:

```text
shared_artifacts/epoch_<n>/<module_name>/<stage_id>/attempt_<n>/
```

Use the provided path; do not invent attempt numbering.
A result's internal file references are relative to the experiment root.
`record_artifact` in the example uses a path relative to the attempt directory.
These are different path bases; see [logging](../logging.md#artifacts).

Configuration-file paths are resolved from the configuration file.
Result links are resolved from the current experiment directory.
The runner does not rewrite arbitrary strings in result JSON when an
experiment is continued elsewhere.

Describe dependencies in the module README. Prepare them explicitly in allocated
runtime storage or document the preconfigured environment you require.
The runner does not build an environment dependency graph automatically.

## 5. Register and run

Start the project's server in maintenance mode and wait for readiness.
With an absolute source path on the server:

```text
uv run python -B cli.py module validate --folder "<absolute-project-path>/modules/hello/1.0" --wait
uv run python -B cli.py --json module add --folder "<absolute-project-path>/modules/hello/1.0" --wait
uv run python -B cli.py module validate --name hello --version 1.0 --wait
```

These commands use the default API URL. Supply `--config` or `--url` before
`module` if needed. Copy `data.module.hash` from the successful registration
response into the [hello template](../experiment_template.md#complete-stage-template).
Then restart the server in run mode and follow the [experiment guide](../basic_dag.md).

Source validation checks the manifest and folder; stored validation checks the
registered package. Neither proves the module's application logic is correct.

A registered name/version cannot be silently replaced. Changing any packaged
file, including the README, changes the module hash. Publish changed content
as a new version and update the template's name/version/hash reference.

## 6. Handle cancellation, repeats, and logging

Check `client.cancel_requested()` at safe points during long work.
Pause waits for the current stage; stop and timeout may interrupt it.
The executor can terminate a stage that ignores cooperative cancellation.

The runner chooses retries. Your module must describe any external effects
that may survive a timeout or interruption and what repeating the work does.
A new request ID does not make an external action exactly-once.

Each process opens its own logger client using the supplied configuration.
Do not share an open logger across processes or create ad hoc log files in
place of the journal. Full template settings are journaled; use a supported
reference to a separate secret source instead of embedding secrets in YAML.

## Module README checklist

Document purpose and role, all settings and dependencies, input/output schemas,
artifact path bases, persistent state, network access, repeat effects, and
owned processes and shutdown behavior.

For service modules, also document readiness, commands, interrupt behavior,
and snapshot completeness. See [service authoring](python_bridges.md) and the
[participant API](participant_protocol.md).

For work inside this repository, follow [AGENTS.md](../../AGENTS.md) and the
[test approval rules](../testing.md#changing-tests).
