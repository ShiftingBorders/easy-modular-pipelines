<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
    <img src="docs/assets/logo.svg" alt="Easy Modular Pipelines" width="420">
  </picture>
</h1>

Easy Modular Pipelines is a Python framework for improving the repeatability
of AI experiments and making them easier to share. Build experiments from
versioned modules, describe their settings and execution in YAML, and inspect
results, logs, resources, and artifacts through the CLI or dashboard.
Reproducibility depends on both the framework's controls and the modules'
implementation and environment; it is not an unconditional guarantee.

> [!IMPORTANT]
> This is a new project under active development. Bugs are possible, and APIs,
> configuration, and stored experiment formats may change. Compatibility with
> older experimental formats is not guaranteed.

## What it does

- **Strict module identity and runtime integrity checks.** Templates pin each
  module by name, version, and SHA-256 hash. The hash covers packaged file paths
  and contents, including the module README. Different content cannot silently
  replace a registered version. Checks compare the experiment's module files
  with both the registered hash and the template at startup and during runtime
  preparation of stage attempts, service launches, and service calls. A mismatch
  fails validation. These are execution-boundary checks, not continuous file
  monitoring. See [module storage](docs/storage.md#versions-hashes-and-failures).
- **Execution controlled through commands.** Run sequential stages and
  long-lived services with explicit inputs, settings, timeouts, and retry
  policies. Pause, step, resume, stop, move the paused stage pointer, or rerun
  a stage when the runtime state permits it.
  See [execution commands](docs/cli.md#execute-and-control-experiments).
- **Snapshots, rollbacks, and recovery.** Create a snapshot at an idle pause
  and use `rollback` to restore saved experiment state and journal history.
  Recover an interrupted experiment, or create a continuation from a valid
  saved snapshot. Restoration of service state relies on the service's snapshot
  implementation. See [snapshots and recovery](docs/basic_dag.md#snapshots-recovery-and-exchange).
- **Experiment packaging, exchange, and installation.** Create a `tar.xz`
  exchange archive from a stopped or completed experiment, share it, inspect
  it, and install it into a new directory. Installation registers the required
  module packages and installs the applied template and static resources for
  another run. Exchange archives carry those inputs; snapshots serve runtime
  restoration. See [archive commands](docs/cli.md#snapshots-and-archives).
- **Execution history and observation.** Record operations and results in a
  shared journal, with a separate dashboard for history, live state, artifacts,
  resource monitoring, and alerts.

Stages currently run sequentially; branching and parallel stage execution are
not supported. See [current capabilities](docs/basic_dag.md#current-limits).

## Reproducibility and responsibility

The library checks module identity and manages execution, snapshots, and exchange.
Module authors remain responsible for dependencies, deterministic behavior,
complete restorable state, and external side effects. These controls improve
repeatability without guaranteeing identical results in every environment.
See [reproducibility and responsibility](docs/reproducibility.md) for the full
boundary and examples.

## Installation

### From source

Use Python 3.12 and `uv`. Clone the repository and install the locked environment:

```text
git clone https://github.com/ShiftingBorders/easy-modular-pipelines.git
cd easy-modular-pipelines
uv sync --locked
uv run python scripts/download_seaweedfs.py
```

Run project commands through `uv run` from this checkout. The managed module
store also needs the SeaweedFS executable: `core/seaweedfs/weed.exe` on Windows
or `core/seaweedfs/weed` on Linux. The download command installs the pinned
SeaweedFS 4.45 build for your Windows/Linux amd64 host and verifies its SHA-256.
The binaries are not stored in Git. Alternatively, configure an existing Filer
with `--filer-url`; see [module storage](docs/storage.md).

Follow the [quickstart](docs/quickstart.md) to register the example modules,
start the server, and run your first experiment.

### PyPI (soon)

A supported PyPI installation workflow is planned. Use the source installation
above for now.

## Documentation

| Task | Guide |
| --- | --- |
| Run your first experiment | [Quickstart](docs/quickstart.md) |
| Use commands, configuration and the interactive shell | [CLI reference](docs/cli.md) |
| Create a stage module | [Module authoring](docs/instructions/modules.md) |
| Create a service or external-process proxy | [Service authoring](docs/instructions/python_bridges.md) |
| Configure and run experiments | [Experiment guide](docs/basic_dag.md) |
| Understand the experiment YAML | [Template reference](docs/experiment_template.md) |
| Debug a run | [Debugging](docs/debugging.md) |
| Use the web interface | [Dashboard](docs/dashboard.md) |
| Browse all guides and API references | [Documentation index](docs/README.md) |

## Development

```text
uv run python -m unittest discover -s tests -p "test_*.py" -v
```

See [testing](docs/testing.md) for requirements and optional suites.
[AGENTS.md](AGENTS.md) defines contribution boundaries and the approval process
for adding or changing tests.
