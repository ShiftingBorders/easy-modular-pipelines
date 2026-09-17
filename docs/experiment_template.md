# Experiment template reference

Create a draft with `cli.py template create <destination> --name <name>`.
The command uses [version-controlled defaults](../default_settings/experiment_template.yaml)
and requires no server. Its empty stage list must be filled before execution.

Current templates use `schema_version: 2`. All top-level fields below are
required; unknown fields are rejected. The runner validates templates before
assembling an experiment.

## Complete stage template

This template matches the [hello module](instructions/modules.md). Replace the
hash marker with the 64-character hash returned by `module add` before running:

```yaml
schema_version: 2
name: hello-experiment
cycles: 1
keep_attempts: 1
start_timeout: 30
runner_timeout_margin_seconds: 2
stages:
  - stage_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    module:
      name: hello
      version: "1.0"
      hash: "<hash-returned-by-module-add>"
    settings:
      message: Hello from my experiment
    timeout_seconds: 30
    errors:
      retries: 0
      retry_delay_seconds: 1
      on_exhausted: pause
services: []
resources: []
unknown_state:
  timeout_seconds: 10
  on_timeout: pause
  recovery_limit: 3
  on_recovery_limit: stop
snapshots:
  mode: "off"
  keep: 2
storage:
  min_snapshot_free_bytes: 67108864
logging:
  busy_timeout_seconds: 5
  max_event_bytes: null
  min_free_bytes: 67108864
  filtered_refresh_interval_seconds: 1
```

The hash marker is deliberately not a valid hash. Obtain the actual registered
hash rather than substituting an arbitrary digest.

## Stages and settings

`stages` is a nonempty ordered list. Each ordinary node declares
`module: {name, version, hash}`, `settings`, `timeout_seconds`, and `errors`.
A `stage_id`, when supplied, is a UUID unique across stage and service definitions;
the assembler assigns missing definition IDs.

Module settings merge recursively with `module.yaml` defaults. Lists and scalars
replace earlier values, and `null` is an explicit value. The accepted result
data of a stage becomes the next stage's input. Modules must handle missing or
incompatible input themselves.

`cycles` and `keep_attempts` are positive integers. `keep_attempts` controls
retention of older attempts for the same stage in the current epoch, not
retention of journal history.

## Services

Declare each service once in `services`. For example:

```yaml
services:
  - service_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    module:
      name: my_service
      version: "1.0"
      hash: "<hash-returned-by-module-add>"
    settings: {}
    heartbeat:
      interval_seconds: 1
      grace_seconds: 10
    command_timeout_seconds: 30
    on_command_timeout: pause
    state_required: false
    errors:
      retries: 1
      retry_delay_seconds: 1
      on_exhausted: pause
```

To call it from the DAG, add a node to `stages`:

```yaml
- stage_id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
  service_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
  settings: {}
  timeout_seconds: 30
  errors:
    retries: 1
    retry_delay_seconds: 1
    on_exhausted: pause
```

These are fragments to merge into the complete template, not separate runnable
files. A service node uses `service_id` instead of `module`. Referenced services
must have explicit IDs. Several nodes can call the same instance.

Startup settings merge defaults with `services[].settings`. Per-node settings
are supplied separately with input data. Service request retries do not
automatically restart the service.

## Errors and deadlines

| Field | Meaning |
| --- | --- |
| Stage `timeout_seconds` | Positive seconds for the whole attempt, including queueing; `null` means no stage deadline. |
| `errors.retries` | Nonnegative automatic retry count. |
| `errors.retry_delay_seconds` | Nonnegative delay between retries. |
| `errors.on_exhausted` | `skip`, `pause`, or `stop` for stage failures; service policies are described below. |
| `start_timeout` | Positive startup deadline; also used as the separate shutdown deadline. |
| `runner_timeout_margin_seconds` | Cancellation and termination confirmation margin. |
| `unknown_state.timeout_seconds` | Positive wait before handling unresolved work. |
| `unknown_state.on_timeout` | `stop`, `pause`, `rerun`, or `skip`. |
| `unknown_state.recovery_limit` | Nonnegative recovery limit; zero immediately applies the limit action. |
| `unknown_state.on_recovery_limit` | `stop` or `pause`. |

Service `errors.on_exhausted` also accepts `skip`. In the current service
restart policy, it bypasses the automatic restart-count limit; it does not
skip a DAG node. Use `pause` or `stop` for a bounded restart policy.
`on_command_timeout` allows `pause`, `restart`, or `stop`.
Heartbeat intervals, grace periods, and service command timeouts are positive.
See [service authoring](instructions/python_bridges.md#readiness-and-timeouts).

A timed-out response cannot later become success. Retry/skip of unresolved
stage work requires confirmation that the previous attempt has stopped.

## Static resources

Each resource has `name`, `path`, and `hash`:

```yaml
resources:
  - name: dataset
    path: ./data/input.json
    hash: null
```

Relative resource paths are resolved from the template file's directory.
Absolute configured paths remain absolute. Resource names must be unique safe
path components. Resources are copied into the experiment. A supplied SHA-256
hash is checked on startup; `null` omits that resource hash check.
Module integrity checks remain mandatory.

Result artifact links follow a different rule: they are relative to the
experiment root. Do not resolve configuration paths from process cwd.

## Snapshots and journal

`snapshots.mode` is `off`, `after_stage`, or `after_epoch`;
`snapshots.keep` is a positive retention count. `off` disables intermediate
automatic snapshots, not finalization snapshots. Manual snapshots require an
idle pause. Stateful services must export complete state.

`storage.min_snapshot_free_bytes` is a nonnegative disk reserve.
The four configurable logging fields are explicit:

- `busy_timeout_seconds`: greater than 0 and at most 60.
- `max_event_bytes`: a positive integer or `null` for no additional event cap.
- `min_free_bytes`: a nonnegative disk reserve.
- `filtered_refresh_interval_seconds`: a positive refresh interval.

The runner supplies logging paths and journal identity; do not add client
connection fields to the template. Full template settings are recorded in the
journal. See [logging](logging.md) and the [weather template](../examples/weather_dag/experiment.template.yaml)
for a complete service-based example.
