# Debugging experiments

Start with an isolated project directory and a registered module version.
Use the same server configuration and API URL as the run you are investigating.
The examples below assume the default API address; add `--config <cli-json>`
before each subcommand for another server.

## Run one stage at a time

Start the server in run mode and wait for `health` to report readiness:

```text
uv run python -B cli.py health
uv run python -B cli.py run --template "<absolute-template-path>" --delayed-start --wait
uv run python -B cli.py status
uv run python -B cli.py step --wait
uv run python -B cli.py status
uv run python -B cli.py logs
```

Wait until startup has reached an idle pause before stepping. Services are
already running at that point. After each step, inspect the stage's input,
effective settings, result, and files before advancing.

`pause --wait` waits for the active attempt; it is not a process breakpoint.
`stop --wait` interrupts work and stops services. `resume --wait` returns to
normal execution. A completed experiment needs a new run or an experiment
rerun; it cannot simply be resumed.

For live observation, use separate terminals:

```text
uv run python -B cli.py status --watch 1
uv run python -B cli.py logs --follow
uv run python -B cli.py resources --watch 1
```

Stopping a watch/follow client with Ctrl+C does not stop the experiment.

## Find the relevant evidence

| Evidence | Where to look |
| --- | --- |
| Experiment directory | Project `experiments.json`, using the experiment ID. |
| Stage result and accepted outcome | Shared journal, via CLI logs or dashboard Events/Commands. |
| Effective settings | Recorded attempt parameters and dashboard Run settings. |
| Stage input and supplied paths | The attempt's `context.json`; treat it as runtime evidence, not an editable configuration. |
| Artifacts | `shared_artifacts/epoch_<n>/<module>/<stage-id>/attempt_<n>/`; result links are experiment-relative. |
| Application exceptions | Journal error events, traceback, captured stderr. |
| Service status and restarts | Status, journal events, dashboard Modules / Services. |
| Controller startup failures | The server terminal and its controller journal, when available. |

Keep experiment, stage, attempt, service-instance, and request IDs distinct.
The journal is the result store; do not search for `execution_result.json`.
A historical artifact record does not prove the file still exists after cleanup.

The [dashboard](dashboard.md) provides Events, Errors, Execution / DAG,
Run settings, Artifacts, and Commands views. Historical state alone does not
prove that a process is currently alive.

## Common problems

| Symptom | Check and next action |
| --- | --- |
| Connection refused or server still starting | Check API URL/port and the server terminal. Poll `health` until ready; managed SeaweedFS startup takes time. |
| `invalid_mode` | Module commands require maintenance mode; DAG commands require run mode. Restart the server in the appropriate mode. |
| Recovery required | Use run mode to recover and stop unfinished experiments before changing registered modules. |
| Module/hash conflict | Compare template name/version/hash and registered content. Any packaged file change, including README, affects the hash. Use a new version or a separate clean example project. |
| Template validation failure | Check required fields, UUID references, quoted versions, and actual registered hashes against the template reference. |
| Stage exits but fails | Check exit code and stdout. It must contain exactly one JSON object with `result` and `data`; banners and extra JSON invalidate it. |
| Service never becomes ready | Check its real startup work and first heartbeat, then `start_timeout`. A socket handshake is not readiness. |
| Timeout despite a later success | The DAG deadline includes queueing. Late success does not replace a timed-out accepted result. |
| Journal write fails | Check free space, permissions, expected journal identity, and the original error. Do not replace it with an empty database. |
| Missing artifact | Check experiment-relative vs attempt-relative paths and attempt retention. A journal entry can outlive its file. |
| Dashboard shows history but no live connection | Verify both `project_root` and `system_api_url` refer to the same runtime. |
| Windows access denied while publishing `state.json` | Shared JSON readers permit delete sharing; publication falls back to an atomic native rename when `os.replace` rejects an open reader. Transient read/write conflicts are retried for at most one second. Persistent errors still require checking permissions, external file locks and filesystem support. |
| Windows access denied while replacing an experiment during rollback | Directory publication retries transient sharing/access errors for at most one second per rename. A persistently open external journal still blocks restoration; close that reader before recovery. The restoration marker retains the completed file-move boundary. |

JSON readers close the file before parsing its contents. On Linux, publication
uses the normal atomic `os.replace`: an existing reader may finish reading the
old file while new readers see the new file. POSIX permission errors are not
treated as transient Windows sharing failures. The previous published JSON is
retained if publication fails; the runner also records its checkpoint in the
journal before updating the optional `state.json` copy.

A source-only `module validate --folder` validates the manifest, not runtime
behavior or stored archives. Use stored validation in maintenance mode when
investigating package integrity.

## Inspect a command without repeating it

```text
uv run python -B cli.py result <command-id> --wait
```

HTTP 202 means admission, not success. A CLI `--wait-timeout` only limits the
client's wait; it does not cancel server work. A missing response or an
unknown/expired receipt does not establish that work was never performed.
Inspect status and journal before deciding whether another action is appropriate.

Stage rerun and service retry are separate:

```text
uv run python -B cli.py rerun stage --position 1 --wait
uv run python -B cli.py retry 1 --wait
```

Use them only after reaching the required paused state and checking whether
previous external effects remain. Unresolved old work must be confirmed stopped
before repeating it.

## Debugger attachment

Stages and services run in their own processes. Attaching a debugger to the
CLI does not attach it to module code. Use the participant/subprocess identity
reported by status and attach your IDE to the actual module process.

For an isolated debugging template, give startup and stage deadlines enough
time for inspection. A stage can use `timeout_seconds: null`. Stopping a service's
event loop at a breakpoint can prevent heartbeat and trigger its failure policy.
Changing client wait time does not change these runtime deadlines.

The framework does not configure an IDE or install a debugger for you.
Follow repository dependency rules if adding debugger tooling.

Do not edit the registered module copy inside an experiment to add a breakpoint,
change settings, or bypass a hash check. Prepare changed source as a new version
and start a new experiment. For reusable diagnostics, use StageClient progress
and state reporting and structured [logging](logging.md).

## Snapshots and interrupted runs

### Reload failures

`template reload --wait` leaves the experiment paused. Inspect its receipt,
`status`, and journal before resuming. Validation failures leave the applied
template unchanged. Failures during application attempt restoration of the
protective snapshot; the reload still returns failure after a successful rollback.
This restores managed experiment files and service state, not arbitrary external
effects performed by services.

If protective snapshot creation fails before changes are applied, reload leaves
the experiment paused when the original services remain healthy and any snapshot
freeze has been released. For example, insufficient snapshot disk space does not
stop those services or discard DAG progress. The command still reports and logs
the failure; resolve its cause before retrying. Unconfirmed write resumption
continues to require stopping the experiment.

If the command was cancelled or the server stopped during application,
`pending_rebuild` identifies the unfinished operation and protective snapshot.
Use `recover <experiment-id> --wait`; it restores the last protected state and
retains the reload audit. Do not bypass recovery by editing state files, starting
another run, or issuing ordinary rollback. Unconfirmed participant termination
prevents file restoration.

For a service launched through a parent process, restoration also waits for
that launcher to finish cleanup. The runner retains launcher and participant
identities across recovery. If the launcher remains alive beyond `start_timeout`,
restoration fails before replacing runtime files; resolve its unfinished cleanup
and retry recovery.

Reload also waits for the launchers of changed or removed services before
replacing their runtime files, including after an ordinary recovery by a new
controller. This uses the saved launcher identity when no local process handle
is available. Unchanged services continue running during this wait.

If an older experiment has no saved launcher identity and the controller has
no local launcher handle, reload rejects changes to that service before creating
the protective snapshot or stopping services. The participant identity alone
does not prove that a parent launcher has finished. The experiment stays paused
with its applied template; changes that leave such services untouched remain
available. Do not fabricate launcher metadata to bypass this check.

For incompatible service data, inspect the service's `load_state` result and
the recorded old/new module versions. Matching names authorize transfer across
versions; they do not establish compatibility. Failed reload changes, errors,
and rollback observations remain available in the journal after restoration.

### Manual snapshots and recovery

At an idle pause, `snapshot --label <label> --wait` records a restoration point.
Use `rollback <snapshot-id> --wait` only when you intend to replace the current
experiment state and history. Refresh readers after the journal generation changes.

After a server interruption, `recover <experiment-id> --wait` reconciles the
existing run and surviving participants. `run --continue-from <experiment-id>`
instead restores a valid saved snapshot into a continuation. Neither operation
should be replaced by manually deleting locks, tokens, or state files.

When reporting a problem, include the command, module version/hash, experiment
and attempt IDs, relevant settings, platform, and the original error/traceback.
Use a reduced template and omit secret values. Raw templates and diagnostic
exports can contain application data.
