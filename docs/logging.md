# Logging and reading experiment history

Each experiment uses a shared local SQLite journal. Each process opens its own
`OperationLogger` client; do not pass an open client across process boundaries.
One separate writer process owns the primary database. The synchronous logging
API and returned event IDs are unchanged; active clients communicate with that
writer over an authenticated local connection.

Modules receive an absolute `logging_config_path` in their runtime context.
Use that configuration instead of creating another journal.

## Events and errors

```python
from pathlib import Path

from core.journal.logger import OperationLogger

# context is the runner-provided module context.
with OperationLogger(Path(context["logging_config_path"])) as logger:
    logger.record_event("module.message", {"text": "Preparing input"})
    try:
        # Perform the module's work here.
        pass
    except Exception as error:
        logger.record_error(error, include_traceback=True)
        raise
```

Use `record_event` for application JSON. Dedicated methods handle operations,
errors, resources, progress, artifacts, templates, parameters, and command
results. Do not write reserved event types using `record_event`.

`logger.operation(operation_type, operation_name)` is a context manager. Explicit
`start_operation`/`finish_operation` are also available.
Recording an error does not itself finish an operation.

JSON must contain finite numbers, valid Unicode, and no cycles; validated object
nesting is limited to 32 levels. Data is copied before storage and is never
silently truncated.

A stage's stdout is reserved for its final result. Use this logger or stderr
for diagnostics. External command output is captured by its executor/proxy.
Do not create independent text log files in place of the experiment journal.

## Progress and resources

Stage modules can use `StageClient.report_progress(value, message)` and
`report_state(data)`; see the [participant API](instructions/participant_protocol.md).
Progress is between 0 and 1. Reported state supports observation, not automatic
restoration of the module's memory.

`OperationLogger.record_resources` accepts named measurements with `value`,
`unit`, `kind`, `scope`, `estimated`, and `attributes`.
Kinds are `delta`, `total`, `gauge`, and `peak`; scopes include
`operation`, `process`, `service`, and `host`.
Operation-scoped measurements need an operation handle.
A missing measurement is `null`, not a made-up zero.
See [resource monitoring](resources.md) for automatic system metrics.

## Artifacts

Register a file after completing its write:

```python
logger.record_artifact(
    "greeting.txt",
    purpose="greeting",
    size_bytes=artifact.stat().st_size,
)
```

In a stage's logger context, this path is relative to its **attempt directory**.
In contrast, a path in final stage result `data` is relative to the
**experiment directory**. Keep these bases distinct.

Artifact registration records metadata; it does not read the file or prove
stage success. Dashboard downloads require enough recorded attempt context,
an in-bounds path, and an existing file. Retention can remove the file later.

## Reading

Start with CLI `logs` / `logs --follow` or dashboard Events and Errors.
For library readers, use an existing runner-provided logging configuration:

```python
from core.journal.logger import OperationLogger

with OperationLogger(existing_config_path, read_only=True) as logger:
    page = logger.read_events(limit=100, view="effective")
    checkpoint = page["checkpoint"]
```

`existing_config_path` must be absolute and identify an existing journal.
Read-only clients do not create a missing journal, modify events, or repair a
schema. SQLite may still use its WAL auxiliary files.

| Method | Use |
| --- | --- |
| `get_journal_info()` | Read schema version, journal ID, and generation. |
| `read_events(checkpoint=None, limit=100, view="raw")` | Page through raw or effective events. |
| `read_changes(checkpoint=None, limit=100)` | Observe result confirmations and other changes, including those without a new event. |
| `read_event_batch(event_ids=None, limit=1000, before=None)` | Read selected original events by indexed ID, or a descending cursor page of the journal tail. |
| `read_command_result(request_id)` | Read the considered command outcome and participant observations. |

Event pages contain `events`, `checkpoint`, `boundary`, and `has_more`.
Change pages contain `changes` and their own checkpoint.
Limits are 1–1000. Continue with the returned checkpoint while `has_more` is true,
even when an effective page has no entries.

Checkpoints include journal identity and generation. A rollback changes the
generation; `JournalGenerationChanged` reports this explicitly. Refresh the
reader's connection information and start a new history view. Do not combine
old and new generations or silently reuse an old write client.

`read_event_batch` returns `events` and the observed `boundary`. ID lookup uses
the existing event ID unique index; tail lookup uses the cursor primary key.
The byte budget can shorten either page. For ID lookup, request any remaining
IDs explicitly; for a tail page, continue with `before` equal to the smallest
returned cursor. An absent ID is not fabricated. This API adds no journal
tables, columns, indexes, or schema migration. It returns original raw events;
use the change feed for effective-result and confirmation metadata.

## Results and reliability

The journal is the sole store of participant call results; artifact data remains
in files. The participant records observed completion, while the runner records
its accepted outcome. The runner's outcome has priority; a late response after
timeout remains diagnostic evidence rather than a new success.

For conditional stages the participant observation retains the raw decision.
The accepted runner response stores the effective application output in `data`
and the validated decision in `execution.dag_decision`. `stage.finished` reports
that accepted response. Runner checkpoints record the applied decision, cursor,
pending input reference, and protected artifact paths together. Attempt
checkpoints also retain input and retry ownership before the next DAG transition.
Control decisions are applied once per accepted attempt; each loop visit has
fresh attempt IDs. Result history does not imply that a superseded visit remains
an input source for the current cursor.

StageExecutor and ParticipantServer write their results. Module authors should
not write a competing `record_command_result` for the same request.

The runner records the template and effective attempt settings before work.
Secrets embedded in settings enter history; use supported references to separate
secret sources when needed. Each event retains the identity of the process that
emitted it, rather than the storage process.

A returned event ID confirms a database COMMIT. If the connection disappears
before the acknowledgement, the client retries the same event ID. The writer
reconciles a committed request without inserting a second event or result.
Unrecoverable standalone storage errors still raise `LoggingStorageError`;
validation and journal-generation errors remain explicit.

## Writer lifetime and recovery

EMP supervises its writer independently of the experiment event loop and the
SQLite worker thread. Process death is checked every 250 ms by default; the
detection budget is two seconds, with a target below one second. A separate
progress check detects stalled SQL work. Restart attempts use a 60-second
recovery budget. The waiting write queue holds 100 requests by default; full
queues apply backpressure without reporting uncommitted writes as successful.

During recovery, journal calls wait and DAG scheduling pauses. Module deadlines
exclude the observed recovery interval. Service heartbeat remains active, and
EMP can still stop owned processes. When the same journal becomes available,
pending requests and DAG execution continue automatically. A user pause remains
in effect. If stopping cannot finish its journal audit, state exposes
`journal_audit_complete: false` separately from confirmed process termination.
Heartbeat observations use one bounded audit worker; repeated pending
observations retain the latest data and report `coalesced_observations`.

If EMP cannot reopen the primary journal, it creates a uniquely named journal
and commits `journal.created_after_failure` as its first event. That event names
the predecessor and the failure. EMP preserves the old database and does not
manually delete its WAL/SHM files. The DAG remains paused while required history
is unavailable; creating a new file does not reconstruct stage results. Failure
to create usable replacement storage also pauses the DAG. Inspect
`journal_writer` and `journal_pause_reason` in experiment state before restoring
the necessary history through the experiment recovery workflow.

Outside EMP, opening a writable logger starts or connects to the same writer
implementation. That process outlives the application that launched it and exits
after five minutes without requests. Active work prevents idle shutdown. The
next client starts it again under a shared startup lock. Closing a standalone
logger closes its connection; it does not stop other clients' writer.

If the EMP owner process dies, its surviving writer switches to standalone
lifetime. Surviving participants can still commit their results. A recovering
EMP verifies ownership, stops that exact writer instance, and takes over
supervision before resuming the experiment.

Active read clients and filtered-view exports use the writer, so their handles
do not keep the primary SQLite file open in other processes during rollback.
Read-only queries report temporary unavailability during recovery so dashboard
workers do not remain blocked behind mandatory writes.
Offline read-only history can be opened without starting a writer; close such
readers before replacing their files. A restored generation rejects old clients
and checkpoints.

Logging calls are synchronous. In an asynchronous application handler, use
`await asyncio.to_thread(logger.record_event, "module.message", data)` to keep
the event loop responsive while waiting for COMMIT. Framework-owned async paths
perform this wait outside their event loop.

## Startup diagnostics

Startup also records small independent diagnostic files, including when the
journal cannot yet accept calls. These are evidence, never command results or
recovery checkpoints. An attempt directory can contain:

| File | Evidence |
| --- | --- |
| `startup-runner.jsonl` | Launch request/return, effective timeout and source, connection, execute dispatch, failure and cleanup. |
| `startup-executor.jsonl` | Executor entry after imports, validated launch, logger open, listener/endpoint readiness, execute receipt and module spawn. |
| `executor.stdout.log`, `executor.stderr.log` | Early uv/Python output, including failures before logger availability. |
| `failure.jsonl` | Runner failure with original exception/traceback and separate cleanup observation. |

Writer evidence is `controller/journals/<experiment-directory>/startup.jsonl`.
Other logger clients can create `startup/client-<pid>-<id>.jsonl` under the
selected database's parent directory; configuration/code directories are not
used for this output. Failures outside an active attempt use the experiment's
`runner/failure.jsonl`, or the project's `controller/failure.jsonl` before an
experiment is available. Paths follow their runtime owners, not the caller's cwd.

Records contain UTC time, monotonic time, process identity and allowlisted
experiment/attempt coordinates. Startup failures include the effective budget,
excluded recovery time, last phase, traceback/cause and error ID. That error ID
links to the later journal error; its journal timestamp can follow cleanup.
The interval before the executor entry record includes interpreter/import work.
Client and writer request records share a `diagnostic_id`, including the
per-client SQLite probe. It is separate from the stage's execution request ID.

Each structured file is limited to 256 KiB and retains one bounded `.previous`
file. Rotation or oversized records produce `diagnostics.truncated`. Each raw
executor stream retains its first 1 MiB while draining later bytes;
`output.truncated`/`output.finished` report loss. Files follow attempt retention,
but removing the live endpoint does not remove the diagnostic files. Diagnostic
I/O failure does not replace the original failure or make execution successful.
Structured metadata excludes authentication tokens and settings/environment
payloads; captured stdout/stderr remains the process's own output.

`journal_writer.active_read` identifies the current read command, its monotonic
start and observed age; `read_queue_depth` counts waiting reads. These are
separate from write `busy` and `queue_depth`. The writer trace separates queue
wait, SQLite opening/quick_check, execution and response delivery; client traces
measure IPC. Routine fast result polls are not logged individually. Read
observations do not introduce a new watchdog or change restart policy.
Integrity-check frequency, COMMIT acknowledgement and replay behavior are
unchanged.

## Template reload audit

Reload uses a root `reload_template` operation. Its records include full old and
candidate templates, definition changes with field paths and before/after
values, execution-boundary decisions, affected result references, and service
state-transfer observations. Absent fields and JSON null are distinguished.
The applied revision uses the existing `template.applied` record.

Rollback exports the reload operation tree through `export_diagnostics` and
imports it through the existing journal restoration transaction. Diagnostic
records preserve the failed candidate and actual effects; they do not publish
that candidate as the restored current template or execution checkpoint.
Restored records retain event IDs but may have new local cursors. Read them
with the new journal generation. A failed mandatory journal write prevents a
successful reload receipt.

## Configuration

Template logging settings are documented in the
[template reference](experiment_template.md#snapshots-and-journal).
Runtime client configuration adds `db_path`, `open_mode`, and
`expected_journal` to those settings.
EMP also supplies `writer_endpoint_path`; preserve this routing information.
An optional `writer_settings` object configures the writer. For example,
`"writer_settings": {"idle_timeout_seconds": 300}` selects the standalone idle
timeout in seconds. Managed EMP writers do not use an idle timeout. Use one
consistent writer policy for clients sharing a journal; an already running
writer retains the policy with which it started.

`open_mode: create` reserves a new journal name with `expected_journal: null`.
`existing` requires the expected `journal_id` and `generation`.
Relative `db_path` is resolved from the containing JSON file; the configuration
path passed to the logger itself must be absolute.
Relative `writer_endpoint_path` uses the same configuration-directory base.
Use runner-provided configurations inside experiments.

Journal snapshots and diagnostic exports are available through the library,
but they are not a substitute for the runner's full experiment snapshot and
restoration workflow.
