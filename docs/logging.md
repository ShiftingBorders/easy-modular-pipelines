# Logging and reading experiment history

Each experiment uses a shared local SQLite journal. Each process opens its own
`OperationLogger` client; do not pass an open client across process boundaries.

Modules receive an absolute `logging_config_path` in their runtime context.
Use that configuration instead of creating another journal.

## Events and errors

```python
from pathlib import Path

from core.logger import OperationLogger

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
from core.logger import OperationLogger

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
| `read_command_result(request_id)` | Read the considered command outcome and participant observations. |

Event pages contain `events`, `checkpoint`, `boundary`, and `has_more`.
Change pages contain `changes` and their own checkpoint.
Limits are 1–1000. Continue with the returned checkpoint while `has_more` is true,
even when an effective page has no entries.

Checkpoints include journal identity and generation. A rollback changes the
generation; `JournalGenerationChanged` reports this explicitly. Refresh the
reader's connection information and start a new history view. Do not combine
old and new generations or silently reuse an old write client.

## Results and reliability

The journal is the sole store of participant call results; artifact data remains
in files. The participant records observed completion, while the runner records
its accepted outcome. The runner's outcome has priority; a late response after
timeout remains diagnostic evidence rather than a new success.

StageExecutor and ParticipantServer write their results. Module authors should
not write a competing `record_command_result` for the same request.

The runner records the template and effective attempt settings before work.
Secrets embedded in settings enter history; use supported references to separate
secret sources when needed. Each event's process identity belongs to its writer.

A returned event ID confirms a database commit. This does not guarantee survival
of disk failure or power loss. Unknown commit outcomes prevent further writes
by that client. Missing, mismatched, or incompatible journals are not silently
recreated. An obligatory journal failure stops execution.

## Configuration

Template logging settings are documented in the
[template reference](experiment_template.md#snapshots-and-journal).
Runtime client configuration adds `db_path`, `open_mode`, and
`expected_journal` to those settings.

`open_mode: create` reserves a new journal name with `expected_journal: null`.
`existing` requires the expected `journal_id` and `generation`.
Relative `db_path` is resolved from the containing JSON file; the configuration
path passed to the logger itself must be absolute.
Use runner-provided configurations inside experiments.

Journal snapshots and diagnostic exports are available through the library,
but they are not a substitute for the runner's full experiment snapshot and
restoration workflow.
