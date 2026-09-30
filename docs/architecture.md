# Core source layout

The library implementation is grouped by responsibility. Import from the owning
module when adding framework code; package `__init__.py` files do not aggregate
the API.

| Package | Responsibility and entry points |
| --- | --- |
| `core.primitives` | JSON values and files, native file locks, process identity, task completion, and locations shipped with the framework. |
| `core.storage` | `hash_db.HashDB`, `seaweed_client.SeaweedDB`, `seaweed_process.SeaweedProcess`, storage contracts, configuration, and errors. |
| `core.modules` | `manager.ModuleManager`, module manifests, package validation, and installation filesystem operations. |
| `core.journal` | `logger.OperationLogger`, events and settings, SQLite storage, filtered views, captured streams, and derived history caches. |
| `core.participants` | Participant protocol and connections, `stage_client.StageClient`, `server.ParticipantServer`, and the stage executor. |
| `core.experiments` | Template assembly, archive exchange and reading, DAG runner, stage/service ownership, snapshots, and runner state. |
| `core.resources` | Controller-owned collection, process and hardware sampling, and observation state. |
| `core.server` | Server settings, runtime process ownership, and maintenance/run controllers. |

`webserver.py` exposes HTTP and `cli.py` is its client. The dashboard remains a
separate application. Version-controlled defaults remain in `default_settings/`.
SeaweedFS binaries are installed under `core/storage/seaweedfs/` by
`uv run python scripts/download_seaweedfs.py` and are not committed.

Dependencies follow ownership: server uses experiments and storage; experiments
use modules, participants, and journals. Storage, journals, participants, and
resources do not import the DAG runner. Primitives have no dependencies on
those higher-level packages. JSON types have one definition in
`core.primitives.json_values`.

Private operations that validate or prepare explicit inputs live beside their
owner in focused modules. The owning classes retain state changes, transactions,
process ownership, cancellation, and recovery decisions. Helpers do not receive
an entire manager or runner merely to access its private fields.

## Import migration

Older public imports remain explicit compatibility re-exports of the same
objects. They contain no independent implementation. This allows existing
immutable module versions to run without changing their source or hashes.

| Legacy import | Preferred import |
| --- | --- |
| `core.logger.OperationLogger` | `core.journal.logger.OperationLogger` |
| `core.runner_utils.stage_client.StageClient` | `core.participants.stage_client.StageClient` |
| `core.runner_utils.participant_server.ParticipantServer` | `core.participants.server.ParticipantServer` |
| `core.runner_utils.runtimeio.read_json` / `write_json` | `core.primitives.json_files.read_json` / `write_json` |
| `core.runner_utils.runtimeio.process_identity` | `core.primitives.processes.process_identity` |
| `core.modulemanager.ModuleManager` | `core.modules.manager.ModuleManager` |
| `core.hashdb.HashDB` | `core.storage.hash_db.HashDB` |
| `core.serverruntime.ServerRuntime` | `core.server.runtime.ServerRuntime` |

The old executor module entry point delegates to `core.participants.executor`.
Legacy `utils` imports also remain aliases; new implementation code does not
import that package. Exception aliases refer to the same exception classes.

The migration does not change journal schemas, experiment formats, configuration
fields, or module identity. Configuration-relative paths still resolve from
their containing configuration file, independently of the caller's working
directory. `load_json` and `read_json` keep their distinct validation contracts.

Core test modules are grouped under `tests/storage`, `tests/modules`,
`tests/journal`, `tests/participants`, `tests/experiments`, `tests/resources`,
and `tests/server`. Shared fixtures remain under `tests/helpers`; application
and dashboard suites keep their existing locations. Test imports and mock
destinations follow the defining implementation module; patching a compatibility
facade does not replace a dependency inside its owner.
See [testing](testing.md) for the commands and approval policy.
