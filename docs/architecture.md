# Core source layout

The library implementation is grouped by responsibility. Import from the owning
module when adding framework code; package `__init__.py` files do not aggregate
the API.

| Package | Responsibility and entry points |
| --- | --- |
| `core.primitives` | JSON values and files, native file locks, process identity, task completion, and locations shipped with the framework. |
| `core.models` | Pydantic configuration and boundary models, grouped by contract. Validators describe data; file access and runtime effects remain with their owners. |
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

All Pydantic model definitions live in `core/models/`. Existing imports from
`core.storage.hash_config` and `core.storage.seaweed_config` remain supported.
Configuration consumers receive validated models where compatible with their
public contracts. `CollectorSettings` retains its public dataclass form.

Experiment template and module manifest models describe their structure and
internal consistency before assembly or launch. Public loaders keep their JSON
results; internal assembly, rebuild, and launch operations consume validated
objects. `validate_template` calls the public `load_template` wrapper so caller
overrides keep working, then validates its JSON result before module inspection.
Registered hashes, actual module roles, filesystem boundaries, and resource
ownership are checked by the operations that use those resources.

Participant identity, endpoint, hello, request, response, and notification models
validate wire data at the receiving boundary. Connection operations use typed
endpoints and replies; public protocol helpers and application handlers retain
their JSON contracts. Authentication, live OS identity, request correlation,
deadlines, and work ownership remain with the participant runtime.

Module preparation validates caller identity and input before writing runtime
files. StageClient validates its context before starting its communication thread;
the executor validates its fixed launch document before opening its journal or
endpoint. Shared models describe progress limits and the exact stdout result
shape. Application settings and result data remain opaque JSON.

Recovery validates command-state observations before updating executor records
or reconciling service work. Service exports validate their relative path before
the owner checks actual filesystem confinement. Conditional result models check
decision structure; the runner retains current DAG membership and payload policy.

Server command, chain and target models normalize admission payloads. Controller
argument models check supported control operations before invoking public runner
methods. Outcome models check identifiers and state/result consistency before
response sizing and caching. Runtime retains mode gates, queue ownership,
identifier retention, priority stop and lifecycle transitions.

Module reads share argument models across run and maintenance modes. Metadata
reads validate references while retaining snapshot selection defaults and actual
filesystem checks. Resource-history and event reads validate query structure
before invoking their owners. Maintenance separates request execution from
failure mapping and retains serial module mutations and concurrent reads.

Controller queues validate command and chain documents before admission, while
runtime notifications preserve their metadata contract and live-process guards.
Service response handling consumes typed observations and separates heartbeat
from work completion. Participant request parsing passes a checked model to
internal reply handling; application callbacks retain validated JSON dictionaries.

Collector snapshots and IPC commands are validated before selecting a journal
or replacing process targets. Incoming packets are validated before updating
resource history and freshness. Supervision retains worker ownership, restart
and journal-closure checks; `ResourceTarget` keeps its public dataclass form.

The low-level `SQLiteEventStore` constructor validates scalar arguments with
standard-library rules and performs no file I/O. File-based journal settings
use Pydantic after their explicit read, reusing those same opening rules.
Journal imports therefore do not trigger Pydantic plugin metadata discovery.

Journal record models validate event envelopes, context, identified checkpoints,
command observations and measurements. Their imports occur at record boundaries;
shared standard-library identity/context rules remain available to the passive
SQLite constructor. Event encoding retains caller key order and the complete
UTF-8 byte limit. Operation ownership, generation checks and result precedence
remain with journal operations.

Diagnostic observers use one shared model for decoding and restoration inputs.
Diagnostic and journal snapshot manifest models describe their data formats;
file readers retain duplicate/reference checks and compare actual checksums,
counts and database identities. Diagnostic event wrappers are validated apart
from the enclosed event so they retain the event's JSON-depth allowance.

Diagnostic operation-tree selection, observer facts, dependency closure and
bundle preparation live in `journal.diagnostics`. Restoration prepares encoded
diagnostic inputs before its write transaction; command-state merge is separate
from indexed persistence. SQLiteEventStore retains connection/lock ownership,
BEGIN/COMMIT/ROLLBACK, actual file identity and failure handling for both fresh
and idempotent restoration.

Private operations that validate or prepare explicit inputs live beside their
owner in focused modules. The owning classes retain state changes, transactions,
process ownership, cancellation, and recovery decisions. Helpers do not receive
an entire manager or runner merely to access its private fields.

Reload layout and cursor calculations live in `experiments.reload` as ordinary
computed records. `experiments.journal` reads progress evidence against an
explicit initial journal boundary; the runner retains publication and rollback.
Service shutdown separates task detachment, shutdown RPC, exit confirmation,
request cancellation and outcome recording while sharing one deadline and
retaining the manager's ownership checks.

Snapshot restoration exposes its transaction phases in `_finish_restore`:
owner/path checks, interrupted participant stop, staging, installation, journal
binding, service restoration and cleanup. Pure state-document preparation lives
in `experiments.restore_inputs`; the snapshot owner retains marker publication,
filesystem/process checks and cancellation barriers.

The runner's DAG loop owns waiting and task cleanup. Focused owner methods
prepare state, apply supervision/readiness decisions, accept stage outcomes,
take boundary snapshots, finish a step or DAG, and launch the next stage.
Unknown ownership, conditional stop and final completion keep distinct loop
control paths.

Recovery keeps selection, ownership and checkpoint-acceptance policy with the
runner. The journal module reads bounded checkpoint/launch evidence, while the
state module reconstructs attempt data without I/O. Minimal attempt ownership
is bound before optional files are read, so failure handling still confirms
termination before repeating unresolved work.

Reload publication uses an internal application record for the computed plan,
original audit references and detach/commit flags. The runner separates candidate
audit, protective snapshot, service-data isolation, DAG publication, state
transfer, durable commit, rollback and cleanup. Persisted pending-rebuild state
and publication barriers retain their existing format and ownership.

New-run component rebinding is separate from selected-generation initialization.
Stage launch separates attempt/identity creation, context preparation, durable
intent binding, owned executor spawn and startup observation. Public run and
launcher hooks retain their existing contracts and effect order.

## Import migration

Legacy compatibility modules have been removed. Import directly from the
owning modules below. Existing module versions that use legacy imports must
be migrated and registered as new versions; update their template hashes.
Do not edit already installed module contents in place.

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

Use `core.participants.executor` as the executor module entry point. The old
`core.runner_utils`, `core.logger_utils`, `core.resource_utils`, and external
`utils` compatibility packages have also been removed. Import shared operations
from `core.primitives`, and storage exceptions from `core.storage.errors`.

The migration does not change journal schemas, experiment formats, configuration
fields, or module identity. Configuration-relative paths still resolve from
their containing configuration file, independently of the caller's working
directory. `load_json` and `read_json` keep their distinct validation contracts.

Core test modules are grouped under `tests/storage`, `tests/modules`,
`tests/journal`, `tests/participants`, `tests/experiments`, `tests/resources`,
and `tests/server`. Shared fixtures remain under `tests/helpers`; application
and dashboard suites keep their existing locations. Test imports and mock
destinations follow the defining implementation module.
See [testing](testing.md) for the commands and approval policy.
