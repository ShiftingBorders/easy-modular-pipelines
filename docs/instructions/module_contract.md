# Module and service behavior contract

This document defines the obligations of modules and services running inside
Easy Modular Pipelines. It applies to human-written and agent-generated code.

It does not prescribe code style, internal architecture, or implementation
language. Repository contribution rules apply separately when contributing
to this repository.

“Must” describes a required behavior. “Should” describes a recommendation
whose applicability depends on the module's application contract.

For implementation examples, see [module authoring](modules.md) and
[service authoring](python_bridges.md). For API and transport details, see
the [participant reference](participant_protocol.md).

## 1. Responsibility boundaries

| Responsibility | Owner |
| --- | --- |
| DAG execution order, attempt retries, and accepted outcomes | Runner |
| Stage process execution and final result validation | Stage executor |
| Service launches, readiness observation, and restart policy | Service manager |
| Experiment snapshots, rollback, and recovery coordination | Runner |
| Application inputs, computation, and output correctness | Module or service |
| Application resources and externally launched processes | Their module or service owner |
| Complete service state export and restoration | Service |
| External side effects and behavior when work is repeated | Module or service |

Modules must work through the provided contracts. They must not modify runner
state, journal storage, module registration records, or framework-owned
control files directly.

A module must not introduce its own DAG retry or experiment recovery policy.
Application-level retries, if needed, must remain within the attempt's
deadline and cancellation behavior and must account for repeated side effects.

Ordinary Python stages should use StageClient; Python services should use
ParticipantServer. Alternative implementations must preserve the applicable
launch, result, and participant contracts.

## 2. Package and application contract

A module must provide a valid manifest and a README describing:

- Its role, purpose, settings, defaults, and required inputs.
- Output structure, artifact references, and failure behavior.
- Required dependencies, external tools, network access, and environment.
- Persistent state, owned resources, and external side effects.
- What cancellation, retrying, and rerunning mean for its work.
- For services: readiness, supported operations, shutdown, and restoration.

The module must validate application data before relying on it or applying
related side effects. Framework validation of the manifest or transport
envelope does not validate application-specific settings or payloads.

Missing input must be handled according to the documented contract, including
after a skip, move, or rerun. Invalid explicit settings must not be silently
replaced by defaults, and saved working state must not silently override them.

The module package is immutable during execution. Changed packaged content,
including its README, must be registered as a new version, with corresponding
template references updated. Modules must not bypass integrity checks.

## 3. Files, paths, and dependencies

Modules must write generated files only into the runtime directories assigned
for that purpose. This includes caches, temporary files, downloaded assets,
generated configuration, and files produced by third-party tools.

Modules must use the supplied runtime paths rather than infer them from the
caller's working directory or construct attempt numbering themselves.

Relative paths read from configuration files must be resolved against the
containing configuration file's directory. Absolute configured paths must be
preserved.

Internal artifact references returned in results must be relative to the
experiment root. Logger artifact records have their own path base; see
[logging](../logging.md#artifacts).

A module must finish producing a referenced artifact before reporting it as
a completed result. It must not require an old experiment's absolute location
to resolve internal result files after transfer or continuation.

Authors must document environment and dependency requirements. The runner
does not automatically reconstruct dependency environments or version external
downloads. Dependencies and mutable external inputs should be pinned or
otherwise identified sufficiently for the intended repeatability.

## 4. Stage completion and failure

A stage attempt succeeds only with exit code 0 and exactly one valid stdout
result JSON describing success.

StageClient.succeed() and StageClient.fail() publish a result; they do not
terminate the process. The stage must return after publishing its final result.

Stages must not write diagnostics, progress messages, installation output,
or unprocessed child-process output to their result stdout. Use the journal
or captured stderr for diagnostics.

Stages must not report success before their required work and output
publication are complete. Required work must not continue in an unmanaged
background process after the stage reports completion.

Errors must remain observable. Modules must not turn failed required work
into a successful empty result or suppress failures merely to let the DAG
continue. Failure data is not forwarded as successful input to the next stage.

Conditional stages must use the declared conditional-result contract.
They must not control execution by editing runner files or issuing an
independent control action. See [conditional stages](modules.md#conditional-stages).

## 5. Cancellation, deadlines, and repeated work

Long-running stages must check for cancellation at safe points and release
owned resources when cancellation is observed. The executor may terminate
a process that does not cooperate.

Services must keep heartbeat, interrupt, and shutdown responsive while work
is running. Blocking the participant event loop with long synchronous work
violates this requirement.

An interrupt handler must stop the identified work before acknowledging
successful interruption. Cancelling an awaiting task alone is insufficient
if a thread, child process, or external operation continues its effects.

A module must not assume that a timeout proves its work never happened.
A late successful operation does not overturn the runner's accepted timeout.

Authors must define the consequences of repeating work. For external effects,
they should use application-appropriate deduplication, idempotency, or explicit
reconciliation where needed. A new request ID does not guarantee exactly-once
execution.

Rollback does not automatically undo external database writes, network
requests, or effects outside the saved experiment state. These limitations
must be documented.

## 6. Service readiness and resource ownership

A service must report a successful heartbeat only when its application is
actually ready, including any required external resource.

An open socket, successful handshake, or launcher exit code alone is not proof
of readiness.

The service owns the resources it launches. It must handle partial startup
failure and shut down owned work and resources through its lifecycle.
It must not stop unrelated processes based only on a saved PID.

A shutdown handler must complete its required cleanup and signal the lifecycle
owner to exit. With ParticipantServer, the owner closes the server after the
handler returns; the handler must not close its own response channel.

Loss of the runner connection must not automatically destroy the service.
A live instance must support reconnection; a restarted instance has a new
identity.

Startup settings and per-call settings are separate inputs. The service must
not assume that startup settings have been merged into each DAG request.
See [service settings](python_bridges.md#settings-and-state).

## 7. Service snapshots and restoration

The runner coordinates snapshots. The service must implement the actual
consistency and restoration of its application state.

- freeze_writes must stop all writers affecting exported state, including
  background tasks and owned external processes, before reporting success.
  Heartbeat must remain responsive.
- save_state must write all required restoration files into the supplied
  output directory and return an experiment-relative state_path.
- Exported files must be complete before success is reported and remain
  available after service shutdown.
- load_state must validate and restore the required state before reporting
  success. Saved working data must not silently substitute for the requested
  snapshot.
- unfreeze_writes must resume ordinary writes when requested.

The service must not report successful freezing, restoration, or unfreezing
when the required effect has not occurred.

Snapshot completeness includes random-generator state, counters, timers,
and external-resource state where they are necessary for the promised
restoration behavior. Any limitations must be explicit.

A stateless service must use the documented stateless contract. Returning
state_path: null must not conceal state that is required for restoration.

## 8. Observation and reproducibility

Each process must use its own logger client with the supplied configuration.
Modules must not replace the shared journal with private log files or write
competing framework result records for the same request.

Application events should explain progress and failures without leaking
credentials. Secrets must not be embedded in template settings, which are
journaled, or included in results and diagnostic messages.

Progress and reported state are observations, not restoration checkpoints.

Authors must document sources of nondeterminism and external prerequisites.
Matching module hashes establish matching package contents; they do not prove
identical environments, deterministic results, or complete restoration.

See [reproducibility and responsibility](../reproducibility.md).

## Completion checklist

Before delivering a module, verify that:

- Its manifest, README, inputs, outputs, and actual behavior agree.
- It uses assigned paths and leaves its package unchanged.
- Success means the required work and artifacts are complete.
- Failure, cancellation, and timeout behavior preserve truthful outcomes.
- Owned resources have a defined startup and cleanup lifecycle.
- Repeated work and external side effects have documented semantics.
- A service reports actual readiness and exports complete required state.
- Verification results distinguish checked behavior from unverified claims.

Registration and manifest validation alone do not demonstrate that these
behavioral obligations are satisfied.
