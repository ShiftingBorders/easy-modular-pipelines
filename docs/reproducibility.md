# Reproducibility and responsibility

The library controls module identity, execution policies, recorded settings and
results, and the snapshot and exchange workflows. Matching module hashes
establish matching packaged content; they do not establish deterministic
behavior or identical execution environments. Resource hashes are checked at
startup when supplied; a resource with `hash: null` has no such check.

Module authors are responsible for the conditions their computation requires:

- **Inputs and environment.** Pin and document dependencies, datasets, model
  weights, external tools, and relevant OS, driver, and hardware requirements.
  The runner does not automatically recreate the module's dependency environment.
  Downloads and external services need their own versioning or captured inputs.
- **Determinism.** Control randomness and other sources of variation, and
  document remaining nondeterminism, such as GPU operations, timing, or changing
  API responses. Identical code and settings alone do not ensure identical results.
- **Portable state and artifacts.** Keep module code immutable, write to the
  supplied runtime directories, and return experiment-relative artifact paths.
  Validate inputs, including missing or changed data after a move or rerun.
- **Restoration and external effects.** Services must freeze actual writers,
  save all required state, and restore it correctly, including random-generator
  state when needed. Authors must define what retrying or rerunning work does
  and handle external side effects. A rollback does not automatically undo an
  external database write, network request, or other effect outside saved state.

For example, a module that downloads the latest model on every run can pass
every module hash check and still produce different results. An exchange archive
makes the packaged experiment transferable; the recipient still needs the
environment and external prerequisites documented by its module authors.
See [module authoring](instructions/modules.md) and
[service state and restoration](instructions/python_bridges.md#settings-and-state).

