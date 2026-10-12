<p align="center">
  <img src="assets/logo-compare-dark.svg" alt="Module identity and integrity checks" width="420">
</p>

# Module identity and integrity checks

An experiment template pins each module by name, version, and SHA-256 hash.
This identifies the packaged code used by the experiment and detects changes
before that code is accepted for execution.

## What is checked

The module hash covers packaged file paths and contents, including the module
README. Different content cannot silently replace an already registered version.
Changes to code or packaged documentation require a new module version and an
updated template reference.

The framework compares the experiment's module files with both the registered
hash and the template at startup and during runtime preparation of stage
attempts, service launches, and service calls. A mismatch fails validation.
These checks happen at execution boundaries; they do not continuously monitor
files while module code is running.

## Use the feature

1. [Register and validate a module](storage.md#register-and-validate-a-module)
   in maintenance mode.
2. Copy the returned name, version, and hash into the
   [experiment template](experiment_template.md).
3. Switch to run mode and [start the experiment](basic_dag.md#start-and-observe).
4. Keep registered source and installed experiment module directories immutable.
   Register changed contents as a new version before using them in a template.

Stored module validation downloads and verifies the package, its manifest, and
hash. Module inspection reports registration and availability metadata; it does
not verify package contents. See [storage and failure handling](storage.md#versions-hashes-and-failures).

## Scope of the guarantee

Matching hashes establish matching packaged content. They do not establish
identical dependencies, datasets, hardware, external services, or deterministic
behavior. Module authors must document and control those requirements.
See [reproducibility and responsibility](reproducibility.md).

Return to [project features](../README.md#what-it-does) or the
[documentation index](README.md).
