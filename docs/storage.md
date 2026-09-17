# Module storage and registration

A module registration pairs an immutable name/version/hash with its archived
code. The server uses HashDB for hashes and SeaweedFS for packages, and installs
local module code under `<project_root>/modules/<name>/<version>/`.

Use the [experiment guide](basic_dag.md) to create the project configuration.

## Start maintenance mode

```text
uv run python -B webserver.py --config "<absolute-project-path>/server.json" --mode maintenance
```

In another terminal, wait for readiness:

```text
uv run python -B cli.py health
```

Use `--config <cli-json>` or `--url <api-url>` before the CLI subcommand when
the server uses a different address.

By default the server creates missing HashDB configuration and schema, initializes
the database, and owns a local SeaweedFS process with data under the project's
`seaweedfs/`. The executable is `core/seaweedfs/weed.exe` on Windows or
`core/seaweedfs/weed` on Linux; the latter needs execute permission.

Defaults are in [webserver.json](../default_settings/webserver.json) and
[seaweed_args.json](../default_settings/seaweed_args.json). For an existing Filer,
pass `--filer-url http://<host>:<port>`. The server then owns its client connection,
not that external service. Disk-reserve checks for a local managed store cannot
measure a remote server's free space.

Changing between `maintenance` and `run` requires stopping and restarting
the server. Module mutations are blocked if unfinished experiments require
recovery; recover and stop them in run mode first.

## Register and validate a module

The folder must be an absolute path readable by the server:

```text
uv run python -B cli.py module validate --folder "<absolute-module-folder>" --wait
uv run python -B cli.py --json module add --folder "<absolute-module-folder>" --wait
uv run python -B cli.py module validate --name hello --version 1.0 --wait
```

Source validation checks `module.yaml` and the source folder contract.
Stored validation downloads and verifies the package, its manifest, and hash.
Module registration validates, archives, registers, verifies, and installs
the module without replacing different existing content.

A successful add result contains:

```json
{
  "module": {
    "name": "hello",
    "version": "1.0",
    "hash": "<actual-sha256>"
  },
  "status": "registered",
  "installation_path": "<absolute-installed-folder>",
  "installed": true
}
```

These fields are inside `data` of the completed command response.
`status: already_registered` means identical content was already registered.
`installed: false` is normal when the same source is already at the installation
path; it does not mean registration failed.

Copy the returned `module` reference into your template. Registration does not
edit templates automatically.

## Versions, hashes, and failures

Keep source files unchanged throughout registration. The hash includes relative
file names and contents, including documentation packaged with the module.
A changed README therefore changes the module hash too.

Use a new version for changed content. An existing name/version with a different
hash, or an incomplete hash/archive pair, produces a conflict.
Do not manually rewrite HashDB to make changed files appear valid.

Registration and installation can fail at different stages. Error notes may
state that registration succeeded while installation did not. Check the actual
outcome; a failed HTTP/client wait is not proof of rollback. Stored validation
and a deliberate retry of identical content help distinguish these cases.

The CLI also provides `module remove --name <name> --version <version> --wait`.
It removes the archive and registered hash, not the installed source folder.
It is a maintenance operation, not an experiment cleanup command.
Do not remove versions still needed by your experiments.

Old example databases containing only hashes are not complete registrations.
Keep an old runtime intact and prepare a separate clean project when trying
the updated examples, rather than pairing an old hash database with an empty
archive store.

## Library use

`ModuleManager` accepts caller-owned `HashDatabase` and `ModuleDatabase`
implementations. `HashDB` and `SeaweedDB` provide the normal implementations.
`SeaweedProcess` owns a managed local server; `SeaweedDB` owns only its HTTP client.

For direct library use, construct and explicitly close each resource in the
calling application. Use absolute paths for library configuration and filesystem
arguments. Serialize mutations of the same name/version across callers and
keep the source stable during hashing and packaging.

Expected storage failures derive from `core.storage_errors.StorageError`:

| Exception | Meaning |
| --- | --- |
| `StorageConfigurationError` | Invalid configuration or database schema. |
| `StorageInputError` | Invalid storage-operation input. |
| `StorageUnavailable` / `StorageClosedError` | Dependency unavailable or resource closed. |
| `StorageConflict` | Incompatible existing state. |
| `StoredObjectNotFound` | Required stored object missing. |
| `StorageCapacityError` | Capacity, archive-size, or disk-reserve limit. |
| `StorageAccessError` | Access denied or read-only storage. |
| `StorageIOError` | Local file I/O failure. |

Filesystem validation and programming errors may have their own exception types.
Preserve the original exception and its notes. A connection failure does not
make repeating a write automatically safe.
