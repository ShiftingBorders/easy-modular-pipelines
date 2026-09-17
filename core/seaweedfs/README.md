# SeaweedFS executables

This directory holds the local SeaweedFS binaries used by the managed module
store. The executables are ignored by Git.

From the repository root, install the pinned build for your Windows/Linux
amd64 host:

```text
uv run python scripts/download_seaweedfs.py
```

To download both binaries:

```text
uv run python scripts/download_seaweedfs.py --platform all
```

The script installs SeaweedFS **4.45** from the official release, matching the
original bundled files exactly:

| Platform | Release archive | Installed file |
| --- | --- | --- |
| Windows amd64 | `windows_amd64.zip` | `weed.exe` |
| Linux amd64 | `linux_amd64_full.tar.gz` | `weed` |

Archive and executable SHA-256 hashes are pinned in the script. See
[module storage](../../docs/storage.md) for installation behavior and options.
