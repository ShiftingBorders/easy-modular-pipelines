"""Install the exact SeaweedFS binaries previously bundled with the project."""

import argparse
import hashlib
import platform
import shutil
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path

VERSION = "4.45"
RELEASE_URL = f"https://github.com/seaweedfs/seaweedfs/releases/download/{VERSION}"
REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
# These archive and executable hashes were checked against the original files.
BUILDS = {
    "windows": {
        "archive": "windows_amd64.zip",
        "archive_sha256": (
            "42186b316d3a60995483a1e58476bcc143b24d37e0507d9df9331bebbb9259f2"
        ),
        "executable": "weed.exe",
        "executable_sha256": (
            "94312f7bace88fd6551f93f33e3f29f6074e9e48cf825b8028ff33593777f6f1"
        ),
    },
    "linux": {
        "archive": "linux_amd64_full.tar.gz",
        "archive_sha256": (
            "66691d884e28a0a5584fdd1a5aebf82c5cc889854dc64323211291be3ad0cda4"
        ),
        "executable": "weed",
        "executable_sha256": (
            "b4bccc35f62347977cbf8326603f1ac3a48dcdb1fb85fcd081a2a62117b8529f"
        ),
    },
}


def verify_sha256(path: Path, expected: str) -> None:
    with path.open("rb") as stream:
        actual = hashlib.file_digest(stream, "sha256").hexdigest()
    if actual != expected:
        raise ValueError(
            f"SHA-256 mismatch for {path}: expected {expected}, got {actual}. "
            "If this is an installed binary, move it aside before retrying."
        )


def install_binary(target_platform: str) -> None:
    build = BUILDS[target_platform]
    destination = REPOSITORY_ROOT / "core" / "seaweedfs" / build["executable"]
    if destination.exists():
        verify_sha256(destination, build["executable_sha256"])
        if target_platform == "linux":
            destination.chmod(destination.stat().st_mode | 0o111)
        print(f"Already installed and verified: {destination}")
        return

    scratch = REPOSITORY_ROOT / ".artifacts" / "tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="seaweedfs-", dir=scratch) as folder:
        temporary_directory = Path(folder)
        archive_path = temporary_directory / build["archive"]
        url = f"{RELEASE_URL}/{build['archive']}"
        print(f"Downloading SeaweedFS {VERSION}: {url}", flush=True)
        with (
            urllib.request.urlopen(url, timeout=60) as response,
            archive_path.open("wb") as output,
        ):
            shutil.copyfileobj(response, output)
        verify_sha256(archive_path, build["archive_sha256"])

        executable_path = temporary_directory / build["executable"]
        # Read only the expected member; never extract arbitrary archive paths.
        if zipfile.is_zipfile(archive_path):
            with (
                zipfile.ZipFile(archive_path) as archive,
                archive.open(build["executable"]) as source,
                executable_path.open("wb") as output,
            ):
                shutil.copyfileobj(source, output)
        else:
            with tarfile.open(archive_path, "r:gz") as archive:
                member = archive.getmember(build["executable"])
                if not member.isfile():
                    raise ValueError(f"Expected a regular file: {member.name}")
                source = archive.extractfile(member)
                if source is None:
                    raise ValueError(f"Cannot read archive member: {member.name}")
                with source, executable_path.open("wb") as output:
                    shutil.copyfileobj(source, output)

        verify_sha256(executable_path, build["executable_sha256"])
        if target_platform == "linux":
            executable_path.chmod(0o755)
        destination.parent.mkdir(parents=True, exist_ok=True)
        executable_path.replace(destination)
    print(f"Installed and verified: {destination}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        choices=("auto", *BUILDS, "all"),
        default="auto",
        help="Build to download (amd64 only); default: current Windows/Linux host.",
    )
    arguments = parser.parse_args()
    target_platform = arguments.platform
    if target_platform == "auto":
        target_platform = platform.system().lower()
        if target_platform not in BUILDS:
            parser.error("Automatic installation supports only Windows and Linux.")
        if platform.machine().lower() not in ("amd64", "x86_64"):
            parser.error(
                "Pinned binaries require amd64. Use --platform only when "
                "preparing files for a different machine."
            )

    targets = list(BUILDS) if target_platform == "all" else [target_platform]
    try:
        for target in targets:
            install_binary(target)
    except (OSError, ValueError, KeyError, tarfile.TarError, zipfile.BadZipFile) as error:
        print(f"SeaweedFS installation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
