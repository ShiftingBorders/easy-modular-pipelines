"""Approved archive plan D: real malformed files and bounded decompression."""

import asyncio
import copy
import hashlib
import io
import lzma
import os
import shutil
import subprocess
import tarfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from core.experimentarchiver import ExperimentArchiver
from core.runner_utils.runtimeio import read_json, write_json
from core.storage_errors import StorageCapacityError
from tests.helpers.archives import (
    ArchiveTestCase,
    archive_members,
    inventory,
    write_archive,
)


class ArchiveValidationTests(ArchiveTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.source_archive()
        self.members = archive_members(self.w.archive)
        self.bad = self.w.root / "modified.tar.xz"

    async def reject(self, members=None, change=None, errors=(ValueError, TypeError)):
        write_archive(
            self.bad,
            self.members if members is None else members,
            manifest_change=change,
        )
        before = inventory(self.w.filer.root)
        with self.assertRaises(errors):
            await self.w.importer.inspect(self.bad)
        self.assertEqual(inventory(self.w.filer.root), before)
        self.assertEqual(self.w.filer.requests, [])
        self.assertFalse((self.w.target / "modules").exists())
        self.assert_work_clean()

    def changed_file(self, name, content):
        members = copy.deepcopy(self.members)
        for member, _data in members:
            if member.name == name:
                members = [
                    (entry, content if entry.name == name else data)
                    for entry, data in members
                ]
                break

        def update(document):
            document["files"][name] = {
                "size": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }

        return members, update

    async def test_manifest_schema_types_and_identity_are_validated(self):
        """D: malformed metadata is rejected before any installation or execution."""
        for key, value in (
            ("schema_version", 2),
            ("schema_version", True),
            ("archive_id", "bad"),
            ("created_at", "2026-09-16T00:00:00"),
            ("created_at", "bad"),
            ("source_experiment_id", ""),
            ("template", "../template.yaml"),
            ("modules", []),
            ("directories", "folder"),
            ("files", []),
        ):
            with self.subTest(key=key, value=value):
                await self.reject(change=lambda d, k=key, v=value: d.update({k: v}))
        await self.reject(change=lambda d: d.pop("archive_id"))
        await self.reject(change=lambda d: d.update(extra=True))
        for value in (
            [],
            {"size": True, "sha256": "a" * 64},
            {"size": -1, "sha256": "a" * 64},
            {"size": 1, "sha256": "bad"},
        ):
            with self.subTest(integrity=value):
                await self.reject(
                    change=lambda d, v=value: d["files"].update({"experiment.yaml": v})
                )

    async def test_inventory_missing_extra_and_modified_files_are_rejected(self):
        """D: exact directories, bytes, sizes and hashes are part of the archive contract."""
        await self.reject(
            [item for item in self.members if item[0].name != "resources/source.txt"]
        )
        extra = tarfile.TarInfo("extra.txt")
        await self.reject([*self.members, (extra, b"extra")])
        await self.reject(
            [
                (member, b"changed" if member.name == "resources/source.txt" else data)
                for member, data in self.members
            ]
        )
        await self.reject(change=lambda d: d["directories"].append("absent"))
        await self.reject(change=lambda d: d["files"].pop("resources/source.txt"))

    async def test_module_code_identity_and_role_are_checked_beyond_file_checksums(
        self,
    ):
        """D: updating file checksums cannot legitimize changed module code or metadata."""
        name = "modules/portable-stage/1/main.py"
        members, update = self.changed_file(
            name, b"raise RuntimeError('must never execute')\n"
        )
        await self.reject(members, update)
        name = "modules/portable-stage/1/module.yaml"
        original = next(data for member, data in self.members if member.name == name)
        for key, value in (("name", "foreign"), ("version", "99"), ("role", "service")):
            with self.subTest(key=key):
                document = yaml.safe_load(original)
                document[key] = value
                members, update = self.changed_file(
                    name, yaml.safe_dump(document).encode()
                )
                await self.reject(members, update)

    async def test_resource_paths_hashes_and_allowed_payload_are_checked(self):
        """D/C: a self-consistent inventory cannot smuggle external resource references."""
        original = next(
            data for member, data in self.members if member.name == "experiment.yaml"
        )
        for path in (
            "../escape",
            str(self.w.root / "outside"),
            "C:/machine/data",
            "resources/absent",
        ):
            with self.subTest(path=path):
                template = yaml.safe_load(original)
                template["resources"][0]["path"] = path
                members, update = self.changed_file(
                    "experiment.yaml", yaml.safe_dump(template).encode()
                )
                await self.reject(members, update)
        members, update = self.changed_file(
            "resources/source.txt", b"tampered resource"
        )
        await self.reject(members, update)
        extra = tarfile.TarInfo("runner/token")
        folder = tarfile.TarInfo("runner")
        folder.type = tarfile.DIRTYPE

        def include_runtime(document):
            document["directories"].append("runner")
            document["files"]["runner/token"] = {
                "size": 6,
                "sha256": hashlib.sha256(b"secret").hexdigest(),
            }

        await self.reject(
            [*self.members, (folder, None), (extra, b"secret")], include_runtime
        )

    async def test_unsafe_member_paths_never_write_outside_workspace(self):
        """D: traversal, Windows aliases and drive/UNC paths are rejected on real archives."""
        sentinel = self.w.root / "sentinel"
        sentinel.write_bytes(b"unchanged")
        for name in (
            "../sentinel",
            "/absolute",
            "C:/sentinel",
            "C:sentinel",
            "//host/share",
            "..\\sentinel",
            "a//b",
            "a/./b",
            "CON",
            "data.",
            "data ",
            "a:stream",
            "a\x01b",
        ):
            with self.subTest(name=name):
                await self.reject([*self.members, (tarfile.TarInfo(name), b"bad")])
        self.assertEqual(sentinel.read_bytes(), b"unchanged")

    async def test_links_special_files_duplicates_and_file_directory_conflicts_are_rejected(
        self,
    ):
        """D: the extractor never follows archive links or ambiguous member identities."""
        for kind in (
            tarfile.SYMTYPE,
            tarfile.LNKTYPE,
            tarfile.FIFOTYPE,
            tarfile.CHRTYPE,
            tarfile.BLKTYPE,
            tarfile.GNUTYPE_SPARSE,
            tarfile.XHDTYPE,
            tarfile.GNUTYPE_LONGNAME,
        ):
            member = tarfile.TarInfo("untrusted")
            member.type, member.linkname = kind, "../outside"
            with self.subTest(kind=kind):
                await self.reject([*self.members, (member, b"")])
        await self.reject([*self.members, self.members[0]])
        await self.reject(
            [*self.members, (tarfile.TarInfo("EXPERIMENT.YAML"), b"collision")]
        )
        await self.reject(
            [*self.members, (tarfile.TarInfo("experiment.yaml/child"), b"conflict")],
            errors=(ValueError, NotADirectoryError, FileExistsError),
        )

    async def test_invalid_xz_checksums_truncation_and_multiple_streams_are_rejected(
        self,
    ):
        """D: complete compression framing is required, including trailing bytes."""
        original = self.w.archive.read_bytes()
        damaged = bytearray(original)
        damaged[-16] ^= 0xFF
        for content in (
            b"",
            b"not xz",
            original[:50],
            original[:-4],
            bytes(damaged),
            original + b"tail",
            original + lzma.compress(b""),
        ):
            with self.subTest(size=len(content)):
                self.bad.write_bytes(content)
                with self.assertRaises((ValueError, lzma.LZMAError, EOFError)):
                    await self.w.importer.inspect(self.bad)
                self.assert_work_clean()

    async def test_tar_checksum_padding_and_terminator_are_checked(self):
        """D: malformed raw tar is rejected even when its outer XZ checksum is valid."""
        raw = lzma.decompress(self.w.archive.read_bytes())
        checksum = bytearray(raw)
        checksum[0] ^= 0x01
        file_padding = bytearray(raw)
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
            member = next(m for m in archive if m.isfile() and m.size % 512)
            file_padding[member.offset_data + member.size] = 1
            end = max(
                m.offset_data + (m.size + 511) // 512 * 512
                for m in archive.getmembers()
            )
        for content in (
            bytes(checksum),
            bytes(file_padding),
            raw[: end + 512],
            raw[:end] + b"x" * 512,
        ):
            self.bad.write_bytes(lzma.compress(content, preset=0))
            with (
                self.subTest(size=len(content)),
                self.assertRaises((ValueError, tarfile.TarError)),
            ):
                await self.w.importer.inspect(self.bad)
        self.assert_work_clean()

    async def test_compressed_unpacked_member_and_manifest_limit_boundaries(self):
        """D: exact small limits pass, one byte/member below the actual requirement fails."""
        settings = read_json(self.w.config)
        measured = {
            "max_archive_bytes": self.w.archive.stat().st_size,
            "max_unpacked_bytes": sum(m.size for m, _ in self.members),
            "max_members": len(self.members),
            "max_manifest_bytes": next(
                m.size for m, _ in self.members if m.name == "manifest.json"
            ),
        }
        for key, boundary in measured.items():
            with self.subTest(key=key):
                config = self.w.root / f"{key}-exact.json"
                write_json(config, {**settings, key: boundary})
                importer = ExperimentArchiver(
                    self.w.target, self.w.target_manager, config_path=config
                )
                await importer.inspect(self.w.archive)
                config = self.w.root / f"{key}-below.json"
                write_json(config, {**settings, key: boundary - 1})
                importer = ExperimentArchiver(
                    self.w.target, self.w.target_manager, config_path=config
                )
                with self.assertRaises(StorageCapacityError):
                    await importer.inspect(self.w.archive)
        self.assert_work_clean()

    async def test_decoder_memory_and_large_declared_sizes_are_bounded_without_large_files(
        self,
    ):
        """D: tiny configured bounds exercise capacity failures without consuming large resources."""
        settings = read_json(self.w.config)
        config = self.w.root / "decoder-settings.json"
        write_json(config, {**settings, "max_decompression_memory_bytes": 1024})
        importer = ExperimentArchiver(
            self.w.target, self.w.target_manager, config_path=config
        )
        with self.assertRaises(lzma.LZMAError):
            await importer.inspect(self.w.archive)
        config = self.w.root / "declared-size-settings.json"
        write_json(config, {**settings, "max_unpacked_bytes": 1024})
        importer = ExperimentArchiver(
            self.w.target, self.w.target_manager, config_path=config
        )
        header = tarfile.TarInfo("too-large")
        header.size = 1024 * 1024
        self.bad.write_bytes(
            lzma.compress(
                header.tobuf(format=tarfile.USTAR_FORMAT) + bytes(1024), preset=0
            )
        )
        with self.assertRaises(StorageCapacityError):
            await importer.inspect(self.bad)
        self.assertLess(self.bad.stat().st_size, 1024)

    async def test_disk_reserve_failure_is_simulated_only_in_owned_workspace(self):
        """D/F: low space is injected for staging only; machine disks and logger stay untouched."""
        actual = shutil.disk_usage

        def low_staging_space(path):
            if any(part.startswith("experiment-archive-") for part in Path(path).parts):
                return SimpleNamespace(free=0)
            return actual(path)

        with (
            patch(
                "core.experimentarchiver.shutil.disk_usage",
                side_effect=low_staging_space,
            ),
            self.assertRaises(StorageCapacityError),
        ):
            await self.w.importer.inspect(self.w.archive)
        self.assert_work_clean()

    async def test_real_directory_junction_is_rejected_without_following_it(self):
        """A/D: an owned junction/symlink cannot redirect source collection or cleanup."""
        outside = self.w.root / "outside"
        outside.mkdir()
        (outside / "sentinel").write_bytes(b"preserve")
        link = self.w.state.experiment_directory / "shared_data/resources/tree/link"
        if os.name == "nt":
            await asyncio.to_thread(
                subprocess.run,
                ["cmd", "/c", "mklink", "/J", str(link), str(outside)],
                check=True,
                capture_output=True,
            )
        else:
            link.symlink_to(outside, target_is_directory=True)
        try:
            with self.assertRaises(ValueError):
                await self.w.archiver.create(
                    self.w.state, self.w.root / "linked.tar.xz"
                )
            self.assertEqual((outside / "sentinel").read_bytes(), b"preserve")
        finally:
            link.rmdir() if os.name == "nt" else link.unlink()
        self.assert_work_clean()

    async def test_ustar_unrepresentable_unicode_leaf_fails_without_publication(self):
        """D/F: short Windows paths can still exceed USTAR's UTF-8 leaf-name limit."""
        root = self.w.state.experiment_directory / "shared_data/resources/tree"
        (root / ("界" * 40)).write_bytes(b"small")
        output = self.w.root / "long-name.tar.xz"
        with self.assertRaises(ValueError):
            await self.w.archiver.create(self.w.state, output)
        self.assertFalse(output.exists())
        self.assert_work_clean()
