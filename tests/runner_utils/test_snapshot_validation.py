"""Approved snapshots.md A/B/C/F: real files and strict archive validation."""

import copy
import hashlib
import os
import shutil
import unittest
from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4

from core.logger_utils.events import LoggingError
from core.runner_utils.runtimeio import read_json, write_json
from tests.helpers.snapshots import SnapshotWorkspace, file_inventory


class SnapshotValidationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = SnapshotWorkspace(services=False, keep=2)
        self.w.template["storage"]["min_snapshot_free_bytes"] = 1
        self.addAsyncCleanup(self.w.close)
        self.runner = await self.w.launch()
        await self.runner.step()
        self.snapshot = await self.runner.snapshot("valid")
        self.archive = self.w.archive(self.snapshot["snapshot_id"])

    def clone(self):
        destination = self.w.root / "validation" / str(uuid4())
        shutil.copytree(self.archive, destination)
        return destination

    def rewrite_inventory(self, directory, relative):
        manifest = read_json(directory / "manifest.json")
        path = directory / relative
        manifest["files"][relative] = {
            "size_bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        write_json(directory / "manifest.json", manifest)

    async def test_manifest_schema_identity_cursor_and_applied_template_are_checked(
        self,
    ):
        """A2/A4/C1: structurally valid JSON still must describe one consistent run."""
        original = read_json(self.archive / "manifest.json")
        changes = [
            (("schema_version",), True),
            (("schema_version",), 2),
            (("snapshot_id",), "invalid"),
            (("sequence",), 0),
            (("sequence",), True),
            (("kind",), "partial"),
            (("created_at",), "2026-01-01T01:00:00+01:00"),
            (("state", "active_attempt"), {}),
            (("state", "experiment_id"), "foreign"),
            (("state", "cycle_number"), 3),
            (("state", "template_path"), "../experiment.yaml"),
            (("state", "template", "name"), "edited"),
            (("state", "last_result"), {"wrong": True}),
        ]
        for keys, value in changes:
            with self.subTest(keys=keys, value=value):
                manifest = copy.deepcopy(original)
                parent = manifest
                for key in keys[:-1]:
                    parent = parent[key]
                parent[keys[-1]] = value
                with self.assertRaises((ValueError, TypeError, KeyError)):
                    self.runner._snapshots._validate_snapshot(
                        self.archive, manifest=manifest
                    )
        self.assertEqual(read_json(self.archive / "manifest.json"), original)

    async def test_missing_extra_changed_files_and_invalid_journal_are_rejected(self):
        """C1/C2: checksum, inventory and actual journal validation are independent."""
        for mutation in (
            "missing",
            "extra",
            "changed",
            "module",
            "journal",
            "result",
            "directory",
            "control",
        ):
            with self.subTest(mutation=mutation):
                directory = self.clone()
                if mutation == "missing":
                    (directory / "files/experiment.yaml").unlink()
                elif mutation == "extra":
                    (directory / "files/shared_data/extra").write_bytes(b"extra")
                elif mutation == "directory":
                    (directory / "files/shared_data/extra").mkdir()
                elif mutation == "changed":
                    (directory / "files/experiment.yaml").write_bytes(b"changed")
                elif mutation == "module":
                    relative = "files/modules/stage-1/1/main.py"
                    with (directory / relative).open("a", encoding="utf-8") as stream:
                        stream.write("\n# modified after registration\n")
                    self.rewrite_inventory(directory, relative)
                elif mutation == "control":
                    manifest = read_json(directory / "manifest.json")
                    relative = "files/" + manifest["state"]["last_result_path"].replace(
                        "execution_result.json", "executor.token"
                    )
                    (directory / relative).write_bytes(b"runtime-only token")
                    self.rewrite_inventory(directory, relative)
                elif mutation == "journal":
                    relative = "journal/journal.sqlite"
                    (directory / relative).write_bytes(b"not a sqlite database")
                    self.rewrite_inventory(directory, relative)
                else:
                    manifest = read_json(directory / "manifest.json")
                    relative = "files/" + manifest["state"]["last_result_path"]
                    result = read_json(directory / relative)
                    result["experiment_id"] = "foreign"
                    write_json(directory / relative, result)
                    self.rewrite_inventory(directory, relative)
                with self.assertRaises(
                    (ValueError, TypeError, KeyError, OSError, LoggingError)
                ):
                    self.runner._snapshots._validate_snapshot(directory)

    async def test_unsafe_member_names_case_collisions_and_runtime_files_are_rejected(
        self,
    ):
        """C3/B5: archive metadata cannot introduce host paths or runtime controls."""
        for name in (
            "../outside",
            "/absolute",
            "C:/outside",
            "files/../outside",
            "files\\shared_data\\outside",
            "files/shared_data/name.",
            "files/runner/state.json",
            "files/shared_artifacts/services/control",
        ):
            with self.subTest(name=name):
                manifest = read_json(self.archive / "manifest.json")
                manifest["files"][name] = {"size_bytes": 0, "sha256": "0" * 64}
                with self.assertRaises(ValueError):
                    self.runner._snapshots._validate_snapshot(
                        self.archive, manifest=manifest
                    )
        manifest = read_json(self.archive / "manifest.json")
        manifest["directories"].append("FILES")
        with self.assertRaisesRegex(ValueError, "collide"):
            self.runner._snapshots._validate_snapshot(self.archive, manifest=manifest)

    @unittest.skipUnless(os.name == "nt", "Windows junction boundary")
    async def test_actual_junction_is_rejected_without_touching_its_target(self):
        """C3: a real Windows junction must never be traversed as snapshot payload."""
        import _winapi

        directory = self.clone()
        outside = self.w.root / "outside"
        outside.mkdir()
        sentinel = outside / "keep"
        sentinel.write_bytes(b"untouched")
        link = directory / "files/shared_data/linked"
        _winapi.CreateJunction(str(outside), str(link))
        try:
            with self.assertRaises(ValueError):
                self.runner._snapshots._validate_snapshot(directory)
            self.assertEqual(sentinel.read_bytes(), b"untouched")
        finally:
            os.rmdir(link)

    async def test_latest_valid_and_retention_ignore_incomplete_and_corrupt_archives(
        self,
    ):
        """C4/C5: a broken latest archive cannot replace the last usable checkpoint."""
        newer = await self.runner.snapshot("newer")
        newer_path = self.w.archive(newer["snapshot_id"])
        (newer_path / "files/experiment.yaml").write_bytes(b"corrupt")
        pending = newer_path.parent / str(uuid4())
        pending.mkdir()
        (pending / "partial").write_bytes(b"unfinished")
        latest = self.runner._snapshots.latest_valid(
            self.runner._state.experiment_directory
        )
        self.assertEqual(latest["snapshot_id"], self.snapshot["snapshot_id"])
        preceding = await self.runner.snapshot("preceding")
        retained = await self.runner.snapshot("retained")
        self.assertFalse(self.archive.exists())
        self.assertTrue(self.w.archive(retained["snapshot_id"]).is_dir())
        self.assertTrue(self.w.archive(preceding["snapshot_id"]).is_dir())
        self.assertTrue(newer_path.is_dir())
        self.assertTrue((pending / "partial").is_file())
        (self.w.archive(retained["snapshot_id"]) / "manifest.json").unlink()
        (self.w.archive(preceding["snapshot_id"]) / "manifest.json").unlink()
        with self.assertRaises(FileNotFoundError):
            self.runner._snapshots.latest_valid(self.runner._state.experiment_directory)

    async def test_sequence_order_survives_backwards_wall_clock(self):
        """C4: choosing the latest snapshot does not depend on clock monotonicity."""
        with patch("core.runner_utils.snapshots.datetime", wraps=datetime) as clock:
            clock.now.return_value = datetime(2000, 1, 1, tzinfo=UTC)
            older_time = await self.runner.snapshot("clock moved backwards")
        self.assertLess(older_time["created_at"], self.snapshot["created_at"])
        self.assertGreater(older_time["sequence"], self.snapshot["sequence"])
        self.assertEqual(
            self.runner._snapshots.latest_valid(
                self.runner._state.experiment_directory
            )["snapshot_id"],
            older_time["snapshot_id"],
        )

    async def test_invalid_selection_is_rejected_before_changing_experiment_files(self):
        """F1/B2: bad labels, IDs and corrupt candidates leave the selected data intact."""
        before = file_inventory(
            self.runner._state.experiment_directory / "shared_artifacts"
        )
        for label in ("", 0, {}):
            with self.subTest(label=label), self.assertRaises((TypeError, ValueError)):
                await self.runner.snapshot(label)
        for identifier in ("bad-id", str(uuid4())):
            with (
                self.subTest(identifier=identifier),
                self.assertRaises((ValueError, FileNotFoundError)),
            ):
                await self.runner.rollback(identifier)
        (self.archive / "files/experiment.yaml").write_bytes(b"damaged")
        with self.assertRaises(ValueError):
            await self.runner.rollback(self.snapshot["snapshot_id"])
        self.assertEqual(
            file_inventory(
                self.runner._state.experiment_directory / "shared_artifacts"
            ),
            before,
        )
        self.assertEqual(self.runner.get_state()["phase"], "waiting")

    async def test_insufficient_space_does_not_publish_a_new_snapshot(self):
        """B2/B4: simulate disk exhaustion without filling the host disk."""
        usage = shutil.disk_usage(self.w.root)
        actual_usage = shutil.disk_usage

        def available_space(path):
            if path == self.runner._state.experiment_directory:
                return type(usage)(usage.total, usage.total, 0)
            return actual_usage(path)

        with (
            patch(
                "core.runner_utils.snapshots.shutil.disk_usage",
                side_effect=available_space,
            ),
            self.assertRaises(OSError),
        ):
            await self.runner.snapshot("no space")
        self.assertEqual(
            [item["snapshot_id"] for item in self.w.manifests()],
            [self.snapshot["snapshot_id"]],
        )

    async def test_required_exports_and_service_definitions_must_match_snapshot(self):
        """C2: required RAM state, service settings and export ownership are validated."""
        w = SnapshotWorkspace()
        self.addAsyncCleanup(w.close)
        runner = await w.launch()
        snapshot = await runner.snapshot()
        archive = w.archive(snapshot["snapshot_id"])
        original = read_json(archive / "manifest.json")
        sid = w.socket["service_id"]
        for fault in ("missing", "foreign", "commands", "definition", "queue"):
            with self.subTest(fault=fault):
                manifest = copy.deepcopy(original)
                if fault == "missing":
                    manifest["services"].pop(sid)
                elif fault == "foreign":
                    manifest["services"][sid] = "../outside"
                elif fault == "commands":
                    manifest["services"][w.commands["service_id"]] = manifest[
                        "services"
                    ][sid]
                elif fault == "definition":
                    manifest["state"]["services"][sid]["definition"]["settings"][
                        "different"
                    ] = True
                else:
                    manifest["state"]["services"][sid]["pending_requests"] = [
                        {"request_id": str(uuid4())}
                    ]
                with self.assertRaises((ValueError, TypeError, KeyError)):
                    runner._snapshots._validate_snapshot(archive, manifest=manifest)

    async def test_snapshot_from_another_experiment_is_refused_before_replacement(self):
        """F1: selecting a valid archive from another experiment cannot authorize rollback."""
        other = SnapshotWorkspace(services=False)
        self.addAsyncCleanup(other.close)
        runner = await other.launch()
        foreign = await runner.snapshot()
        location = self.archive.parent / foreign["snapshot_id"]
        shutil.copytree(other.archive(foreign["snapshot_id"]), location)
        root = self.runner._state.experiment_directory
        before = file_inventory(root / "shared_artifacts")
        with self.assertRaises(ValueError):
            await self.runner.rollback(foreign["snapshot_id"])
        self.assertEqual(file_inventory(root / "shared_artifacts"), before)
        self.assertEqual(self.runner.get_state()["phase"], "waiting")
