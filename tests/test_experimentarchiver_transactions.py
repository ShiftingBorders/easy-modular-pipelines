"""Approved archive plan F/E: real owner crashes around filesystem publication."""

import asyncio
import shutil

from core.runner_utils.runtimeio import read_json
from tests.helpers.archives import ArchiveTestCase, inventory


class ArchivePublicationTests(ArchiveTestCase):
    async def test_native_owner_crash_before_and_after_archive_publication(self):
        """F: only an already published complete file is usable after abrupt process exit."""
        await self.w.prepare()
        before = inventory(self.w.state.experiment_directory)
        for phase in ("before_archive", "after_archive"):
            with self.subTest(phase=phase):
                child, ownership, _ = await self.w.start_child(
                    "create", project=self.w.source, phase=phase
                )
                self.assertEqual(await asyncio.wait_for(child.wait(), 30), 23)
                self.assertEqual(
                    read_json(ownership.with_suffix(".phase.json"))["phase"], phase
                )
                self.assertEqual(self.w.archive.exists(), phase == "after_archive")
                self.assertEqual(inventory(self.w.state.experiment_directory), before)
        checked = await self.w.importer.inspect(self.w.archive)
        self.assertEqual(checked["manifest"]["source_experiment_id"], "archive-source")
        saved = self.w.archive.read_bytes()
        with self.assertRaises(FileExistsError):
            await self.w.create()
        self.assertEqual(self.w.archive.read_bytes(), saved)
        # Crashes may leave unpublished staging; none is treated as a ready archive.
        self.assertTrue(list(self.w.root.rglob("experiment-archive-*")))

    async def test_native_owner_crash_at_module_and_bundle_rename_preserves_retriable_data(
        self,
    ):
        """F/E: disk/DB state after each actual rename determines safe reuse and repeat behavior."""
        await self.source_archive()
        archive_bytes = self.w.archive.read_bytes()
        module = self.w.target / "modules/portable-stage/1"
        for phase in ("before_module", "after_module", "before_bundle", "after_bundle"):
            with self.subTest(phase=phase):
                # Remove only this fixture's previous successful local publications.
                # Registered hashes and uploaded archives deliberately survive and are reused.
                for path in (module, self.w.destination):
                    if path.exists():
                        self.assertTrue(path.resolve().is_relative_to(self.w.root))
                        self.assertFalse(path.is_symlink() or path.is_junction())
                        shutil.rmtree(path)
                child, ownership, _ = await self.w.start_child("install", phase=phase)
                self.assertEqual(await asyncio.wait_for(child.wait(), 40), 23)
                self.assertEqual(
                    read_json(ownership.with_suffix(".phase.json"))["phase"], phase
                )
                self.assertEqual(module.exists(), phase != "before_module")
                self.assertEqual(self.w.destination.exists(), phase == "after_bundle")
                self.assertTrue(
                    self.w.target_hashes.get_module_hash("portable-stage", "1")
                )
                self.assertTrue(
                    (self.w.filer.root / "modules/portable-stage/1").is_file()
                )
                self.assertEqual(self.w.archive.read_bytes(), archive_bytes)
                if phase == "after_bundle":
                    before = inventory(self.w.destination)
                    with self.assertRaises(FileExistsError):
                        await self.w.importer.install(
                            self.w.archive, self.w.destination
                        )
                    self.assertEqual(inventory(self.w.destination), before)
                    destination = self.w.target / "repeat-after-published"
                else:
                    destination = self.w.destination
                completed = await self.w.importer.install(self.w.archive, destination)
                self.assertFalse(completed["modules"][0]["registered"])
                self.assertTrue((destination / "experiment.yaml").is_file())
                self.assertTrue(
                    self.w.target_manager.validate_module("portable-stage", "1", module)
                )
