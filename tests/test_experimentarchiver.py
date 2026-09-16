"""Approved archive plan A/B/C/E: inputs, stopped state and portable installation."""

import copy
import json
import os
from pathlib import Path
from unittest.mock import patch

import yaml

from core.experimentarchiver import ExperimentArchiver
from core.experimentassembler import ExperimentAssembler
from core.runner_utils.runtimeio import write_json
from core.storage_errors import StorageConflict
from tests.helpers.archives import ArchiveTestCase, inventory


class ArchiveInputTests(ArchiveTestCase):
    async def test_missing_invalid_json_and_zero_positive_limits_are_rejected(self):
        """A: configuration must be a complete JSON object with valid positive limits."""
        for name, content in (("invalid.json", "{"), ("array.json", "[]")):
            config = self.w.root / name
            config.write_text(content, encoding="utf-8")
            with self.subTest(name=name), self.assertRaises((TypeError, ValueError)):
                ExperimentArchiver(self.w.source, self.w.manager, config_path=config)
        for config in (self.w.root / "missing.json", self.w.root):
            with self.subTest(config=config), self.assertRaises(OSError):
                ExperimentArchiver(self.w.source, self.w.manager, config_path=config)
        settings = json.loads(self.w.config.read_text())
        for key in settings.keys() - {"min_free_bytes", "compression_preset"}:
            config = self.w.root / f"zero-{key}.json"
            write_json(config, {**settings, key: 0})
            with self.subTest(key=key), self.assertRaises(ValueError):
                ExperimentArchiver(self.w.source, self.w.manager, config_path=config)

    async def test_constructor_is_read_only_and_rejects_invalid_settings(self):
        """A: construction validates settings without opening stores or launching work."""
        before = inventory(self.w.root)
        ExperimentArchiver(self.w.source, self.w.manager, config_path=self.w.config)
        self.assertEqual(inventory(self.w.root), before)
        self.assertEqual(self.w.filer.requests, [])
        settings = json.loads(self.w.config.read_text())
        cases = [{}, {**settings, "extra": 1}, {**settings, "schema_version": 2}]
        for key in settings:
            for value in (True, "1", -1, None):
                cases.append({**settings, key: value})
        cases.append({**settings, "compression_preset": 10})
        for index, bad in enumerate(cases):
            with self.subTest(settings=bad):
                config = self.w.root / f"invalid-settings-{index}.json"
                write_json(config, bad)
                with self.assertRaises((TypeError, ValueError)):
                    ExperimentArchiver(
                        self.w.source, self.w.manager, config_path=config
                    )

    async def test_absolute_paths_and_configuration_are_independent_of_cwd(self):
        """A/C: template resource paths resolve from the template, not caller cwd."""
        await self.w.prepare()
        old = Path.cwd()
        try:
            os.chdir(self.w.target)
            result = await self.w.create()
            checked = await self.w.importer.inspect(self.w.archive)
        finally:
            os.chdir(old)
        self.assertEqual(checked["manifest"], result["manifest"])
        self.assert_work_clean()

    async def test_public_paths_reject_relative_missing_and_non_file_inputs(self):
        """A: invalid archive/config/destination inputs preserve existing data."""
        for value in (None, 2, Path("relative")):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                ExperimentArchiver(value, self.w.manager)
        for value in (
            None,
            2,
            Path("relative"),
            self.w.root / "missing",
            self.w.source,
        ):
            with (
                self.subTest(value=value),
                self.assertRaises((TypeError, ValueError, FileNotFoundError)),
            ):
                await self.w.importer.inspect(value)
        await self.source_archive()
        existing = self.w.root / "existing"
        existing.write_text("unchanged", encoding="utf-8")
        for destination in (
            Path("relative"),
            existing,
            self.w.target,
            self.w.target / "modules/new",
            self.w.target / "work/new",
        ):
            with (
                self.subTest(destination=destination),
                self.assertRaises((ValueError, FileExistsError)),
            ):
                await self.w.importer.install(self.w.archive, destination)
        self.assertEqual(existing.read_text(), "unchanged")
        self.assert_work_clean()


class ArchiveCreationTests(ArchiveTestCase):
    async def test_incomplete_restore_unconfirmed_requests_and_wrong_identity_are_rejected(
        self,
    ):
        """B: terminal flags cannot override unresolved transaction/queue/registry state."""
        await self.w.prepare(services=True)
        state = self.w.state
        marker = (
            self.w.source
            / "controller/restore_transactions"
            / f"{state.experiment_directory.name}.json"
        )
        write_json(marker, {"phase": "files_installed"})
        with self.assertRaises(RuntimeError):
            await self.w.create()
        marker.unlink()
        instance = next(iter(state.services.values()))
        instance.pending_requests.append({"request_id": "unfinished"})
        try:
            with self.assertRaises(RuntimeError):
                await self.w.create()
        finally:
            instance.pending_requests.clear()
        identifier = state.experiment_id
        try:
            state.experiment_id = "unregistered"
            with self.assertRaises(FileNotFoundError):
                await self.w.create()
        finally:
            state.experiment_id = identifier
        self.assertFalse(self.w.archive.exists())

    async def test_stopped_archive_contains_only_applied_inputs_and_preserves_source(
        self,
    ):
        """B/C: actual stop, ignored pending YAML edits, exact inventory and no runtime tokens."""
        await self.w.prepare(versions=True)
        self.w.state.template_path.write_text(
            "unapplied invalid YAML: [", encoding="utf-8"
        )
        before = inventory(self.w.state.experiment_directory)
        result = await self.w.create()
        manifest = result["manifest"]
        self.assertEqual(len(manifest["modules"]), 2)
        self.assertEqual({m["version"] for m in manifest["modules"]}, {"1", "2"})
        self.assertIn("resources/tree/empty", manifest["directories"])
        self.assertIn("resources/tree/данные с пробелом.txt", manifest["files"])
        self.assertTrue(
            all(
                name.startswith(("modules/", "resources/")) or name == "experiment.yaml"
                for name in manifest["files"]
            )
        )
        self.assertEqual(inventory(self.w.state.experiment_directory), before)
        with self.assertRaises(FileExistsError):
            await self.w.create()
        with self.assertRaises(ValueError):
            await self.w.archiver.create(
                self.w.state, self.w.state.experiment_directory / "archive.tar.xz"
            )
        self.assert_work_clean()

    async def test_pause_and_active_stage_cannot_be_archived(self):
        """B: real running/paused stage ownership cannot be confused with shutdown."""
        gate = self.w.root / "release-stage"
        self.w.gates.append(gate)
        await self.w.prepare(stopped=False, gate=gate)
        with self.assertRaises(RuntimeError):
            await self.w.create()
        await self.w.runner.resume()
        from tests.helpers.dag import wait_until

        await wait_until(lambda: self.w.state.active_attempt is not None)
        with self.assertRaises(RuntimeError):
            await self.w.create()
        self.assertFalse(self.w.archive.exists())
        gate.touch()
        await self.w.runner.stop()

    async def test_inconsistent_template_missing_code_hash_or_resource_is_rejected(
        self,
    ):
        """B: integrity failures do not publish an archive."""
        await self.w.prepare()
        state = self.w.state
        original = copy.deepcopy(state.template)
        state.template["name"] = "different"
        with self.assertRaises(ValueError):
            await self.w.create()
        state.template = original
        module = original["stages"][0]["module"]
        code = (
            state.experiment_directory
            / "modules"
            / module["name"]
            / module["version"]
            / "main.py"
        )
        saved = code.read_bytes()
        code.write_bytes(b"changed")
        with self.assertRaises(ValueError):
            await self.w.create()
        code.write_bytes(saved)
        self.w.hashes.remove_module_hash(module["name"], module["version"])
        with self.assertRaises(ValueError):
            await self.w.create()
        self.w.hashes.add_module_hash(module["name"], module["version"], module["hash"])
        resource = state.experiment_directory / "shared_data/resources/source.txt"
        resource.unlink()
        with self.assertRaises(FileNotFoundError):
            await self.w.create()
        self.assertFalse(self.w.archive.exists())
        self.assert_work_clean()


class ArchiveInstallationTests(ArchiveTestCase):
    async def test_incomplete_storage_pairs_and_file_destinations_do_not_get_overwritten(
        self,
    ):
        """E: hash-only/archive-only registrations and local files remain explicit conflicts."""
        await self.source_archive()
        package = self.w.filer.root / "modules/portable-stage/1"
        package.parent.mkdir(parents=True)
        for kind in ("hash_only", "archive_only"):
            with self.subTest(kind=kind):
                if kind == "hash_only":
                    self.w.target_hashes.add_module_hash(
                        "portable-stage",
                        "1",
                        self.w.definition["stages"][0]["module"]["hash"],
                    )
                else:
                    package.write_bytes(b"existing package")
                with self.assertRaises(StorageConflict):
                    await self.w.importer.install(self.w.archive, self.w.destination)
                self.assertFalse(self.w.destination.exists())
                self.w.target_hashes.remove_module_hash("portable-stage", "1")
                package.unlink(missing_ok=True)
        target = self.w.target / "modules/portable-stage/1"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"local file")
        with self.assertRaises(StorageConflict):
            await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertEqual(target.read_bytes(), b"local file")

    async def test_later_upload_failure_keeps_completed_modules_for_repeat(self):
        """E/F: failure on the second real HTTP upload does not erase the first registration."""
        await self.source_archive(versions=True)
        self.w.filer.fail_paths["/modules/portable-stage/2"] = 503
        from core.storage_errors import StorageUnavailable

        with self.assertRaises(StorageUnavailable) as raised:
            await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertTrue((self.w.target / "modules/portable-stage/1/main.py").is_file())
        self.assertFalse((self.w.target / "modules/portable-stage/2").exists())
        self.assertTrue(self.w.target_hashes.get_module_hash("portable-stage", "1"))
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "2"), ""
        )
        self.assertIn('"version": "1"', " ".join(raised.exception.__notes__))
        self.w.filer.fail_paths.clear()
        result = await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertEqual([m["registered"] for m in result["modules"]], [False, True])
        self.assertEqual([m["installed"] for m in result["modules"]], [False, True])

    async def test_install_registers_real_packages_and_returns_assemblable_template(
        self,
    ):
        """E: fresh SQLite/HTTP stores, two versions, relative resources and actual assembler."""
        await self.source_archive(versions=True)
        result = await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertEqual(len(result["modules"]), 2)
        self.assertTrue(
            all(m["registered"] and m["installed"] for m in result["modules"])
        )
        template = Path(result["template_path"])
        self.assertEqual(
            yaml.safe_load(template.read_text())["resources"][0]["path"],
            "resources/source.txt",
        )
        self.assertFalse((self.w.target / "experiments.json").exists())
        state = await ExperimentAssembler(
            self.w.target, self.w.target_manager
        ).assemble(template, "assembled-import")
        self.assertEqual(
            (
                state.experiment_directory / "shared_data/resources/source.txt"
            ).read_text(),
            "portable input\n",
        )
        for entry in result["modules"]:
            folder = self.w.target / "modules" / entry["name"] / entry["version"]
            self.assertTrue(
                self.w.target_manager.validate_module(
                    entry["name"], entry["version"], folder
                )
            )
            self.assertTrue(
                (
                    self.w.filer.root / "modules" / entry["name"] / entry["version"]
                ).is_file()
            )
        self.assert_work_clean()

    async def test_identical_modules_are_reused_and_existing_bundle_is_not_replaced(
        self,
    ):
        """E: a second destination reuses identical modules; the first bundle is immutable."""
        await self.source_archive()
        first = await self.w.importer.install(self.w.archive, self.w.destination)
        before = inventory(self.w.target / "modules")
        second = await self.w.importer.install(self.w.archive, self.w.target / "second")
        self.assertTrue(
            all(not m["registered"] and not m["installed"] for m in second["modules"])
        )
        self.assertEqual(first["archive_id"], second["archive_id"])
        with self.assertRaises(FileExistsError):
            await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertEqual(inventory(self.w.target / "modules"), before)

    async def test_all_conflicts_are_preflighted_before_any_registration(self):
        """E: a conflict in the later module prevents installing earlier modules."""
        await self.source_archive(versions=True)
        self.w.target_hashes.add_module_hash("portable-stage", "2", "a" * 64)
        with self.assertRaises(StorageConflict):
            await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "1"), ""
        )
        self.assertEqual(list(self.w.filer.root.rglob("*")), [])
        self.assertFalse(self.w.destination.exists())

    async def test_local_conflict_preserves_existing_bytes(self):
        """E: an unrelated existing module directory is never overwritten."""
        await self.source_archive()
        folder = self.w.target / "modules/portable-stage/1"
        folder.mkdir(parents=True)
        (folder / "main.py").write_text("old code", encoding="utf-8")
        before = inventory(folder)
        with self.assertRaises(StorageConflict):
            await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertEqual(inventory(folder), before)
        self.assertEqual(
            self.w.target_hashes.get_module_hash("portable-stage", "1"), ""
        )

    async def test_partial_local_failure_reports_registered_module_and_repeat_finishes(
        self,
    ):
        """E/F: completed registration remains usable after a later local failure."""
        await self.source_archive()
        original_copy = self.w.importer._copy

        def fail_module(source, target):
            if target.name == "module":
                raise OSError("local copy failed")
            return original_copy(source, target)

        with (
            patch.object(self.w.importer, "_copy", side_effect=fail_module),
            self.assertRaises(OSError) as raised,
        ):
            await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertIn('"registered": true', " ".join(raised.exception.__notes__))
        self.assertFalse(self.w.destination.exists())
        result = await self.w.importer.install(self.w.archive, self.w.destination)
        self.assertFalse(result["modules"][0]["registered"])
        self.assertTrue(result["modules"][0]["installed"])
        self.assert_work_clean()
