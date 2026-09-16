"""Approved basic_dag.md A1-A7: real files/SQLite and mocked Filer HTTP."""

import asyncio
import copy
import hashlib
import json
import os
import shutil
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import yaml

from core.experimentassembler import ExperimentAssembler, find_experiment
from core.storage_errors import StorageUnavailable
from tests.helpers.dag import DagWorkspace, wait_until


class ExperimentAssemblerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.assembler = ExperimentAssembler(
            self.workspace.root, self.workspace.manager
        )
        self.template = self.workspace.template()
        self.path = self.workspace.write_template(self.template)

    def test_complete_template_preserves_explicit_values(self):
        """A1: valid explicit values and original source are retained."""
        text, result = self.assembler.load_template(self.path)
        self.assertEqual(text, self.path.read_text(encoding="utf-8"))
        self.assertEqual(result, self.template)

    def test_rejects_missing_and_unknown_template_fields(self):
        """A1: absent mandatory controls and unknown fields do not create a build."""
        for key in self.template:
            with self.subTest(missing=key):
                candidate = copy.deepcopy(self.template)
                del candidate[key]
                with self.assertRaises((ValueError, TypeError)):
                    self.assembler.load_template(
                        self.workspace.write_template(candidate)
                    )
        candidate = {**self.template, "unknown": True}
        with self.assertRaises(ValueError):
            self.assembler.load_template(self.workspace.write_template(candidate))
        self.assertFalse((self.workspace.root / "experiments").exists())

    def test_rejects_invalid_types_ids_hashes_and_policy_boundaries(self):
        """A1: public field boundaries, including booleans masquerading as counts."""
        cases = [
            (("cycles",), True),
            (("cycles",), 0),
            (("keep_attempts",), -1),
            (("schema_version",), True),
            (("start_timeout",), 0),
            (("runner_timeout_margin_seconds",), float("inf")),
            (("stages", 0, "stage_id"), "not-a-uuid"),
            (("stages", 0, "module", "hash"), "x" * 64),
            (("stages", 0, "module", "name"), "../outside"),
            (("stages", 0, "module", "version"), "../outside"),
            (("stages", 0, "errors", "retries"), True),
            (("stages", 0, "errors", "retry_delay_seconds"), -1),
            (("stages", 0, "errors", "on_exhausted"), "invented"),
            (("stages", 0, "timeout_seconds"), False),
            (("logging", "max_event_bytes"), 0),
            (("logging", "min_free_bytes"), -1),
            (("unknown_state", "recovery_limit"), True),
        ]
        for keys, value in cases:
            with self.subTest(keys=keys, value=value):
                candidate = copy.deepcopy(self.template)
                target = candidate
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = value
                with self.assertRaises((TypeError, ValueError)):
                    self.assembler.load_template(
                        self.workspace.write_template(candidate)
                    )
        candidate = copy.deepcopy(self.template)
        candidate["stages"].append(copy.deepcopy(candidate["stages"][0]))
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            self.assembler.load_template(self.workspace.write_template(candidate))

    def test_rejects_empty_dag_and_unsupported_modes(self):
        """A2/snapshots A4: empty DAG, malformed services and invalid modes fail early."""
        for field, value, error in (
            ("stages", [], ValueError),
            ("services", [{}], ValueError),
            ("snapshots", {"mode": "unknown", "keep": 1}, ValueError),
        ):
            with self.subTest(field=field):
                candidate = {**self.template, field: value}
                with self.assertRaises(error):
                    self.assembler.load_template(
                        self.workspace.write_template(candidate)
                    )

    def test_accepts_all_snapshot_modes(self):
        """Snapshots A4: each supported mode survives template validation."""
        for mode in ("off", "after_stage", "after_epoch"):
            with self.subTest(mode=mode):
                candidate = {**self.template, "snapshots": {"mode": mode, "keep": 2}}
                _, loaded = self.assembler.load_template(
                    self.workspace.write_template(candidate)
                )
                self.assertEqual(loaded["snapshots"], candidate["snapshots"])

    async def test_paths_resolve_from_config_in_another_cwd(self):
        """A3: config-relative and absolute resources work from an unrelated cwd."""
        config_folder = self.workspace.root / "settings"
        config_folder.mkdir()
        relative = config_folder / "relative.bin"
        relative.write_bytes(b"relative")
        absolute = self.workspace.root / "absolute.bin"
        absolute.write_bytes(b"absolute")
        self.template["resources"] = [
            {"name": "relative", "path": "relative.bin", "hash": None},
            {"name": "absolute", "path": str(absolute), "hash": None},
        ]
        path = self.workspace.write_template(self.template, "settings/template.yaml")
        previous = Path.cwd()
        other = self.workspace.root / "elsewhere"
        other.mkdir()
        try:
            os.chdir(other)
            state = await self.assembler.assemble(path, "paths")
        finally:
            os.chdir(previous)
        self.assertEqual(state.template["resources"][0]["path"], str(relative))
        self.assertEqual(state.template["resources"][1]["path"], str(absolute))
        for item, data in (("relative", b"relative"), ("absolute", b"absolute")):
            self.assertEqual(
                (
                    state.experiment_directory / "shared_data/resources" / item
                ).read_bytes(),
                data,
            )

    async def test_copies_only_required_versions_once_and_assigns_ids(self):
        """A4: shared code versions are copied once; definitions retain distinct IDs."""
        worker = self.template["stages"][0]["module"]
        newer = self.workspace.module("worker", "2")
        unused = self.workspace.module("unused", "1")
        stages = [
            self.workspace.stage(worker),
            self.workspace.stage(worker),
            self.workspace.stage(newer),
        ]
        del stages[1]["stage_id"]
        source_hash = self.workspace.manager.module_hash(
            "worker", self.workspace.root / "modules/worker/1"
        )
        with patch(
            "core.experimentassembler.shutil.copytree", wraps=shutil.copytree
        ) as copier:
            state = await self.assembler.assemble(
                self.workspace.write_template(self.workspace.template(stages)), "copies"
            )
        self.assertEqual(copier.call_count, 2)
        self.assertFalse(
            (state.experiment_directory / "modules" / unused["name"]).exists()
        )
        ids = [stage["stage_id"] for stage in state.template["stages"]]
        self.assertEqual(len(set(ids)), 3)
        for value in ids:
            UUID(value)
        self.assertEqual(ids[0], stages[0]["stage_id"])
        self.assertEqual(
            yaml.safe_load(state.template_path.read_text()), state.template
        )
        self.assertEqual(
            self.workspace.manager.module_hash(
                "worker", state.experiment_directory / "modules/worker/1"
            ),
            source_hash,
        )

    async def test_module_integrity_mismatch_missing_and_unavailable_database(self):
        """A5: all three hash sources must agree; storage failures propagate."""
        state = await self.assembler.assemble(self.path, "integrity")
        stage = state.template["stages"][0]
        with (
            patch.object(self.workspace.hashes, "get_module_hash", return_value=""),
            self.assertRaises(ValueError),
        ):
            self.assembler.check_module(state, stage)
        with (
            patch.object(
                self.workspace.hashes,
                "get_module_hash",
                side_effect=StorageUnavailable("offline"),
            ),
            self.assertRaises(StorageUnavailable),
        ):
            self.assembler.check_module(state, stage)
        stage["module"]["hash"] = "0" * 64
        with self.assertRaises(ValueError):
            self.assembler.check_module(state, stage)
        stage["module"]["hash"] = self.template["stages"][0]["module"]["hash"]
        (state.experiment_directory / "modules/worker/1/main.py").write_text(
            "changed", encoding="utf-8"
        )
        with self.assertRaises(ValueError):
            self.assembler.check_module(state, stage)

    async def test_resource_bytes_nested_tree_and_eight_mib_file(self):
        """A6: actual copies preserve small, nested, and 8 MiB resource contents."""
        source = self.workspace.root / "resources"
        (source / "nested/deeper").mkdir(parents=True)
        (source / "small.bin").write_bytes(b"\x00\xffsmall")
        (source / "nested/deeper/file.bin").write_bytes(b"nested")
        large = source / "large.bin"
        large.write_bytes(bytes(range(256)) * 32768)
        self.template["resources"] = [
            {"name": name, "path": str(source / filename), "hash": digest}
            for name, filename, digest in (
                ("small", "small.bin", hashlib.sha256(b"\x00\xffsmall").hexdigest()),
                ("tree", "nested", None),
                ("large", "large.bin", hashlib.sha256(large.read_bytes()).hexdigest()),
            )
        ]
        state = await self.assembler.assemble(
            self.workspace.write_template(self.template), "resources"
        )
        target = state.experiment_directory / "shared_data/resources"
        self.assertEqual(
            (target / "small").read_bytes(), (source / "small.bin").read_bytes()
        )
        self.assertEqual((target / "large").read_bytes(), large.read_bytes())
        self.assertEqual((target / "tree/deeper/file.bin").read_bytes(), b"nested")
        self.assembler.check_resources(state)
        (target / "small").write_bytes(b"wrong")
        with self.assertRaises(ValueError):
            self.assembler.check_resources(state)

    async def test_resource_hash_is_checked_on_start_and_missing_source_cleans_build(
        self,
    ):
        """A6/A7: assembly copies before hash checking; missing inputs leave no instance."""
        resource = self.workspace.root / "resource.bin"
        resource.write_bytes(b"content")
        self.template["resources"] = [
            {"name": "resource", "path": str(resource), "hash": "0" * 64}
        ]
        path = self.workspace.write_template(self.template)
        state = await self.assembler.assemble(path, "copied")
        with self.assertRaises(ValueError):
            self.assembler.check_resources(state)
        before = json.loads((self.workspace.root / "experiments.json").read_text())
        resource.unlink()
        with self.assertRaises(FileNotFoundError):
            await self.assembler.assemble(path, "missing")
        self.assertEqual(
            json.loads((self.workspace.root / "experiments.json").read_text()), before
        )
        self.assertEqual(
            list((self.workspace.root / "experiments").iterdir()),
            [state.experiment_directory],
        )

    async def test_id_conflict_and_safe_registry_lookup(self):
        """A7: composite IDs use a separate folder; conflicts never overwrite it."""
        state = await self.assembler.assemble(self.path, "child:parent")
        self.assertEqual(
            find_experiment(self.workspace.root, "child:parent"),
            state.experiment_directory,
        )
        self.assertNotEqual(state.experiment_directory.name, "child:parent")
        with self.assertRaises(FileExistsError):
            await self.assembler.assemble(self.path, "child:parent")
        (self.workspace.root / "experiments.json").write_text('{"bad":"../outside"}')
        with self.assertRaises((ValueError, FileNotFoundError)):
            find_experiment(self.workspace.root, "bad")

    async def test_template_and_registry_write_failures_preserve_existing_instance(
        self,
    ):
        """A7: failed publication removes the new build and retains existing files."""
        state = await self.assembler.assemble(self.path, "existing")
        original = Path.write_text
        for boundary in ("experiment.yaml", ".experiments-"):

            def fail(path, *args, boundary=boundary, **kwargs):
                if path != self.path and (
                    path.name == boundary or path.name.startswith(boundary)
                ):
                    raise OSError("publication failed")
                return original(path, *args, **kwargs)

            with (
                self.subTest(boundary=boundary),
                patch.object(Path, "write_text", fail),
                self.assertRaisesRegex(OSError, "publication failed"),
            ):
                await self.assembler.assemble(self.path, "failed")
            self.assertEqual(
                list((self.workspace.root / "experiments").iterdir()),
                [state.experiment_directory],
            )
            self.assertEqual(
                list(
                    json.loads((self.workspace.root / "experiments.json").read_text())
                ),
                ["existing"],
            )

    async def test_cancelled_copy_finishes_before_removing_its_build(self):
        """A7: a worker must not write into a build after cancellation cleanup."""
        async with asyncio.timeout(30):
            entered, release, finished = (
                threading.Event(),
                threading.Event(),
                threading.Event(),
            )
            original = shutil.copytree

            def controlled_copy(source, target, *args, **kwargs):
                entered.set()
                release.wait(5)
                try:
                    return original(source, target, *args, **kwargs)
                finally:
                    finished.set()

            with patch("core.experimentassembler.shutil.copytree", controlled_copy):
                task = asyncio.create_task(
                    self.assembler.assemble(self.path, "cancelled")
                )
                await wait_until(entered.is_set)
                task.cancel()
                try:
                    await asyncio.wait({task}, timeout=0.1)
                finally:
                    release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                await wait_until(finished.is_set)
            self.assertEqual(list((self.workspace.root / "experiments").iterdir()), [])
            self.assertFalse((self.workspace.root / "experiments.json").exists())
