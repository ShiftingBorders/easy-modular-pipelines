"""Approved RT01-RT08, RT13-RT14: reload identity, progress and real stage input."""

import asyncio
import copy
import os
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

import yaml

from core.runner_utils.runtimeio import read_json
from core.runner_utils.state import state_from_document, state_to_document
from tests.helpers.reload import candidate, events, trace, version, workspace


class TemplateReloadTests(unittest.IsolatedAsyncioTestCase):
    async def make_run(self, *, cycles=2):
        work = workspace(cycles=cycles)
        self.addAsyncCleanup(work.close)
        await work.launch()
        return work

    async def test_noop_preserves_revision_snapshot_cursor_and_audits_rt04(self):
        work = await self.make_run()
        runner = work.runner
        await runner.step()
        await runner.snapshot("existing")
        before = state_to_document(runner._state)
        snapshots = work.manifests()
        for path in (None, runner._state.template_path):
            result = await runner.reload_template(path)
            self.assertFalse(result["changed"])
            self.assertIsNone(result["snapshot_id"])
            self.assertEqual(
                result["template_revision_id"], before["template_revision_id"]
            )
            self.assertEqual(state_to_document(runner._state), before)
        self.assertEqual(work.manifests(), snapshots)
        self.assertEqual(len(events(runner, "reload.unchanged")), 2)
        self.assertEqual(len(events(runner, "template.applied")), 1)

    async def test_default_path_is_experiment_copy_and_snapshot_uses_applied_template_rt01_rt08(
        self,
    ):
        work = await self.make_run()
        runner = work.runner
        old = candidate(work)
        changed = candidate(work)
        changed["stages"][0]["settings"]["echo"] = "experiment-copy"
        work.template_path.write_text("invalid: original source", encoding="utf-8")
        runner._state.template_path.write_text(
            yaml.safe_dump(changed), encoding="utf-8"
        )
        result = await runner.reload_template()
        self.assertEqual(runner._state.template, changed)
        saved = read_json(work.archive(result["snapshot_id"]) / "manifest.json")
        self.assertEqual(saved["state"]["template"], old)
        self.assertEqual(saved["state"]["template"]["snapshots"]["mode"], "off")
        self.assertEqual(len(work.manifests()), 1)
        self.assertEqual(trace(runner), [])

    async def test_forbidden_top_fields_and_invalid_documents_leave_applied_state_rt02(
        self,
    ):
        work = await self.make_run()
        runner = work.runner
        before = state_to_document(runner._state)
        cases = []
        for field, value in {
            "name": "other",
            "cycles": 3,
            "keep_attempts": 2,
            "schema_version": 3,
            "start_timeout": 40,
            "runner_timeout_margin_seconds": 4,
            "resources": [
                {"name": "data", "path": str(work.root / "input"), "hash": None}
            ],
            "unknown_state": {
                **before["template"]["unknown_state"],
                "recovery_limit": 4,
            },
            "snapshots": {"mode": "after_stage", "keep": 8},
            "storage": {"min_snapshot_free_bytes": 1},
            "logging": {**before["template"]["logging"], "busy_timeout_seconds": 4},
            "stages": [],
            "unexpected": 1,
        }.items():
            document = candidate(work)
            document[field] = value
            cases.append((field, yaml.safe_dump(document)))
        missing = candidate(work)
        del missing["services"]
        cases.extend([("missing", yaml.safe_dump(missing)), ("bad yaml", "stages: [")])
        bad_reference = candidate(work)
        del bad_reference["stages"][0]["module"]
        bad_reference["stages"][0]["service_id"] = str(uuid4())
        cases.append(("unknown service", yaml.safe_dump(bad_reference)))
        for field, value in (
            ("settings", []),
            ("timeout_seconds", -1),
            ("errors", {"retries": -1}),
        ):
            invalid = candidate(work)
            invalid["stages"][0][field] = value
            cases.append((field, yaml.safe_dump(invalid)))
        deep = candidate(work)
        nested = deep["stages"][0]["settings"]
        for _ in range(40):
            nested["child"] = {}
            nested = nested["child"]
        cases.append(("depth", yaml.safe_dump(deep)))
        for label, text in cases:
            with self.subTest(label=label):
                path = work.root / "invalid.yaml"
                path.write_text(text, encoding="utf-8")
                with self.assertRaises((ValueError, TypeError)):
                    await runner.reload_template(path)
                self.assertEqual(state_to_document(runner._state), before)
        with self.assertRaises(FileNotFoundError):
            await runner.reload_template(work.root / "absent.yaml")
        with self.assertRaises(ValueError):
            await runner.reload_template(Path("relative.yaml"))
        self.assertEqual(work.manifests(), [])

    async def test_ids_formatting_typed_settings_and_role_reuse_rt03(self):
        work = await self.make_run()
        runner = work.runner
        document = candidate(work)
        original_ids = [s["stage_id"] for s in document["stages"]]
        path = work.files.write_template(document, "ordered.yaml")
        self.assertFalse((await runner.reload_template(path))["changed"])
        document["stages"][0]["module"]["name"] = (
            " " + document["stages"][0]["module"]["name"] + " "
        )
        self.assertFalse(
            (await runner.reload_template(work.files.write_template(document)))[
                "changed"
            ]
        )
        for value in (True, 1):
            document = candidate(work)
            document["stages"][0]["settings"]["typed"] = value
            self.assertTrue(
                (await runner.reload_template(work.files.write_template(document)))[
                    "changed"
                ]
            )
        document = candidate(work)
        inserted = copy.deepcopy(work.extra_stage)
        del inserted["stage_id"]
        document["stages"].append(inserted)
        await runner.reload_template(work.files.write_template(document))
        ids = [s["stage_id"] for s in runner._state.template["stages"]]
        self.assertEqual(ids[:4], original_ids)
        UUID(ids[-1])
        self.assertEqual(len(set(ids)), 5)
        self.assertFalse((await runner.reload_template())["changed"])
        duplicate = candidate(work)
        duplicate["stages"][-1]["stage_id"] = ids[0]
        with self.assertRaises(ValueError):
            await runner.reload_template(work.files.write_template(duplicate))
        invalid = candidate(work)
        invalid["stages"][0]["stage_id"] = "not-a-uuid"
        with self.assertRaises(ValueError):
            await runner.reload_template(work.files.write_template(invalid))

    async def test_cursor_matrix_and_actual_next_input_rt06_rt07(self):
        cases = [
            ("change_b", 2, "B", ["A"]),
            ("change_d", 2, "C", ["A", "B"]),
            ("insert_before_b", 2, "X", ["A"]),
            ("insert_before_c", 2, "X", ["A", "B"]),
            ("remove_b", 2, "C", ["A"]),
            ("remove_b_completed_c", 3, "C", ["A"]),
            ("remove_c", 2, "D", ["A", "B"]),
            ("remove_tail", 2, "A", []),
            ("swap_completed", 3, "C", ["A"]),
            ("swap_future", 2, "D", ["A", "B"]),
            ("earlier_move", 3, "A", []),
            ("bypassed_b", 1, "D", []),
        ]
        for label, steps, next_label, input_trail in cases:
            with self.subTest(case=label):
                work = await self.make_run()
                runner = work.runner
                for _ in range(steps):
                    await runner.step()
                if label == "earlier_move":
                    runner.move(1)
                elif label == "bypassed_b":
                    runner.move(4)
                before_trace = trace(runner)
                ids = dict(runner._state.stage_result_ids)
                attempts = dict(runner._state.stage_attempt_numbers)
                document = candidate(work)
                stages = document["stages"]
                if label in ("change_b", "bypassed_b"):
                    stages[1]["settings"]["echo"] = "new"
                elif label == "change_d":
                    stages[3]["timeout_seconds"] = 40
                elif label == "earlier_move":
                    stages[2]["errors"]["retries"] = 1
                elif label.startswith("insert"):
                    stages.insert(
                        1 if label.endswith("b") else 2, copy.deepcopy(work.extra_stage)
                    )
                elif label.startswith("remove_b"):
                    stages.pop(1)
                elif label == "remove_c":
                    stages.pop(2)
                elif label == "remove_tail":
                    del stages[2:]
                elif label == "swap_completed":
                    stages[1], stages[2] = stages[2], stages[1]
                else:
                    stages[2], stages[3] = stages[3], stages[2]
                result = await runner.reload_template(
                    work.files.write_template(document)
                )
                self.assertEqual(trace(runner), before_trace)
                self.assertEqual(runner._state.mode, "paused")
                self.assertEqual(runner._state.stage_attempt_numbers, attempts)
                self.assertIsNone(runner._state.pending_rebuild)
                self.assertEqual(len(work.manifests()), 1)
                for request_id in ids.values():
                    self.assertEqual(
                        runner._journal.client.read_command_result(request_id)[
                            "outcome"
                        ],
                        "succeeded",
                    )
                await runner.step()
                latest = trace(runner)[-1]
                self.assertEqual(latest["label"], next_label, result)
                actual_input = latest["input"]
                self.assertEqual(
                    [] if actual_input is None else actual_input["trail"], input_trail
                )
                self.assertEqual(latest["cycle"], 2 if label == "remove_tail" else 1)
                await work.close()

    async def test_registered_new_module_and_invalid_hash_are_checked_before_mutation_rt05(
        self,
    ):
        work = workspace()
        self.addAsyncCleanup(work.close)
        second = version(work, work.stages[1], "2")
        runner = await work.launch()
        before = candidate(work)
        for reference in (
            {**second["module"], "hash": "0" * 64},
            {**second["module"], "version": "missing"},
        ):
            document = candidate(work)
            document["stages"][1]["module"] = reference
            with self.assertRaises((ValueError, FileNotFoundError, NotADirectoryError)):
                await runner.reload_template(work.files.write_template(document))
            self.assertEqual(runner._state.template, before)
            self.assertEqual(work.manifests(), [])
        document = candidate(work)
        document["stages"][1] = second
        await runner.reload_template(work.files.write_template(document))
        root = runner._state.experiment_directory
        self.assertTrue((root / "modules" / second["module"]["name"] / "1").is_dir())
        self.assertTrue((root / "modules" / second["module"]["name"] / "2").is_dir())
        self.assertEqual(
            read_json(work.root / "experiments.json"),
            {runner._state.experiment_id: root.name},
        )

    async def test_paths_resolve_from_candidate_file_not_cwd_rt02(self):
        work = workspace()
        self.addAsyncCleanup(work.close)
        (work.root / "input.txt").write_text("resource", encoding="utf-8")
        work.template["resources"] = [
            {"name": "data", "path": "input.txt", "hash": None}
        ]
        runner = await work.launch()
        document = candidate(work)
        document["resources"][0]["path"] = "../input.txt"
        path = work.files.write_template(document, "sub/template.yaml")
        previous = Path.cwd()
        try:
            os.chdir(work.root)
            self.assertFalse((await runner.reload_template(path))["changed"])
        finally:
            os.chdir(previous)
        document["resources"][0]["path"] = "input.txt"
        with self.assertRaises(ValueError):
            await runner.reload_template(
                work.files.write_template(document, "sub/template.yaml")
            )

    async def test_field_audit_and_commit_identities_rt13_rt14(self):
        work = await self.make_run()
        runner = work.runner
        old = candidate(work)
        old_revision, old_run = runner._state.template_revision_id, runner._state.run_id
        document = candidate(work)
        document["stages"][0]["settings"].update(nested={"present": None}, typed=True)
        runner._command_context = {
            "command_id": str(uuid4()),
            "command_chain_id": str(uuid4()),
        }
        result = await runner.reload_template(work.files.write_template(document))
        changes = events(runner, "reload.definition_changed")
        field = next(
            f
            for f in changes[-1]["data"]["fields"]
            if f["path"] == ["settings", "nested"]
        )
        self.assertFalse(field["before_present"])
        self.assertEqual(field["after"], {"present": None})
        self.assertEqual(
            changes[-1]["context"]["command_id"], runner._command_context["command_id"]
        )
        self.assertEqual(
            events(runner, "reload.previous_template")[-1]["data"]["template"], old
        )
        self.assertEqual(
            events(runner, "reload.candidate")[-1]["data"]["template"], document
        )
        applied = events(runner, "template.applied")[-1]
        self.assertEqual(applied["data"]["previous_template_revision_id"], old_revision)
        self.assertEqual(applied["context"]["previous_run_id"], old_run)
        self.assertEqual(
            applied["data"]["template_revision_id"], result["template_revision_id"]
        )
        self.assertEqual(
            yaml.safe_load(runner._state.template_path.read_text(encoding="utf-8")),
            document,
        )
        self.assertIsNone(
            events(runner, "runner.checkpoint")[-1]["data"]["pending_rebuild"]
        )

    async def test_pending_rebuild_validation_rt18(self):
        work = await self.make_run()
        root = work.runner._state.experiment_directory
        original = state_to_document(work.runner._state)
        pending = {
            "operation_id": str(uuid4()),
            "snapshot_id": str(uuid4()),
            "template_revision_id": str(uuid4()),
            "run_id": str(uuid4()),
        }
        valid = {
            **original,
            "stable_snapshot_id": pending["snapshot_id"],
            "pending_rebuild": pending,
        }
        self.assertEqual(state_from_document(root, valid).pending_rebuild, pending)
        for bad in (
            {},
            {**pending, "extra": True},
            {**pending, "snapshot_id": "bad"},
            {**pending, "snapshot_id": str(uuid4())},
            {**pending, "run_id": 1},
        ):
            with self.subTest(bad=bad), self.assertRaises((ValueError, TypeError)):
                state_from_document(root, {**valid, "pending_rebuild": bad})

    async def test_rejects_busy_and_terminal_boundaries_rt01(self):
        work = await self.make_run()
        runner = work.runner
        for field, value in (("_maintenance", True), ("_service_retrying", True)):
            with (
                self.subTest(field=field),
                patch.object(runner, field, value),
                self.assertRaises(RuntimeError),
            ):
                await runner.reload_template()
        gate = work.files.gate()
        document = candidate(work)
        document["stages"][0]["settings"]["gate"] = str(gate)
        await runner.reload_template(work.files.write_template(document))
        task = asyncio.create_task(runner.step())
        self.addAsyncCleanup(asyncio.gather, task, return_exceptions=True)
        self.addCleanup(gate.touch)
        from tests.helpers.services import wait_for

        await wait_for(lambda: runner._state.active_attempt is not None)
        with self.assertRaises(RuntimeError):
            await runner.reload_template()

        gate.touch()
        await task
        await runner.stop()
        with self.assertRaises(RuntimeError):
            await runner.reload_template()

    async def test_skipped_progress_survives_reload_and_continuation_rt06_rt07(self):
        for continuation in (False, True):
            with self.subTest(continuation=continuation):
                work = workspace()
                self.addAsyncCleanup(work.close)
                work.template["stages"][1]["settings"]["mode"] = "fail"
                work.template["stages"][1]["errors"]["on_exhausted"] = "skip"
                runner = await work.launch()
                await runner.step()
                await runner.step()
                self.assertTrue(runner._pending_advance)
                source = runner._state.experiment_id
                if continuation:
                    await runner.snapshot("accepted skip")
                    await runner.close()
                    runner = work.replacement()
                    await runner.run(experiment_id=source, continue_run=True)
                    await runner._ready.wait()
                else:
                    future_only = candidate(work)
                    future_only["stages"][3]["settings"]["echo"] = "future"
                    await runner.reload_template(work.files.write_template(future_only))
                document = candidate(work)
                document["stages"][1]["settings"]["mode"] = "success"
                await runner.reload_template(work.files.write_template(document))
                self.assertEqual(runner._state.stage_position, 2)
                self.assertFalse(runner._pending_advance)
                await runner.step()
                self.assertEqual(trace(runner)[-1]["label"], "B")
                self.assertEqual(trace(runner)[-1]["input"]["trail"], ["A"])
                if continuation:
                    self.assertEqual(
                        runner._state.stage_result_origins[work.stages[0]["stage_id"]],
                        source,
                    )
                await work.close()

    async def test_retry_counters_attempts_and_final_cycle_boundary_rt06_rt07(self):
        work = workspace(cycles=1)
        self.addAsyncCleanup(work.close)
        work.template["stages"][1]["settings"]["failures"] = 1
        work.template["stages"][1]["errors"].update(retries=1, retry_delay_seconds=0)
        runner = await work.launch()
        await runner.step()
        await runner.step()
        sid = work.stages[1]["stage_id"]
        self.assertEqual(runner._state.stage_retry_counts[sid], 1)
        self.assertEqual(runner._state.stage_attempt_numbers[sid], 2)
        old_requests = set(runner._state.used_request_ids)
        old_result = runner._state.stage_result_ids[sid]
        document = candidate(work)
        document["stages"][1]["settings"]["echo"] = "rerun"
        await runner.reload_template(work.files.write_template(document))
        self.assertNotIn(sid, runner._state.stage_retry_counts)
        self.assertEqual(runner._state.stage_attempt_numbers[sid], 2)
        await runner.step()
        self.assertEqual(trace(runner)[-1]["attempt"], 3)
        self.assertNotIn(runner._state.stage_result_ids[sid], old_requests)
        self.assertEqual(
            runner._journal.client.read_command_result(old_result)["outcome"],
            "succeeded",
        )
        document = candidate(work)
        del document["stages"][2:]
        await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner.get_state()["phase"], "waiting")
        starts = len(trace(runner))
        await runner.resume()
        await asyncio.wait_for(runner._task, 30)
        self.assertEqual(runner.get_state()["phase"], "completed")
        self.assertEqual(len(trace(runner)), starts)

    async def test_failed_attempt_is_not_accepted_progress_and_manual_rerun_keeps_boundary_rt06(
        self,
    ):
        work = workspace()
        self.addAsyncCleanup(work.close)
        work.template["stages"][1]["settings"]["mode"] = "fail"
        runner = await work.launch()
        await runner.step()
        with self.assertRaises(RuntimeError):
            await runner.step()
        self.assertEqual(runner._state.stage_position, 2)
        self.assertFalse(runner._pending_advance)
        document = candidate(work)
        document["stages"][1]["settings"]["mode"] = "success"
        await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner._state.stage_position, 2)
        await runner.rerun("stage", position=2)
        document = candidate(work)
        document["stages"][3]["errors"]["retries"] = 1
        await runner.reload_template(work.files.write_template(document))
        await runner.step()
        self.assertEqual(trace(runner)[-1]["label"], "C")
        self.assertEqual(trace(runner)[-1]["input"]["trail"], ["A", "B"])

    async def test_integrity_failure_keeps_old_template_and_files_rt05_rt08(self):
        work = workspace()
        self.addAsyncCleanup(work.close)
        second = version(work, work.stages[0], "2")
        runner = await work.launch()
        before = candidate(work)
        module = work.root / "modules" / second["module"]["name"] / "2"
        (module / "main.py").write_text("corrupt", encoding="utf-8")
        document = candidate(work)
        document["stages"][0]["module"] = second["module"]
        with self.assertRaisesRegex(ValueError, "integrity"):
            await runner.reload_template(work.files.write_template(document))
        self.assertEqual(runner._state.template, before)
        self.assertFalse(
            (
                runner._state.experiment_directory
                / "modules"
                / second["module"]["name"]
                / "2"
            ).exists()
        )
        self.assertEqual(work.manifests(), [])

    async def test_linked_module_is_rejected_before_snapshot_rt05(self):
        work = workspace()
        self.addAsyncCleanup(work.close)
        linked = (
            work.root
            / "modules"
            / work.extra_stage["module"]["name"]
            / "1"
            / "linked.txt"
        )
        source = work.root / "outside.txt"
        source.write_text("outside module", encoding="utf-8")
        try:
            linked.symlink_to(source)
        except OSError as error:
            self.skipTest(f"File symlinks are unavailable: {error}")
        runner = await work.launch()
        document = candidate(work)
        document["stages"].append(copy.deepcopy(work.extra_stage))
        with self.assertRaisesRegex(ValueError, "links"):
            await runner.reload_template(work.files.write_template(document))
        self.assertEqual(work.manifests(), [])

    async def test_continuation_uses_protective_old_snapshot_until_new_snapshot_rt08_rt20(
        self,
    ):
        work = await self.make_run()
        runner = work.runner
        old = candidate(work)
        changed = candidate(work)
        changed["stages"][0]["settings"]["echo"] = "new revision"
        source = runner._state.experiment_id
        result = await runner.reload_template(work.files.write_template(changed))
        self.assertEqual(len(work.manifests()), 1)
        await runner.close()
        continuation = work.replacement()
        await continuation.run(experiment_id=source, continue_run=True)
        await continuation._ready.wait()
        self.assertEqual(continuation._state.template, old)
        await continuation.close()
        recovered = work.replacement()
        await recovered.recover(source)
        self.assertEqual(recovered._state.template, changed)
        self.assertEqual(
            recovered._state.template_revision_id, result["template_revision_id"]
        )
        snapshot = await recovered.snapshot("after reload")
        await recovered.close()
        continuation = work.replacement()
        await continuation.run(experiment_id=source, continue_run=True)
        await continuation._ready.wait()
        self.assertEqual(continuation._state.template, changed)
        self.assertEqual(
            continuation._state.stable_snapshot_id, snapshot["snapshot_id"]
        )
