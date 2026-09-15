"""Approved basic_dag.md D3-D7: runtime integration with the real shared journal."""

import asyncio
import json
import unittest
from datetime import datetime
from unittest.mock import patch

from core.logger_utils.events import LoggingStorageError
from tests.helpers.dag import DagSession, DagWorkspace, process_running, wait_until


class RuntimeJournalTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.session = DagSession(self.workspace)
        await self.session.start()
        self.addAsyncCleanup(self.session.close)

    async def test_three_process_roles_write_one_journal_and_reads_require_selected_id(
        self,
    ):
        """D4/D5: actual writers share identity/generation and expose finished state."""
        async with asyncio.timeout(30):
            launched = await self.session.launch(self.workspace.template())
            reply = await self.session.send("step")
            self.assertEqual(reply["result"], "success", reply)
            state = self.session.runner.get_state()
            self.assertTrue(state["executor"]["finished"])
            self.assertIsNone(state["executor"]["current"])
            self.assertEqual(state["source"], "runner")
            self.assertIsNotNone(datetime.fromisoformat(state["observed_at"]).tzinfo)
            records = self.workspace.events(self.session.runner)
            # Resource sampling is periodic, so this short stage may also have a
            # fourth writer. The three execution roles must retain separate clients.
            execution_records = [
                row
                for row in records
                if row["context"]["source"] != "resource_collector"
            ]
            self.assertEqual(
                {row["context"]["source"] for row in execution_records},
                {"runner", "executor", "module"},
            )
            execution_pids = {row["context"]["process_id"] for row in execution_records}
            self.assertEqual(len(execution_pids), 3)
            for row in records:
                if row["context"]["source"] == "resource_collector":
                    self.assertNotIn(row["context"]["process_id"], execution_pids)
            root = self.session.runner._state.experiment_directory
            configs = [
                json.loads(path.read_text())
                for path in (root / "runner/logging").glob("*.json")
            ]
            self.assertEqual(
                {item["logging"]["db_path"] for item in configs},
                {str(root / "journals/events.sqlite")},
            )
            identities = [
                item["logging"]["expected_journal"]
                for item in configs
                if item["logging"]["open_mode"] == "existing"
            ]
            self.assertTrue(identities)
            self.assertTrue(all(value == identities[0] for value in identities))
            self.assertEqual(
                (
                    await self.session.send(
                        "logs.read", {"experiment_id": launched["experiment_id"]}
                    )
                )["result"],
                "success",
            )
            for args in ({}, {"experiment_id": "someone-else"}):
                self.assertEqual(
                    (await self.session.send("logs.read", args))["result"], "fail"
                )

    async def test_state_write_failure_is_logged_and_execution_continues(self):
        """D3: state.json write failures do not have mandatory-journal semantics."""
        async with asyncio.timeout(30):
            await self.session.launch(self.workspace.template())
            with patch.object(
                self.session.runner._state_store,
                "save",
                side_effect=OSError("state disk failure"),
            ):
                result = await self.session.send("step")
            self.assertEqual(result["result"], "success", result)
            self.assertEqual(self.session.runner.get_state()["phase"], "completed")
            errors = [
                event
                for event in self.workspace.events(self.session.runner)
                if event["event_type"] == "error.recorded"
            ]
            self.assertTrue(errors)
            self.assertIn("state disk failure", json.dumps(errors))

    async def test_mandatory_journal_failure_stops_active_stage(self):
        """D3/D7: a command-side journal failure must stop actual owned work."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            await self.session.launch(
                self.workspace.template(
                    [self.workspace.stage(settings={"gate": str(gate)})]
                ),
                paused=False,
            )
            _, ready = await self.session.ready_attempt()
            failure = LoggingStorageError("required journal unavailable")
            with patch.object(
                self.session.runner._journal.client, "record_event", side_effect=failure
            ):
                reply = await self.session.send("pause")
                self.assertEqual(reply["result"], "fail")
                await wait_until(
                    lambda: self.session.runner.get_state()["phase"] == "failed",
                    timeout=4,
                )
                self.assertFalse(process_running(ready["pid"]))
            self.assertIn(
                "required journal unavailable",
                self.session.runner.get_state()["error"]["message"],
            )

    async def test_original_error_survives_secondary_logging_failure(self):
        """D7: structured emergency diagnostics preserve the original failure."""
        async with asyncio.timeout(30):
            await self.session.launch(self.workspace.template())
            with patch.object(
                self.session.runner._journal.client,
                "record_error",
                side_effect=LoggingStorageError("secondary logger failure"),
            ):
                await self.session.runner._fail(OSError("original failure"), {})
            files = list((self.workspace.root / "controller").glob("failure-*.json"))
            self.assertEqual(len(files), 1)
            record = json.loads(files[0].read_text())
            self.assertEqual(record["message"], "original failure")
            self.assertEqual(record["logging_error"], "secondary logger failure")

    async def test_keep_attempts_retains_other_stages_epochs_and_journal_history(self):
        """D6: only old attempts of the pointer stage/current cycle are removed."""
        async with asyncio.timeout(30):
            for keep in (1, 2):
                with self.subTest(keep=keep):
                    module = self.workspace.module(f"worker-{keep}")
                    stages = [self.workspace.stage(module) for _ in range(3)]
                    await self.session.launch(
                        self.workspace.template(stages, cycles=2, keep_attempts=keep)
                    )
                    self.assertEqual(
                        (await self.session.send("step"))["result"], "success"
                    )
                    for _ in range(2):
                        self.assertEqual(
                            (
                                await self.session.send(
                                    "rerun", {"scope": "stage", "position": 1}
                                )
                            )["result"],
                            "success",
                        )
                    root = self.session.runner._state.experiment_directory
                    first = (
                        root
                        / "shared_artifacts/epoch_1"
                        / module["name"]
                        / stages[0]["stage_id"]
                    )
                    names = sorted(path.name for path in first.iterdir())
                    self.assertEqual(
                        names, [f"attempt_{number}" for number in range(4 - keep, 4)]
                    )
                    for _ in range(3):
                        self.assertEqual(
                            (await self.session.send("step"))["result"], "success"
                        )
                    self.assertEqual(self.session.runner.get_state()["cycle_number"], 2)
                    self.assertEqual(
                        sorted(path.name for path in first.iterdir()), names
                    )
                    other = (
                        root
                        / "shared_artifacts/epoch_1"
                        / module["name"]
                        / stages[1]["stage_id"]
                        / "attempt_1/execution_result.json"
                    )
                    self.assertTrue(other.is_file())
                    parameters = [
                        event
                        for event in self.workspace.events(self.session.runner)
                        if event["event_type"] == "attempt.parameters"
                    ]
                    self.assertEqual(len(parameters), 6)
                    self.assertTrue(
                        all(
                            {"template_yaml", "template", "effective_settings"}
                            <= event["data"].keys()
                            for event in parameters
                        )
                    )
                    await self.session.send("stop")
