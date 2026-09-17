"""Approved groups A-K: real 30-second weather service in a four-step DAG.

All scenarios keep the real 40-second first step. No clock or TCP substitutes.
The object storage fixture serves files over loopback HTTP; journals use SQLite.
"""

import asyncio
import shutil
import subprocess
import unittest
from pathlib import Path
from uuid import uuid4

import yaml

from core.runner_utils.experimentrunner import ExperimentRunner
from tests.helpers.archives import ArchiveWorkspace, inventory
from tests.helpers.dag import REPOSITORY, process_running, terminate_owned, wait_until


class WeatherDagTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ArchiveWorkspace()
        self.addAsyncCleanup(self.w.close)
        self.template = self.w.template()
        references = {}
        for role in ("stage", "service"):
            name = f"weather_{role}"
            directory = self.w.source / "modules" / name / "1"
            directory.mkdir(parents=True)
            shutil.copy2(
                Path(__file__).parent / "helpers" / f"weather_{role}.py",
                directory / "main.py",
            )
            metadata = {
                "schema_version": 2,
                "name": name,
                "version": "1",
                "role": role,
                "implementation": "full",
                "commands": {"start": ["python", "-B", "main.py"]},
                "defaults": {"startup_only": 1} if role == "service" else {},
            }
            (directory / "module.yaml").write_text(
                yaml.safe_dump(metadata), encoding="utf-8"
            )
            self.w.hashes.add_module_hash(
                name, "1", self.w.manager.module_hash(name, directory)
            )
            references[role] = {
                "name": name,
                "version": "1",
                "hash": self.w.hashes.get_module_hash(name, "1"),
            }
        self.sid = str(uuid4())
        policy = {"retries": 0, "retry_delay_seconds": 0.1, "on_exhausted": "pause"}
        self.service = {
            "service_id": self.sid,
            "module": references["service"],
            "settings": {},
            "heartbeat": {"interval_seconds": 1, "grace_seconds": 5},
            "command_timeout_seconds": 10,
            "on_command_timeout": "pause",
            "state_required": True,
            "errors": dict(policy),
        }
        self.template["services"] = [self.service]
        self.template["stages"] = []
        for operation in ("wait", "service", "format", "write"):
            node = {
                "stage_id": str(uuid4()),
                "settings": {} if operation == "service" else {"operation": operation},
                "timeout_seconds": 60,
                "errors": dict(policy),
            }
            node.update(
                {"service_id": self.sid}
                if operation == "service"
                else {"module": references["stage"]}
            )
            self.template["stages"].append(node)
        self.path = self.w.source / "template.yaml"
        self.code_before = inventory(self.w.source / "modules")
        self.runner = self.new_runner()

    def new_runner(self):
        runner = ExperimentRunner(
            self.w.source, self.w.manager, archive_config_path=self.w.config
        )
        self.w.runners.append(runner)
        return runner

    async def launch(self):
        self.path.write_text(yaml.safe_dump(self.template), encoding="utf-8")
        await self.runner.run(self.path, delayed_start=True)
        await asyncio.wait_for(self.runner._ready.wait(), 30)
        self.assertEqual(
            self.runner.get_state()["phase"], "waiting", self.runner.get_state()
        )
        return self.runner._state.services[self.sid]

    def events(self):
        events, cursor = [], None
        while True:
            page = self.runner._journal.client.read_events(cursor, limit=1000)
            events.extend(row["event"] for row in page["events"])
            if not page["has_more"]:
                return events
            cursor = page["checkpoint"]

    async def finish(self):
        await self.runner.resume()
        await wait_until(
            lambda: self.runner.get_state()["phase"] in ("completed", "failed"),
            timeout=100,
        )
        self.assertEqual(
            self.runner.get_state()["phase"], "completed", self.runner.get_state()
        )
        state = self.runner._state
        record = self.runner._journal.client.read_command_result(state.last_result_id)
        result = record["response"]["data"]
        self.assertEqual(
            (state.experiment_directory / result["path"]).read_text(encoding="utf-8"),
            result["text"],
        )
        self.assertIn("Новосибирск:", result["text"])
        self.assertIn("°C", result["text"])
        self.assertGreaterEqual(result["forecast"]["sequence"], 2)
        self.assertEqual(inventory(self.w.source / "modules"), self.code_before)
        self.assertEqual(
            list(state.experiment_directory.rglob("execution_result.json")), []
        )
        self.assertTrue(all(instance.stopped for instance in state.services.values()))
        self.assertTrue(
            all(
                not process_running(instance.process_identity["pid"])
                for instance in state.services.values()
            )
        )
        self.assertEqual(len(state.stage_result_ids), 4)
        return result

    async def test_complete_real_timing_dataflow_and_journal(self):
        """J/K23: 40 real seconds include the service's next 30-second forecast."""
        await self.launch()
        await self.finish()
        state = self.runner._state
        first_id = state.stage_result_ids[self.template["stages"][0]["stage_id"]]
        first = self.runner._journal.client.read_command_result(first_id)
        self.assertGreaterEqual(first["response"]["data"]["waited_seconds"], 40)
        generated = [
            event["data"]
            for event in self.events()
            if event["event_type"] == "weather.generated"
        ]
        self.assertGreaterEqual(
            generated[1]["generated_monotonic"] - generated[0]["generated_monotonic"],
            30,
        )
        calls = [
            event["data"]
            for event in self.events()
            if event["event_type"] == "weather.request"
        ]
        self.assertEqual([call["settings"] for call in calls], [{}])
        self.assertTrue(
            any(event["event_type"] == "artifact.recorded" for event in self.events())
        )

    async def test_pause_step_and_resume_keep_same_service(self):
        """E14: pause finishes the current wait; step calls the existing service."""
        instance = await self.launch()
        await self.runner.resume()
        await wait_until(lambda: self.runner.get_state()["phase"] == "stage_running")
        await asyncio.wait_for(self.runner.pause(), 70)
        self.assertEqual(self.runner.get_state()["mode"], "paused")
        self.assertTrue(process_running(instance.process_identity["pid"]))
        self.assertEqual((await self.runner.step())["result"]["result"], "success")
        self.assertIs(self.runner._state.services[self.sid], instance)
        await self.finish()

    async def test_retry_service_request_without_process_restart(self):
        """D/E12-14: a failed response gets a new call ID, retaining the service."""
        node = self.template["stages"][1]
        node["settings"] = {"fail_first": True}
        node["errors"]["retries"] = 1
        instance = await self.launch()
        await self.finish()
        calls = [
            event["data"]
            for event in self.events()
            if event["event_type"] == "weather.request"
        ]
        self.assertEqual(len(calls), 2)
        self.assertNotEqual(calls[0]["request_id"], calls[1]["request_id"])
        self.assertEqual(instance.restart_count, 0)

    async def test_snapshot_rollback_and_complete(self):
        """H20: snapshot after forecast acquisition preserves its ledger result."""
        await self.launch()
        self.assertEqual(
            (await asyncio.wait_for(self.runner.step(), 70))["result"]["result"],
            "success",
        )
        self.assertEqual((await self.runner.step())["result"]["result"], "success")
        snapshot = await self.runner.snapshot("weather acquired")
        retained = self.runner._state.last_result_id
        self.assertEqual((await self.runner.step())["result"]["result"], "success")
        await self.runner.rollback(snapshot["snapshot_id"])
        self.assertEqual(self.runner._state.last_result_id, retained)
        await self.finish()

    async def test_actual_runner_crash_recovers_live_wait_without_replay(self):
        """E/H19: an independent runner dies while its executor and service survive."""
        instance = await self.launch()
        experiment_id = self.runner._state.experiment_id
        await self.runner.close()
        output = (self.w.root / "weather-owner.log").open("wb")
        self.w.child_files.append(output)
        owner = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--project",
            str(REPOSITORY),
            "--no-sync",
            "python",
            "-B",
            "-m",
            "tests.helpers.snapshot_owner",
            "--root",
            str(self.w.source),
            "--experiment",
            experiment_id,
            "--operation",
            "stage",
            "--phase",
            "stage_running",
            "--action",
            "crash",
            cwd=REPOSITORY,
            stdout=output,
            stderr=output,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.w.children.append(owner)
        self.assertEqual(
            await asyncio.wait_for(owner.wait(), 40),
            23,
            (self.w.root / "weather-owner.log").read_text(encoding="utf-8"),
        )
        self.assertTrue(process_running(instance.process_identity["pid"]))
        self.runner = self.new_runner()
        await self.runner.recover(experiment_id)
        await wait_until(lambda: self.runner._state.active_attempt is None, timeout=70)
        await self.finish()
        self.assertEqual(
            self.runner._state.stage_attempt_numbers[
                self.template["stages"][0]["stage_id"]
            ],
            1,
        )

    async def test_stop_while_waiting_terminates_module_and_service(self):
        """B/D11: stop interrupts the first step and leaves no output or live service."""
        instance = await self.launch()
        await self.runner.resume()
        await wait_until(
            lambda: (
                self.runner._state.active_attempt is not None
                and self.runner._state.active_attempt.process_identity is not None
            ),
            timeout=15,
        )
        attempt = self.runner._state.active_attempt
        await self.runner.stop()
        self.assertFalse(process_running(instance.process_identity["pid"]))
        self.assertFalse(process_running(attempt.process_identity["pid"]))
        self.assertEqual(
            list(self.runner._state.experiment_directory.rglob("weather.txt")), []
        )
        self.assertTrue(self.runner.get_state()["termination_confirmed"])

    async def test_runner_crash_during_service_call_recovers_without_replay(self):
        """E/H19: preserve the sent service call across actual owner process loss."""
        self.template["stages"][1]["settings"] = {"delay_first": 12}
        instance = await self.launch()
        await asyncio.wait_for(self.runner.step(), 70)
        experiment_id = self.runner._state.experiment_id
        await self.runner.close()
        output = (self.w.root / "weather-call-owner.log").open("wb")
        self.w.child_files.append(output)
        owner = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--project",
            str(REPOSITORY),
            "--no-sync",
            "python",
            "-B",
            "-m",
            "tests.helpers.snapshot_owner",
            "--root",
            str(self.w.source),
            "--experiment",
            experiment_id,
            "--operation",
            "stage",
            "--phase",
            "service_work",
            "--action",
            "crash",
            cwd=REPOSITORY,
            stdout=output,
            stderr=output,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.w.children.append(owner)
        self.assertEqual(
            await asyncio.wait_for(owner.wait(), 30),
            23,
            (self.w.root / "weather-call-owner.log").read_text(encoding="utf-8"),
        )
        self.runner = self.new_runner()
        await self.runner.recover(experiment_id)
        await wait_until(lambda: self.runner._state.active_attempt is None, timeout=30)
        self.assertEqual(
            self.runner._state.services[self.sid].service_instance_id,
            instance.service_instance_id,
        )
        await self.finish()
        calls = [
            event for event in self.events() if event["event_type"] == "weather.request"
        ]
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            self.runner._state.stage_attempt_numbers[
                self.template["stages"][1]["stage_id"]
            ],
            1,
        )

    async def test_timeout_late_response_and_retry_keep_original_service(self):
        """D/E13: timeout remains accepted even after the service finishes that call."""
        node = self.template["stages"][1]
        node["settings"] = {"delay_first": 3}
        node["timeout_seconds"] = 2
        node["errors"]["retries"] = 1
        instance = await self.launch()
        await self.finish()
        calls = [
            event["data"]
            for event in self.events()
            if event["event_type"] == "weather.request"
        ]
        self.assertEqual(len(calls), 2)
        first = self.runner._journal.client.read_command_result(calls[0]["request_id"])
        self.assertEqual(first["outcome"], "timed_out")
        self.assertEqual(first["author"], "runner")
        self.assertEqual(
            {item["author"] for item in first["observations"]},
            {"runner", "participant"},
        )
        self.assertEqual(instance.restart_count, 0)

    async def test_service_crash_restarts_under_manager_and_dag_completes(self):
        """D12: actual service process loss is handled by ServiceManager."""
        self.service["errors"]["retries"] = 1
        old = await self.launch()
        terminate_owned(old.process_identity)
        try:
            await wait_until(
                lambda: (
                    self.runner._state.services[self.sid] is not old
                    and (
                        self.runner._state.services[self.sid].ready
                        or self.runner._state.services[self.sid].blocked_action
                    )
                ),
                timeout=35,
            )
        except TimeoutError:
            diagnostics = [str(self.runner.get_state())]
            diagnostics.extend(
                path.read_text(encoding="utf-8")
                for path in self.runner._state.experiment_directory.glob(
                    "shared_artifacts/services/**/stderr.log"
                )
            )
            self.fail("\n".join(diagnostics))
        replacement = self.runner._state.services[self.sid]
        self.assertTrue(
            replacement.ready,
            (replacement.artifacts_directory / "stderr.log").read_text(
                encoding="utf-8"
            ),
        )
        self.assertEqual(replacement.restart_count, 1)
        await self.finish()

    async def test_archive_installs_service_reference_and_runs_four_steps(self):
        """F/H21: transport the mixed DAG via real HTTP object storage and archive."""
        await self.launch()
        await self.runner.stop()
        await self.runner.create_archive(self.w.archive)
        installed = await self.w.importer.install(self.w.archive, self.w.destination)
        self.runner = self.w.target_runner()
        await self.runner.run(Path(installed["template_path"]), delayed_start=True)
        await asyncio.wait_for(self.runner._ready.wait(), 30)
        self.assertEqual(
            self.runner._state.template["stages"][1]["service_id"], self.sid
        )
        self.assertEqual(
            len(
                list(
                    (self.runner._state.experiment_directory / "modules").glob(
                        "*/*/module.yaml"
                    )
                )
            ),
            2,
        )
        await self.finish()
