"""Real process/HTTP/TCP/SQLite weather DAGs with controlled functional waits.

The timing smoke retains the real 40-second stage and 30-second service timer.
Other cases release owned gates after observing the operation under test.
"""

import asyncio
import json
import shutil
import subprocess
import time
import traceback
import unittest
from collections import deque
from pathlib import Path
from uuid import uuid4

import psutil
import yaml

from core.experiments.runner import ExperimentRunner
from core.experiments.state import _process_identity_document, _process_identity_pid
from core.primitives.json_files import read_json
from core.primitives.processes import process_identity
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
            "settings": {"controlled_forecast": True},
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
        self.template["stages"][0]["settings"]["controlled_wait"] = True
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

    async def _wait_for_wait(self):
        """Observe module entry, not merely creation of its executor process."""

        def entered():
            attempt = self.runner._state.active_attempt
            if (
                attempt is not None
                and attempt.stage_id == self.template["stages"][0]["stage_id"]
                and attempt.process_identity is not None
                and (attempt.artifacts_directory / "wait-entered").is_file()
            ):
                return attempt
            return None

        attempt = await wait_until(entered, timeout=30)
        self.w.gates.append(attempt.artifacts_directory / "release-wait")
        return attempt

    async def _release_wait(self):
        """Generate the second forecast in the live service before releasing work."""
        attempt = await self._wait_for_wait()
        instance = self.runner._state.services[self.sid]
        (instance.artifacts_directory / "generate-next").touch()
        await wait_until(
            (instance.artifacts_directory / "next-generated").is_file, timeout=30
        )
        self.assertIs(self.runner._state.active_attempt, attempt)
        self.assertFalse((attempt.artifacts_directory / "wait-finished").exists())
        (attempt.artifacts_directory / "release-wait").touch()

    async def _step_wait(self):
        step = asyncio.create_task(self.runner.step())
        try:
            await self._release_wait()
            self.assertEqual(
                (await asyncio.wait_for(step, 30))["result"]["result"], "success"
            )
        finally:
            if not step.done():
                step.cancel()
            await asyncio.gather(step, return_exceptions=True)

    async def _start_owned_runner(
        self, log_name: str, experiment_id: str
    ) -> asyncio.subprocess.Process:
        """Start a runner and register its process, output and ownership for cleanup."""
        output = (self.w.root / log_name).open("wb")
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
            # No internal halt: the parent chooses the crash after work enters.
            "--operation",
            "stage",
            "--phase",
            "parent_controlled",
            cwd=REPOSITORY,
            stdout=output,
            stderr=output,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.w.children.append(owner)
        self.w.child_owners.append(self.w.source / "owner-ready.json")
        return owner

    async def _crash_owner_after_entry(self, log_name, entered_path=None):
        """Kill an owned runner only after its dispatched work has entered its gate."""
        state = self.runner._state
        await self.runner.close()
        owner = await self._start_owned_runner(log_name, state.experiment_id)
        await wait_until((self.w.source / "owner-ready.json").is_file, timeout=30)
        attempt = await wait_until(
            lambda: (
                self.runner._state_store.load(state.experiment_directory).active_attempt
            ),
            timeout=30,
        )
        if entered_path is None:
            entered_path = attempt.artifacts_directory / "wait-entered"
            self.w.gates.append(attempt.artifacts_directory / "release-wait")
        await wait_until(entered_path.is_file, timeout=30)
        if attempt.service_id is not None:
            await wait_until(
                lambda: (
                    self.runner._state_store.load(state.experiment_directory)
                    .services[self.sid]
                    .active_request
                ),
                timeout=30,
            )
        saved = self.runner._state_store.load(state.experiment_directory)
        if attempt.service_id is not None:
            self.assertEqual(
                saved.services[self.sid].active_request.request_id, attempt.request_id
            )
            self.assertIsNotNone(saved.services[self.sid].active_request.sent_monotonic)
        expected_process = (
            saved.services[self.sid].process_identity
            if attempt.service_id is not None
            else read_json(attempt.artifacts_directory / "process.json")["stage"]
        )
        self.assertIsNone(owner.returncode)
        self.assertTrue(process_running(_process_identity_pid(expected_process)))
        # Terminate the Python owner identified by its own fixture, not uv's wrapper.
        owner_identity = read_json(self.w.source / "owner-ready.json")["process"]
        terminate_owned(owner_identity)
        self.assertNotEqual(await asyncio.wait_for(owner.wait(), 30), 0)
        return attempt, expected_process

    async def finish(self):
        await self.runner.resume()
        first = self.template["stages"][0]
        if (
            first["settings"].get("controlled_wait")
            and first["stage_id"] not in self.runner._state.stage_result_ids
        ):
            await self._release_wait()
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
                not process_running(_process_identity_pid(instance.process_identity))
                for instance in state.services.values()
            )
        )
        self.assertEqual(len(state.stage_result_ids), 4)
        return result

    async def test_complete_real_timing_dataflow_and_journal(self):
        """J/K23: 40 real seconds include the service's next 30-second forecast."""
        self.service["settings"] = {}
        self.template["stages"][0]["settings"].pop("controlled_wait")
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
        attempt = await self._wait_for_wait()
        pause = asyncio.create_task(self.runner.pause())
        try:
            await wait_until(lambda: self.runner._state.pause_requested, timeout=30)
            self.assertFalse(pause.done())
            self.assertIs(self.runner._state.active_attempt, attempt)
            await self._release_wait()
            await asyncio.wait_for(pause, 30)
        finally:
            if not pause.done():
                pause.cancel()
            await asyncio.gather(pause, return_exceptions=True)
        self.assertEqual(self.runner.get_state()["mode"], "paused")
        self.assertTrue(
            process_running(_process_identity_pid(instance.process_identity))
        )
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
        await self._step_wait()
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
        original, original_process = await self._crash_owner_after_entry(
            "weather-owner.log"
        )
        self.assertTrue(
            process_running(_process_identity_pid(instance.process_identity))
        )
        self.runner = self.new_runner()
        try:
            await self.runner.recover(experiment_id)
            recovered = await self._wait_for_wait()
            self.assertEqual(recovered.attempt_id, original.attempt_id)
            self.assertEqual(
                _process_identity_document(recovered.process_identity),
                _process_identity_document(original_process),
            )
            await self._release_wait()
            await wait_until(
                lambda: self.runner._state.active_attempt is None, timeout=30
            )
            await self.finish()
            self.assertEqual(
                self.runner._state.stage_attempt_numbers[
                    self.template["stages"][0]["stage_id"]
                ],
                1,
            )
        except Exception as failure:
            # Capture before fixture cleanup stops participants and removes the journal.
            destination = (
                REPOSITORY
                / ".artifacts/ci-failures"
                / f"weather-recovery-{uuid4()}.json"
            )
            report = {
                "test": self.id(),
                "error": repr(failure),
                "traceback": traceback.format_exc(),
                "observed_monotonic": time.monotonic(),
                "diagnostic_errors": [],
            }
            try:
                report["state"] = self.runner.get_state()
                state = self.runner._state
                attempt = None if state is None else state.active_attempt
                report["attempt"] = None if attempt is None else vars(attempt).copy()
                report["recovery_count"] = (
                    None if state is None else state.unknown_state_recovery_count
                )
                identities = [instance.process_identity]
                if attempt is not None:
                    identities.append(attempt.process_identity)
                    process_path = attempt.artifacts_directory / "process.json"
                    if process_path.is_file():
                        record = read_json(process_path)
                        report["process_record"] = record
                        identities.extend([record.get("stage"), record.get("executor")])
                report["processes"] = []
                for expected in identities:
                    if expected is None:
                        continue
                    observation = {"expected": _process_identity_document(expected)}
                    try:
                        pid = _process_identity_pid(expected)
                        observation["actual"] = process_identity(pid)
                        process = psutil.Process(pid)
                        observation["status"] = process.status()
                        observation["command"] = process.cmdline()
                    except (OSError, psutil.Error) as error:
                        observation["error"] = repr(error)
                    report["processes"].append(observation)
            except Exception as error:  # noqa: BLE001 - Keep the original test failure primary.
                report["diagnostic_errors"].append(f"state/processes: {error!r}")
            events = deque(maxlen=200)
            try:
                checkpoint = None
                deadline = time.monotonic() + 5
                for _page in range(20):
                    page = self.runner._journal.client.read_events(
                        checkpoint, limit=1000
                    )
                    events.extend(row["event"] for row in page["events"])
                    checkpoint = page["checkpoint"]
                    if not page["has_more"] or time.monotonic() >= deadline:
                        report["journal_scan_complete"] = not page["has_more"]
                        break
                else:
                    report["journal_scan_complete"] = False
            except Exception as error:  # noqa: BLE001 - A broken journal is useful diagnostic evidence.
                report["diagnostic_errors"].append(f"journal: {error!r}")
            report["events"] = list(events)
            try:
                with (self.w.root / "weather-owner.log").open("rb") as stream:
                    stream.seek(0, 2)
                    stream.seek(max(0, stream.tell() - 128 * 1024))
                    report["owner_output_tail"] = stream.read().decode(
                        "utf-8", "replace"
                    )
            except OSError as error:
                report["diagnostic_errors"].append(f"owner output: {error!r}")
            try:
                limit = 10 * 1024 * 1024
                encoded = json.dumps(report, ensure_ascii=True, default=str).encode(
                    "utf-8"
                )
                while len(encoded) > limit and report["events"]:
                    report["events"].pop(0)
                    report["journal_events_trimmed"] = True
                    encoded = json.dumps(report, ensure_ascii=True, default=str).encode(
                        "utf-8"
                    )
                if len(encoded) > limit:
                    encoded = json.dumps(
                        {
                            "test": self.id(),
                            "diagnostic_error": "Report exceeded 10 MiB.",
                        }
                    ).encode("utf-8")
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(encoded)
                failure.add_note(f"Weather recovery diagnostics: {destination}")
            except Exception as error:  # noqa: BLE001 - Reporting must never hide the test failure.
                failure.add_note(
                    f"Could not save weather recovery diagnostics: {error!r}"
                )
            raise

    async def test_stop_while_waiting_terminates_module_and_service(self):
        """B/D11: stop interrupts the first step and leaves no output or live service."""
        instance = await self.launch()
        await self.runner.resume()
        attempt = await self._wait_for_wait()
        await self.runner.stop()
        self.assertFalse(
            process_running(_process_identity_pid(instance.process_identity))
        )
        self.assertFalse(
            process_running(_process_identity_pid(attempt.process_identity))
        )
        self.assertEqual(
            list(self.runner._state.experiment_directory.rglob("weather.txt")), []
        )
        self.assertTrue(self.runner.get_state()["termination_confirmed"])

    async def test_runner_crash_during_service_call_recovers_without_replay(self):
        """E/H19: preserve the sent service call across actual owner process loss."""
        self.template["stages"][1]["settings"] = {"hold_first": True}
        instance = await self.launch()
        await self._step_wait()
        experiment_id = self.runner._state.experiment_id
        release = instance.artifacts_directory / "release-first"
        self.w.gates.append(release)
        original, original_process = await self._crash_owner_after_entry(
            "weather-call-owner.log",
            instance.artifacts_directory / "first-request-entered",
        )
        self.runner = self.new_runner()
        await self.runner.recover(experiment_id)
        self.assertEqual(
            self.runner._state.active_attempt.request_id, original.request_id
        )
        self.assertEqual(
            self.runner._state.services[self.sid].process_identity, original_process
        )
        release.touch()
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
        self.assertEqual(calls[0]["data"]["request_id"], original.request_id)
        self.assertEqual(
            self.runner._state.stage_attempt_numbers[
                self.template["stages"][1]["stage_id"]
            ],
            1,
        )

    async def test_timeout_late_response_and_retry_keep_original_service(self):
        """D/E13: timeout remains accepted even after the service finishes that call."""
        node = self.template["stages"][1]
        node["settings"] = {"hold_first": True}
        node["timeout_seconds"] = 2
        node["errors"]["retries"] = 1
        instance = await self.launch()
        release = instance.artifacts_directory / "release-first"
        entered = instance.artifacts_directory / "first-request-entered"
        self.w.gates.append(release)
        await self._step_wait()
        completion = asyncio.create_task(self.finish())
        try:
            await wait_until(entered.is_file, timeout=30)
            request_id = read_json(entered)["request_id"]
            first = await wait_until(
                lambda: self.runner._journal.client.read_command_result(request_id),
                timeout=30,
            )
            self.assertEqual(first["outcome"], "timed_out")
            self.assertEqual(first["author"], "runner")
            self.assertEqual(
                {item["author"] for item in first["observations"]}, {"runner"}
            )
            release.touch()
            await asyncio.wait_for(completion, 60)
        finally:
            release.touch()
            if not completion.done():
                completion.cancel()
            await asyncio.gather(completion, return_exceptions=True)
        calls = [
            event["data"]
            for event in self.events()
            if event["event_type"] == "weather.request"
        ]
        self.assertEqual(len(calls), 2)
        self.assertNotEqual(calls[0]["request_id"], calls[1]["request_id"])
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
            self.runner._state.template.model_dump(exclude_unset=True)["stages"][1][
                "service_id"
            ],
            self.sid,
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
