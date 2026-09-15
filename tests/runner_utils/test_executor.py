"""Approved basic_dag.md B1-B10: independent stage execution and TCP protocol."""

import asyncio
import copy
import ctypes
import json
import os
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.experimentassembler import ExperimentAssembler
from core.logger_utils.events import LoggingStorageError
from core.runner_utils.connection import ParticipantConnection
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.launch import ModuleLauncher
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from tests.helpers.dag import (
    REPOSITORY,
    DagSession,
    DagWorkspace,
    process_running,
    terminate_owned,
    wait_until,
)


class StageExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.session = DagSession(self.workspace)
        await self.session.start()
        self.addAsyncCleanup(self.session.close)

    async def test_full_context_merging_and_streams_are_available_before_completion(
        self,
    ):
        """B1/B2/B4: real context and streaming data arrive while a stage is held."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            defaults = {"nested": {"left": 1, "values": [1, 2]}, "nullable": "default"}
            module = self.workspace.module(defaults=defaults)
            stage = self.workspace.stage(
                module,
                settings={
                    "nested": {"right": 2, "values": [9]},
                    "nullable": None,
                    "echo": "данные🌍" * 12000,
                    "gate": str(gate),
                    "split_output": True,
                },
            )
            await self.session.launch(self.workspace.template([stage]))
            step = self.session.post("step")
            directory, ready = await self.session.ready_attempt()
            context = read_json(directory / "received.json")
            self.assertEqual(
                context["settings"]["nested"], {"left": 1, "right": 2, "values": [9]}
            )
            self.assertIsNone(context["settings"]["nullable"])
            self.assertEqual(context["settings"]["echo"], stage["settings"]["echo"])
            for field in (
                "experiment_directory",
                "resources_directory",
                "settings_directory",
                "module_data_directory",
                "artifacts_directory",
                "logging_config_path",
            ):
                self.assertTrue(Path(context[field]).is_absolute(), field)
            await wait_until(
                lambda: (
                    {
                        row["data"]["stream"]
                        for row in self.workspace.events(self.session.runner)
                        if row["event_type"] == "command.output"
                    }
                    == {"stdout", "stderr"}
                )
            )
            self.assertFalse(step.done())
            self.assertTrue(process_running(ready["pid"]))
            gate.touch()
            reply = await step
            self.assertEqual(reply["result"], "success", reply)
            self.assertEqual(
                reply["data"]["result"]["data"]["echo"], stage["settings"]["echo"]
            )
            original = self.session.runner._assembler.read_module(
                self.workspace.root / "modules/worker/1"
            )
            self.assertEqual(original["defaults"], defaults)

    async def test_parameters_and_intent_precede_process_start(self):
        """B3: committed journal ordering is checked across independent processes."""
        async with asyncio.timeout(30):
            await self.session.launch(self.workspace.template())
            self.assertEqual((await self.session.send("step"))["result"], "success")
            events = self.workspace.events(self.session.runner)
            types = [event["event_type"] for event in events]
            self.assertLess(
                types.index("template.applied"), types.index("attempt.parameters")
            )
            self.assertLess(
                types.index("attempt.parameters"), types.index("control.intent")
            )
            self.assertLess(
                types.index("control.intent"), types.index("stage.process_started")
            )
            parameters = events[types.index("attempt.parameters")]
            self.assertEqual(
                parameters["data"]["template_yaml"],
                self.session.runner._state.template_yaml,
            )
            self.assertEqual(
                parameters["data"]["template"], self.session.runner._state.template
            )

    async def test_parameter_write_failure_never_spawns_executor(self):
        """B3: a refused mandatory record blocks all process startup."""
        async with asyncio.timeout(30):
            await self.session.launch(self.workspace.template())
            with (
                patch.object(
                    self.session.runner._journal.client,
                    "record_attempt_parameters",
                    side_effect=LoggingStorageError("parameters refused"),
                ),
                patch("core.runner_utils.stages.subprocess.Popen") as spawn,
            ):
                result = await self.session.send("step")
                self.assertEqual(result["result"], "fail")
                spawn.assert_not_called()
            self.assertEqual(self.session.runner.get_state()["phase"], "failed")
            self.assertEqual(
                list(self.workspace.root.glob("experiments/**/ready.json")), []
            )

    async def test_handshake_identity_token_queries_and_duplicate_request_id(self):
        """B5/B7: real connections authenticate and answer while the module is alive."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            await self.session.launch(
                self.workspace.template(
                    [self.workspace.stage(settings={"gate": str(gate)})]
                )
            )
            step = self.session.post("step")
            _, ready = await self.session.ready_attempt()
            root = self.session.runner._state.experiment_directory
            endpoint_path = root / "executor.lock.json"
            endpoint = read_json(endpoint_path)
            expected = {
                key: endpoint[key]
                for key in ("experiment_id", "stage_id", "attempt_id")
            }
            wrong = {**expected, "attempt_id": str(uuid4())}
            with self.assertRaises(ValueError):
                await ParticipantConnection(endpoint_path, wrong).connect(
                    timeout_seconds=1
                )
            modified = copy.deepcopy(endpoint)
            modified["executor"]["created_at_os"] += 1
            write_json(endpoint_path, modified)
            try:
                with self.assertRaises(ValueError):
                    await ParticipantConnection(endpoint_path, expected).connect(
                        timeout_seconds=1
                    )
            finally:
                write_json(endpoint_path, endpoint)
            token_path = Path(endpoint["endpoint"]["token_file"])
            token = token_path.read_text()
            token_path.write_text("wrong token", encoding="utf-8")
            try:
                with self.assertRaises((ValueError, asyncio.IncompleteReadError)):
                    await ParticipantConnection(endpoint_path, expected).connect(
                        timeout_seconds=1
                    )
            finally:
                token_path.write_text(token, encoding="utf-8")
            connection = ParticipantConnection(endpoint_path, expected)
            await connection.connect(timeout_seconds=1)
            try:
                heartbeat_id = str(uuid4())
                heartbeat = await connection.request(
                    heartbeat_id, "heartbeat", {}, timeout_seconds=1
                )
                self.assertEqual(heartbeat["request_id"], heartbeat_id)
                self.assertEqual(heartbeat["data"]["process"]["pid"], ready["pid"])
                self.assertFalse(heartbeat["data"]["finished"])
                status = await connection.query_command_state(
                    str(uuid4()), timeout_seconds=1
                )
                self.assertEqual(status["data"]["current"], expected)
                with self.assertRaisesRegex(ValueError, "twice"):
                    await connection.request(
                        heartbeat_id, "heartbeat", {}, timeout_seconds=1
                    )
                self.assertTrue(process_running(ready["pid"]))
            finally:
                await connection.close()
            gate.touch()
            self.assertEqual((await step)["result"], "success")
            self.assertNotIn(
                token, json.dumps(self.workspace.events(self.session.runner))
            )

    async def test_partial_frames_multiple_messages_and_eof(self):
        """B6: actual TCP byte boundaries are independent of JSON message boundaries."""
        async with asyncio.timeout(30):
            sent, release = asyncio.Event(), asyncio.Event()
            first = json.dumps({"message": "Привет 🌍"}, ensure_ascii=False).encode(
                "utf-8"
            )
            second = b'{"number":2}'

            async def peer(reader, writer):
                writer.write(len(first).to_bytes(8, "big")[:3])
                await writer.drain()
                sent.set()
                await release.wait()
                writer.write(
                    len(first).to_bytes(8, "big")[3:]
                    + first
                    + len(second).to_bytes(8, "big")
                    + second
                )
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            server = await asyncio.start_server(peer, "127.0.0.1", 0)
            connection = ParticipantConnection(self.workspace.root / "unused.json", {})
            try:
                connection._reader, connection._writer = await asyncio.open_connection(
                    "127.0.0.1", server.sockets[0].getsockname()[1]
                )
                result = asyncio.create_task(connection.receive_message())
                await sent.wait()
                done, _ = await asyncio.wait({result}, timeout=0.05)
                self.assertEqual(done, set())
                release.set()
                self.assertEqual(await result, {"message": "Привет 🌍"})
                self.assertEqual(await connection.receive_message(), {"number": 2})
                with self.assertRaises(asyncio.IncompleteReadError):
                    await connection.receive_message()
                self.assertIsNone(connection._writer)
            finally:
                release.set()
                await connection.close()
                server.close()
                await server.wait_closed()

    async def test_malformed_and_truncated_frames_close_connection(self):
        """B6: invalid length/UTF-8/JSON and truncated frames close the stream."""
        async with asyncio.timeout(30):
            for frame in (
                b"\0" * 8,
                (4).to_bytes(8, "big") + b"bad!",
                (1).to_bytes(8, "big") + b"\xff",
                (9).to_bytes(8, "big") + b"{}",
                b"\0\0",
            ):
                with self.subTest(frame=frame):

                    async def peer(reader, writer, frame=frame):
                        writer.write(frame)
                        await writer.drain()
                        writer.close()
                        await writer.wait_closed()

                    server = await asyncio.start_server(peer, "127.0.0.1", 0)
                    connection = ParticipantConnection(
                        self.workspace.root / "unused.json", {}
                    )
                    try:
                        (
                            connection._reader,
                            connection._writer,
                        ) = await asyncio.open_connection(
                            "127.0.0.1", server.sockets[0].getsockname()[1]
                        )
                        with self.assertRaises(
                            (ValueError, UnicodeError, asyncio.IncompleteReadError)
                        ):
                            await connection.receive_message()
                        self.assertIsNone(connection._writer)
                    finally:
                        await connection.close()
                        server.close()
                        await server.wait_closed()

    async def test_exit_status_and_stdout_jointly_determine_success(self):
        """B8: success requires both the process exit and exactly one valid result."""
        async with asyncio.timeout(30):
            module = self.workspace.module(implementation="action")
            for mode in (
                "success",
                "fail",
                "invalid",
                "empty",
                "extra",
                "missing_data",
                "nonzero",
            ):
                with self.subTest(mode=mode):
                    await self.session.launch(
                        self.workspace.template(
                            [self.workspace.stage(module, settings={"mode": mode})]
                        )
                    )
                    reply = await self.session.send("step")
                    self.assertEqual(
                        reply["result"],
                        "success" if mode == "success" else "fail",
                        reply,
                    )
                    paths = list(
                        self.session.runner._state.experiment_directory.glob(
                            "shared_artifacts/**/execution_result.json"
                        )
                    )
                    self.assertEqual(len(paths), 1)
                    result = read_json(paths[0])
                    self.assertEqual(result["exit_code"], 7 if mode == "nonzero" else 0)
                    if mode in ("invalid", "empty", "extra", "missing_data"):
                        self.assertEqual(
                            result["error"]["code"], "invalid_stage_result"
                        )
                    await self.session.send("stop")

    async def _probe(self, fault: str):
        module = self.workspace.module(f"probe-{fault}")
        stage = self.workspace.stage(module)
        assembler = ExperimentAssembler(self.workspace.root, self.workspace.manager)
        state = await assembler.assemble(
            self.workspace.write_template(self.workspace.template([stage])), fault
        )
        journal = RunnerJournal()
        journal.open(state, create=True)
        self.addCleanup(journal.close)
        context = {
            "experiment_id": state.experiment_id,
            "run_id": state.run_id,
            "template_revision_id": state.template_revision_id,
            "stage_id": stage["stage_id"],
            "stage_execution_id": str(uuid4()),
            "attempt_id": str(uuid4()),
            "cycle_number": 1,
            "attempt_number": 1,
            "module_name": module["name"],
            "module_version": module["version"],
            "module_hash": module["hash"],
        }
        directory = state.experiment_directory / "shared_artifacts/probe"
        launch = ModuleLauncher(assembler, journal).prepare(
            state, stage, context, directory, None
        )
        write_json(directory / "launch.json", launch)
        process = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--project",
            str(REPOSITORY),
            "--no-sync",
            "python",
            "-B",
            "-m",
            "tests.helpers.dag_executor",
            "--launch",
            str(directory / "launch.json"),
            "--fault",
            fault,
            cwd=REPOSITORY,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        output = asyncio.create_task(process.communicate())

        async def cleanup():
            (directory / "release-executor").touch(exist_ok=True)
            if process.returncode is None:
                await asyncio.wait_for(asyncio.shield(output), 8)
            else:
                await output

        self.addAsyncCleanup(cleanup)
        await wait_until(lambda: (directory / "checkpoint.json").is_file())
        return state, directory, process, output

    async def test_result_publication_precedes_lock_removal_and_survives_executor_crash(
        self,
    ):
        """B9: a crash on either side of publication must not manufacture success."""
        async with asyncio.timeout(30):
            for fault in ("before_result", "after_result"):
                with self.subTest(fault=fault):
                    state, directory, process, output = await self._probe(fault)
                    self.assertTrue(
                        (state.experiment_directory / "executor.lock.json").is_file()
                    )
                    self.assertEqual(
                        (directory / "execution_result.json").is_file(),
                        fault == "after_result",
                    )
                    metadata = read_json(directory / "process.json")
                    terminate_owned(metadata["executor"])
                    await asyncio.wait_for(asyncio.shield(output), 5)
                    self.assertNotEqual(process.returncode, 0)
                    self.assertFalse(process_running(metadata["stage"]["pid"]))
                    if fault == "after_result":
                        result = read_json(directory / "execution_result.json")
                        self.assertEqual(result["attempt_id"], metadata["attempt_id"])
                        self.assertEqual(result["exit_code"], 0)
                        self.assertEqual(result["response"]["result"], "success")

    async def test_result_write_failure_keeps_lock_and_reports_failure(self):
        """B8/B9: failure to publish mandatory result cannot look like success."""
        async with asyncio.timeout(30):
            state, directory, process, output = await self._probe("write_failure")
            _, stderr = await asyncio.wait_for(asyncio.shield(output), 5)
            self.assertNotEqual(process.returncode, 0)
            self.assertIn(b"injected result publication failure", stderr)
            self.assertFalse((directory / "execution_result.json").exists())
            self.assertTrue(
                (state.experiment_directory / "executor.lock.json").exists()
            )

    async def test_executor_timeout_terminates_the_stage_and_reports_failed_attempt(
        self,
    ):
        """B10: the independent executor enforces its deadline."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            await self.session.launch(
                self.workspace.template(
                    [self.workspace.stage(settings={"gate": str(gate)}, timeout=1)]
                )
            )
            step = self.session.post("step")
            directory, ready = await self.session.ready_attempt()
            reply = await step
            self.assertEqual(reply["result"], "fail")
            self.assertFalse(process_running(ready["pid"]))
            self.assertEqual(
                read_json(directory / "execution_result.json")["interruption_reason"],
                "timeout",
            )

    async def test_runner_deadline_ignores_a_late_success(self):
        """B10: runner T+margin remains effective when executor timeout is suppressed."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            await self.session.launch(
                self.workspace.template(
                    [self.workspace.stage(settings={"gate": str(gate)}, timeout=0.2)]
                )
            )
            stages = self.session.runner._stages
            original = stages._launcher.prepare

            def prepare(*args, **kwargs):
                launch = original(*args, **kwargs)
                launch["timeout_seconds"] = None
                return launch

            reasons = []

            async def finish_instead_of_kill(state, reason):
                reasons.append(reason)
                gate.touch()
                path = (
                    state.active_attempt.artifacts_directory / "execution_result.json"
                )
                await wait_until(path.is_file)
                result = read_json(path)
                return result["exit_code"] == 0

            with (
                patch.object(stages._launcher, "prepare", side_effect=prepare),
                patch.object(stages, "interrupt", side_effect=finish_instead_of_kill),
            ):
                reply = await self.session.send("step")
            self.assertEqual(reply["result"], "fail")
            self.assertEqual(reasons, ["timeout"])
            self.assertIsNone(self.session.runner.get_state()["result"])
            finishes = [
                row
                for row in self.workspace.events(self.session.runner)
                if row["event_type"] == "stage.finished"
            ]
            self.assertEqual(finishes[-1]["data"]["outcome"], "timed_out")
            self.assertEqual(
                finishes[-1]["data"]["result"],
                {"result": "fail", "data": {"reason": "timed_out"}},
            )

    def test_os_identity_has_exact_creation_value(self):
        """B5: native process identity includes an integer creation value and boot/host."""
        identity = process_identity(os.getpid())
        self.assertEqual(identity["pid"], os.getpid())
        self.assertIs(type(identity["created_at_os"]), int)
        self.assertGreater(identity["created_at_os"], 0)
        self.assertTrue(identity["host_id"])
        self.assertTrue(identity["boot_id"])
        if os.name == "nt":
            from ctypes import wintypes

            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.GetCurrentProcess.restype = wintypes.HANDLE
            kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [
                ctypes.POINTER(wintypes.FILETIME)
            ] * 4
            times = [wintypes.FILETIME() for _ in range(4)]
            self.assertTrue(
                kernel.GetProcessTimes(
                    kernel.GetCurrentProcess(),
                    *(ctypes.byref(value) for value in times),
                )
            )
            expected = int.from_bytes(bytes(times[0]), "little")
        else:
            fields = (
                Path("/proc/self/stat")
                .read_text(encoding="utf-8")
                .rsplit(")", 1)[1]
                .split()
            )
            expected = int(fields[19])
        self.assertEqual(identity["created_at_os"], expected)
