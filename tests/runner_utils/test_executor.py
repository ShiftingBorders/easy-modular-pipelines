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
from core.runner_utils.protocol import read_frame
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from tests.helpers.dag import (
    REPOSITORY,
    DagSession,
    DagWorkspace,
    process_running,
    terminate_owned,
    wait_until,
)


def journal_result(state, directory):
    from core.logger import OperationLogger

    context = read_json(directory / "context.json")
    with OperationLogger(
        Path(context["logging_config_path"]), read_only=True
    ) as logger:
        record = logger.read_command_result(context["context"]["request_id"])
        if record is None:
            return None
        observation = next(
            item for item in record["observations"] if item["author"] == "participant"
        )
        return {**record, "response": observation["event"]["data"]["response"]}


class StageExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        self.session = DagSession(self.workspace)
        await self.session.start()
        self.addAsyncCleanup(self.session.close)

    async def test_result_published_during_reconnect_is_accepted(self):
        """Use the durable result when a disconnected executor finishes before retry."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            marker = str(uuid4())
            module = self.workspace.module()
            stage = self.workspace.stage(
                module, settings={"gate": str(gate), "echo": marker}, timeout=30
            )
            template = self.workspace.template([stage, self.workspace.stage(module)])
            template["unknown_state"]["timeout_seconds"] = 10
            await self.session.launch(template)
            step = self.session.post("step")
            _, ready = await self.session.ready_attempt()
            runner = self.session.runner
            original_attempt = runner._state.active_attempt
            request_id = original_attempt.request_id
            connect = runner._stages._connect
            connection_errors = []
            published_responses = []

            async def finish_before_connect(attempt, timeout):
                self.assertEqual(attempt.request_id, request_id)
                self.assertIsNone(runner._journal.client.read_command_result(request_id))
                gate.touch()
                record = await wait_until(
                    lambda: runner._journal.client.read_command_result(request_id),
                    timeout=timeout,
                )
                self.assertEqual(record["author"], "participant")
                self.assertEqual(record["outcome"], "succeeded")
                published_responses.append(record["response"])
                await wait_until(lambda: not attempt.endpoint_path.exists(), timeout=timeout)
                try:
                    await connect(attempt, timeout)
                except FileNotFoundError as error:
                    connection_errors.append(error)
                    raise

            with patch.object(runner._stages, "_connect", new=finish_before_connect):
                # Close a real TCP client while its stage is still waiting at the gate.
                await runner._stages._connection.close()
                reply = await step

            self.assertTrue(connection_errors, "The real endpoint connection must fail.")
            self.assertEqual(reply["result"], "success", reply)
            state = runner._state
            self.assertIsNone(state.active_attempt, runner.get_state())
            self.assertEqual(state.unknown_state_recovery_count, 0)
            self.assertEqual(state.last_result_id, request_id)
            accepted = runner._journal.client.read_command_result(request_id)
            self.assertEqual(accepted["author"], "runner")
            self.assertEqual(accepted["outcome"], "succeeded")
            self.assertEqual(accepted["response"], published_responses[0])
            self.assertEqual(state.last_result["echo"], marker)
            self.assertEqual(state.stage_attempt_numbers[stage["stage_id"]], 1)
            trace = state.experiment_directory / "shared_data/trace.jsonl"
            entries = [
                json.loads(line) for line in trace.read_text(encoding="utf-8").splitlines()
            ]
            starts = [entry for entry in entries if entry["event"] == "start"]
            self.assertEqual(len(starts), 1)
            self.assertEqual(starts[0]["pid"], ready["pid"])

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
            endpoint_path = self.session.runner._state.active_attempt.endpoint_path
            endpoint = read_json(endpoint_path)
            expected = {
                key: endpoint[key]
                for key in (
                    "experiment_id",
                    "participant_id",
                    "participant_instance_id",
                )
            }
            wrong = {**expected, "participant_instance_id": str(uuid4())}
            with self.assertRaises(ValueError):
                await ParticipantConnection(endpoint_path, wrong).connect(
                    timeout_seconds=1
                )
            modified = copy.deepcopy(endpoint)
            modified["process"]["created_at_os"] += 1
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
                self.assertEqual(
                    status["data"]["current"],
                    {
                        "request_id": self.session.runner._state.active_attempt.request_id,
                        "command": "execute",
                    },
                )
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
            connection = ParticipantConnection(
                self.workspace.root / "unused.json",
                {
                    "experiment_id": "frame-test",
                    "participant_id": str(uuid4()),
                    "participant_instance_id": str(uuid4()),
                },
            )
            try:
                connection._reader, connection._writer = await asyncio.open_connection(
                    "127.0.0.1", server.sockets[0].getsockname()[1]
                )
                result = asyncio.create_task(read_frame(connection._reader))
                await sent.wait()
                done, _ = await asyncio.wait({result}, timeout=0.05)
                self.assertEqual(done, set())
                release.set()
                self.assertEqual(await result, {"message": "Привет 🌍"})
                self.assertEqual(await read_frame(connection._reader), {"number": 2})
                with self.assertRaises(asyncio.IncompleteReadError):
                    await read_frame(connection._reader)
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
                        self.workspace.root / "unused.json",
                        {
                            "experiment_id": "frame-test",
                            "participant_id": str(uuid4()),
                            "participant_instance_id": str(uuid4()),
                        },
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
                            await read_frame(connection._reader)
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
                    state = self.session.runner._state
                    self.assertEqual(
                        list(state.experiment_directory.rglob("execution_result.json")),
                        [],
                    )
                    events = self.workspace.events(self.session.runner)
                    record = next(
                        row
                        for row in reversed(events)
                        if row["event_type"] == "command.result"
                        and row["data"]["author"] == "participant"
                    )
                    result = record["data"]["response"]
                    self.assertEqual(
                        result["execution"]["exit_code"], 7 if mode == "nonzero" else 0
                    )
                    if mode in ("invalid", "empty", "extra", "missing_data"):
                        self.assertEqual(
                            result["data"]["error"]["code"], "stage_execution_failed"
                        )
                    await self.session.send("stop")

    async def test_interrupt_before_execute_exits_without_starting_module(self):
        """B6: a result waiter alone must not keep an unused executor alive."""
        state, directory, process, output = await self._probe("no_execute")
        context = read_json(directory / "launch.json")["context"]
        connection = ParticipantConnection(directory / "executor.lock.json", context)
        try:
            await connection.connect(timeout_seconds=3)
            reply = await connection.request(
                str(uuid4()),
                "interrupt",
                {"request_id": context["request_id"], "reason": "stopped"},
                timeout_seconds=3,
            )
            self.assertTrue(reply["data"]["stopped"])
            await asyncio.wait_for(asyncio.shield(output), 3)
            self.assertEqual(process.returncode, 0)
            self.assertFalse((directory / "ready.json").exists())
            self.assertIsNone(read_json(directory / "process.json")["stage"])
            self.assertEqual(
                journal_result(state, directory)["response"]["data"]["reason"],
                "stopped",
            )
        finally:
            await connection.close()

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
        context.update(
            participant_id=context["stage_id"],
            participant_instance_id=context["attempt_id"],
            request_id=context["attempt_id"],
        )
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
                try:
                    await asyncio.wait_for(asyncio.shield(output), 3)
                except TimeoutError:
                    terminate_owned(read_json(directory / "process.json")["executor"])
                    await asyncio.wait_for(asyncio.shield(output), 5)
            else:
                await output

        self.addAsyncCleanup(cleanup)
        connection = ParticipantConnection(directory / "executor.lock.json", context)
        await wait_until(lambda: (directory / "executor.lock.json").exists())
        await connection.connect(timeout_seconds=10)
        if fault == "no_execute":
            await connection.close()
            return state, directory, process, output

        async def dispatch():
            try:
                await connection.request(
                    context["request_id"], "execute", launch["call"], timeout_seconds=20
                )
            except (OSError, TimeoutError):
                pass
            finally:
                await connection.close()

        call = asyncio.create_task(dispatch())
        self.addAsyncCleanup(asyncio.gather, call, return_exceptions=True)
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
                    self.assertTrue((directory / "executor.lock.json").is_file())
                    self.assertEqual(
                        journal_result(state, directory) is not None,
                        fault == "after_result",
                    )
                    metadata = read_json(directory / "process.json")
                    terminate_owned(metadata["executor"])
                    await asyncio.wait_for(asyncio.shield(output), 5)
                    self.assertNotEqual(process.returncode, 0)
                    self.assertFalse(process_running(metadata["stage"]["pid"]))
                    if fault == "after_result":
                        result = journal_result(state, directory)["response"]
                        self.assertEqual(result["execution"]["exit_code"], 0)
                        self.assertEqual(result["result"], "success")

    async def test_result_write_failure_keeps_lock_and_reports_failure(self):
        """B8/B9: failure to publish mandatory result cannot look like success."""
        async with asyncio.timeout(30):
            state, directory, process, output = await self._probe("write_failure")
            await asyncio.wait_for(asyncio.shield(output), 5)
            self.assertNotEqual(process.returncode, 0)
            self.assertFalse((directory / "execution_result.json").exists())
            self.assertIsNone(journal_result(state, directory))

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
                journal_result(self.session.runner._state, directory)["response"][
                    "execution"
                ]["interruption_reason"],
                "timeout",
            )

    async def test_runner_deadline_ignores_a_late_success(self):
        """B10: runner T+margin remains effective when executor timeout is suppressed."""
        async with asyncio.timeout(30):
            gate = self.workspace.gate()
            await self.session.launch(
                self.workspace.template(
                    [self.workspace.stage(settings={"gate": str(gate)}, timeout=5)]
                )
            )
            stages = self.session.runner._stages
            original = ParticipantConnection.request

            async def request(connection, request_id, command, args, **kwargs):
                if command == "execute":
                    kwargs["deadline_monotonic"] = None
                return await original(connection, request_id, command, args, **kwargs)

            reasons = []

            async def finish_instead_of_kill(state, reason):
                reasons.append(reason)
                gate.touch()
                await wait_until(
                    lambda: any(
                        item["author"] == "participant"
                        for item in self.session.runner._journal.client.read_command_result(
                            state.active_attempt.request_id
                        )["observations"]
                    )
                )
                return True

            with (
                patch.object(ParticipantConnection, "request", request),
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
                {"result": "fail", "data": {"reason": "timeout"}},
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
