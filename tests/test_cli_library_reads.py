"""Approved direct-library checks for templates, modules and receipts."""

import asyncio
import copy
import os
import queue
import shutil
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import yaml

from core.experimentassembler import ExperimentAssembler
from core.experimentcontroller import ExperimentController
from core.logger_utils.events import LoggingStorageError
from core.maintenancecontroller import MaintenanceController
from core.storage_errors import (
    StorageClosedError,
    StorageError,
    StorageUnavailable,
    StoredObjectNotFound,
)
from tests import test_hashdb, test_modulemanager, test_server_results
from tests.helpers.dag import DagWorkspace, wait_until
from tests.helpers.services import ServiceWorkspace


class ModuleListingTests(test_hashdb.HashDBTestCase):
    def test_empty_ordered_and_closed_hash_listing_q2_13(self):
        database = self._create_hash_db()
        self.addCleanup(database.close_connection)
        self.assertEqual(database.list_module_hashes(), [])
        database.add_module_hash("z", "1", "z-hash")
        database.add_module_hash("a", "2", "a2-hash")
        database.add_module_hash("a", "1", "a1-hash")
        self.assertEqual(
            database.list_module_hashes(),
            [
                {"name": "a", "version": "1", "hash": "a1-hash"},
                {"name": "a", "version": "2", "hash": "a2-hash"},
                {"name": "z", "version": "1", "hash": "z-hash"},
            ],
        )
        database.hash_db.execute('DROP TABLE "MAIN"')
        with self.assertRaises(StorageError):
            database.list_module_hashes()
        database.close_connection()
        with self.assertRaises(StorageClosedError):
            database.list_module_hashes()


class ModuleInspectionTests(test_modulemanager.ModuleManagerTestCase):
    def test_inspection_presence_identity_and_storage_failures_q2_14_15(self):
        self.assertEqual(self.manager.list_modules(), {"items": []})
        with self.assertRaises(StoredObjectNotFound):
            asyncio.run(self.manager.inspect_module("absent", "1"))
        for name, version in (("../outside", "1"), ("demo", ".."), (None, "1")):
            with (
                self.subTest(name=name, version=version),
                self.assertRaises((TypeError, ValueError)),
            ):
                asyncio.run(self.manager.inspect_module(name, version))
        self.manager.register_module("demo", "1", self.source)
        result = asyncio.run(self.manager.inspect_module(" demo ", " 1 "))
        self.assertTrue(result["archive_available"])
        self.assertFalse(result["installed"])
        self.assertEqual(result["integrity"], "not_checked")
        self.assertEqual(result["module"]["name"], "demo")
        (self.storage / "demo/1").mkdir(parents=True)
        self.assertTrue(
            asyncio.run(self.manager.inspect_module("demo", "1"))["installed"]
        )
        self.objects.clear()
        self.assertFalse(
            asyncio.run(self.manager.inspect_module("demo", "1"))["archive_available"]
        )
        with (
            patch.object(
                self.archives,
                "check_module_stored",
                side_effect=StorageUnavailable("offline"),
            ),
            self.assertRaises(StorageUnavailable),
        ):
            asyncio.run(self.manager.inspect_module("demo", "1"))


class TemplateValidationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.files = DagWorkspace()
        self.addCleanup(self.files.close)
        self.assembler = ExperimentAssembler(self.files.root, self.files.manager)
        self.template = self.files.template()
        self.path = self.files.write_template(self.template)

    async def test_optional_service_ids_warn_without_blocking_assembly(self):
        services = ServiceWorkspace()
        self.addCleanup(services.close)
        definition = services.service()
        reference = definition["module"]
        source = services.experiment / "modules" / reference["name"] / "1"
        target = self.files.root / "modules" / reference["name"] / "1"
        shutil.copytree(source, target)
        self.files.manager.register_module(reference["name"], "1", target)
        definition["module"] = {
            **reference,
            "name": f" {reference['name']} ",
            "version": " 1 ",
        }
        unnamed = copy.deepcopy(definition)
        unnamed.pop("service_id")
        stage = self.files.stage(reference)
        stage.pop("module")
        stage["service_id"] = definition["service_id"]
        self.template["stages"].append(stage)
        for index, entries in enumerate(([unnamed, definition], [definition, unnamed])):
            self.template["services"] = entries
            self.files.write_template(self.template)
            result = await self.assembler.validate_template(self.path)
            self.assertTrue(result["valid"])
            self.assertEqual(len(result["modules"]), 2)
            self.assertEqual(len(result["warnings"]), 1)
            self.assertIn(f"services[{index}]", result["warnings"][0])
            state = await self.assembler.assemble(self.path, str(uuid4()))
            self.assertTrue(state.template["services"][index]["service_id"])
        self.template["services"] = [definition]
        self.files.write_template(self.template)
        self.assertEqual(
            (await self.assembler.validate_template(self.path))["warnings"], []
        )
        stage["service_id"] = str(uuid4())
        self.files.write_template(self.template)
        with self.assertRaisesRegex(ValueError, "explicit service_id"):
            await self.assembler.validate_template(self.path)

    async def test_normalized_references_match_assembly_and_preserve_source(self):
        reference = self.files.module("MixedCase", "Version2")
        self.template["stages"] = [
            self.files.stage(
                {
                    **reference,
                    "name": " \tMixedCase ",
                    "version": " Version2\n",
                }
            )
        ]
        self.files.write_template(self.template)
        original = self.path.read_bytes()
        text, normalized = self.assembler.load_template(self.path)
        self.assertEqual(text, self.path.read_text(encoding="utf-8"))
        self.assertEqual(normalized["stages"][0]["module"], reference)
        result = await self.assembler.validate_template(self.path)
        self.assertEqual(result["modules"], [reference])
        state = await self.assembler.assemble(self.path, str(uuid4()))
        saved = yaml.safe_load(state.template_path.read_text(encoding="utf-8"))
        self.assertEqual(saved["stages"][0]["module"], reference)
        self.assertEqual(self.path.read_bytes(), original)
        self.template["stages"][0]["module"].update(
            name=" Mixed Case ", version=" Version 2 ", hash=reference["hash"].upper()
        )
        self.files.write_template(self.template)
        _, normalized = self.assembler.load_template(self.path)
        self.assertEqual(
            normalized["stages"][0]["module"],
            {
                "name": "Mixed Case",
                "version": "Version 2",
                "hash": reference["hash"].upper(),
            },
        )

    async def test_invalid_padded_references_fail_before_creating_files(self):
        for key in ("name", "version"):
            for value in (" \t", " . ", " .. ", " ../outside ", 1, None):
                candidate = copy.deepcopy(self.template)
                candidate["stages"][0]["module"][key] = value
                self.files.write_template(candidate)
                with self.subTest(key=key, value=value):
                    with self.assertRaises((ValueError, TypeError)):
                        await self.assembler.validate_template(self.path)
                    with self.assertRaises((ValueError, TypeError)):
                        await self.assembler.assemble(self.path, str(uuid4()))
        self.assertFalse((self.files.root / "experiments").exists())

    async def test_slow_template_loading_keeps_loop_and_hash_owner_responsive(self):
        loop = asyncio.get_running_loop()
        owner = threading.get_ident()
        started = asyncio.Event()
        release = threading.Event()
        original = self.assembler.load_template
        lookup = self.files.hashes.get_module_hash
        threads = []

        def load(path):
            threads.append(threading.get_ident())
            loop.call_soon_threadsafe(started.set)
            if not release.wait(5):
                raise TimeoutError("Test did not release template loading")
            return original(path)

        def get_hash(*args):
            self.assertEqual(threading.get_ident(), owner)
            return lookup(*args)

        with (
            patch.object(self.assembler, "load_template", side_effect=load),
            patch.object(self.files.hashes, "get_module_hash", side_effect=get_hash),
        ):
            task = asyncio.create_task(self.assembler.validate_template(self.path))
            try:
                await asyncio.wait_for(started.wait(), 2)
                self.assertFalse(task.done())
                self.assertNotEqual(threads, [owner])
            finally:
                release.set()
                result = await asyncio.wait_for(task, 5)
        self.assertTrue(result["valid"])
        self.assertFalse((self.files.root / "experiments").exists())

    async def test_cancellation_during_loading_never_starts_module_inspection(self):
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        finished = asyncio.Event()
        release = threading.Event()
        original = self.assembler.load_template

        def load(path):
            loop.call_soon_threadsafe(started.set)
            try:
                if not release.wait(5):
                    raise TimeoutError("Test did not release template loading")
                return original(path)
            finally:
                loop.call_soon_threadsafe(finished.set)

        with (
            patch.object(self.assembler, "load_template", side_effect=load),
            patch.object(
                self.files.manager, "inspect_module", new_callable=AsyncMock
            ) as inspect,
        ):
            task = asyncio.create_task(self.assembler.validate_template(self.path))
            try:
                await asyncio.wait_for(started.wait(), 2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 2)
            finally:
                release.set()
                await asyncio.wait_for(finished.wait(), 5)
            inspect.assert_not_awaited()
        self.assertFalse((self.files.root / "experiments").exists())

    async def test_repeated_cancellation_waits_for_archive_and_stops_validation(self):
        self.template["stages"].append(self.files.stage(self.files.module("second")))
        self.files.write_template(self.template)
        loop = asyncio.get_running_loop()
        for fail in (False, True):
            started = asyncio.Event()
            release = threading.Event()
            finished = threading.Event()

            def check(
                *args, started=started, release=release, finished=finished, fail=fail
            ):
                loop.call_soon_threadsafe(started.set)
                try:
                    if not release.wait(5):
                        raise TimeoutError("Test did not release archive check")
                    if fail:
                        raise StorageUnavailable("offline after cancellation")
                    return True
                finally:
                    finished.set()

            with (
                self.subTest(fail=fail),
                patch.object(
                    self.files.archives, "check_module_stored", side_effect=check
                ) as check_archive,
            ):
                task = asyncio.create_task(self.assembler.validate_template(self.path))
                try:
                    await asyncio.wait_for(started.wait(), 2)
                    for _ in range(2):
                        task.cancel()
                        await asyncio.sleep(0)
                        self.assertFalse(task.done())
                finally:
                    release.set()
                    with self.assertRaises(asyncio.CancelledError):
                        await asyncio.wait_for(task, 5)
                self.assertTrue(finished.is_set())
                check_archive.assert_called_once_with("worker", "1")

    async def test_validation_failure_inputs_and_no_assembly_q2_10(self):
        for path in (Path("relative.yaml"), self.files.root / "missing.yaml"):
            with (
                self.subTest(path=path),
                self.assertRaises((ValueError, FileNotFoundError)),
            ):
                await self.assembler.validate_template(path)
        for text in ("stages: [", "{}", "[]"):
            self.path.write_text(text, encoding="utf-8")
            with self.subTest(text=text), self.assertRaises((ValueError, TypeError)):
                await self.assembler.validate_template(self.path)
        self.files.write_template(self.template)
        with (
            patch.object(Path, "read_text", side_effect=PermissionError("denied")),
            self.assertRaises(PermissionError),
        ):
            await self.assembler.validate_template(self.path)
        self.assertFalse((self.files.root / "experiments").exists())

    async def test_reference_hash_archive_and_storage_failures_q2_11_12(self):
        reference = self.template["stages"][0]["module"]
        for changes, expected in (
            ({"name": "absent"}, StoredObjectNotFound),
            ({"hash": "f" * 64}, ValueError),
        ):
            candidate = copy.deepcopy(self.template)
            candidate["stages"][0]["module"].update(changes)
            with self.subTest(changes=changes), self.assertRaises(expected):
                await self.assembler.validate_template(
                    self.files.write_template(candidate)
                )
        self.files.write_template(self.template)
        with (
            patch.object(
                self.files.hashes,
                "get_module_hash",
                side_effect=StorageUnavailable("hash offline"),
            ),
            self.assertRaises(StorageUnavailable),
        ):
            await self.assembler.validate_template(self.path)
        with (
            patch.object(
                self.files.archives,
                "check_module_stored",
                side_effect=StorageUnavailable("archive offline"),
            ),
            self.assertRaises(StorageUnavailable),
        ):
            await self.assembler.validate_template(self.path)
        self.files.objects.clear()
        with self.assertRaises(FileNotFoundError):
            await self.assembler.validate_template(self.path)
        self.assertEqual(
            self.files.hashes.get_module_hash(reference["name"], reference["version"]),
            reference["hash"],
        )
        self.assertFalse((self.files.root / "experiments").exists())

    async def test_scope_repeated_references_and_relative_resources_q2_33(self):
        reference = self.template["stages"][0]["module"]
        self.template["stages"].append(self.files.stage(reference))
        self.template["resources"] = [
            {"name": "input", "path": "absent.bin", "hash": "0" * 64}
        ]
        self.files.write_template(self.template)
        previous = Path.cwd()
        try:
            os.chdir(self.files.root / "modules")
            result = await self.assembler.validate_template(self.path)
            _, normalized = self.assembler.load_template(self.path)
        finally:
            os.chdir(previous)
        self.assertTrue(result["valid"])
        self.assertEqual(result["modules"], [reference])
        self.assertFalse(result["archive_integrity_checked"])
        self.assertEqual(
            Path(normalized["resources"][0]["path"]), self.path.parent / "absent.bin"
        )
        self.assertFalse((self.files.root / "experiments").exists())


class ReceiptListingTests(unittest.IsolatedAsyncioTestCase):
    setUp = test_server_results.ResultCacheTests.setUp
    submit = test_server_results.ResultCacheTests.submit
    reply = test_server_results.ResultCacheTests.reply

    def test_filters_order_pages_and_arrivals_q2_17(self):
        self.runtime.settings.max_records = 20
        first = self.submit("pause")
        self.reply(first)
        second = self.submit("run")
        self.reply(second, state="failed")
        third = self.submit("stop")
        page = self.runtime.list_commands(limit=1)
        self.assertEqual([item["command_id"] for item in page["items"]], [first])
        self.assertTrue(page["has_more"])
        self.assertEqual(page["next_after"], first)
        fourth = self.submit("pause")
        self.assertEqual(
            [
                item["command_id"]
                for item in self.runtime.list_commands(after=first)["items"]
            ],
            [second, third, fourth],
        )
        self.assertEqual(
            [
                item["command_id"]
                for item in self.runtime.list_commands(state="failed", command="run")[
                    "items"
                ]
            ],
            [second],
        )
        self.assertEqual(self.runtime.list_commands(command="missing")["items"], [])

    def test_invalid_and_expired_cursor_q2_16_17(self):
        for args in ({"limit": 0}, {"limit": 1001}, {"state": "bad"}, {"after": "bad"}):
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.runtime.list_commands(**args)
        with self.assertRaises(test_server_results.ServerError):
            self.runtime.list_commands(after=str(uuid4()))
        identifier = self.submit()
        self.reply(identifier)
        self.runtime.settings.result_ttl = 0
        # Completion has already occurred; advance the pruning clock deterministically.
        with (
            patch("core.serverruntime.time.monotonic", return_value=10**12),
            self.assertRaises(test_server_results.ServerError),
        ):
            self.runtime.list_commands(after=identifier)

    async def test_runtime_read_limits_q2_31(self):
        for name in ("stats.experiments", "stats.snapshots", "stats.artifacts"):
            with (
                self.subTest(command=name),
                self.assertRaises(test_server_results.ServerError) as failure,
            ):
                await self.runtime.read(name)
            self.assertEqual(failure.exception.code, "controller_timeout")
            self.assertEqual(self.runtime._reads, {})
            self.runtime._requests.get_nowait()
        self.runtime.settings.max_response_bytes = 100
        task = asyncio.create_task(self.runtime.read("stats.artifacts"))
        await asyncio.sleep(0)
        request = self.runtime._requests.get_nowait()
        self.runtime._accept_response(
            {
                "command_id": request["command_id"],
                "state": "succeeded",
                "result": "success",
                "data": {"items": ["large" * 100]},
                "error": None,
            }
        )
        result = await task
        self.assertEqual(result["error"]["code"], "response_too_large")
        self.assertIsNone(result["data"])


class ControllerReadTests(unittest.IsolatedAsyncioTestCase):
    async def test_maintenance_mutation_chains_remain_serial_after_read_failure(self):
        files = DagWorkspace()
        self.addCleanup(files.close)
        requests, responses = queue.Queue(), queue.Queue()
        controller = MaintenanceController(
            files.manager,
            SimpleNamespace(
                record_error=lambda *a, **k: None, record_event=lambda *a, **k: None
            ),
            requests,
            responses,
            shutdown_requested=asyncio.Event(),
            recovery_required=[],
            project_root=files.root,
        )
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = []

        async def remove(name, version):
            calls.append(name)
            if name == "first":
                entered.set()
                await release.wait()
            if name == "fail":
                raise StorageUnavailable("mutation failed")
            return True

        commands = [
            {
                "command_id": name,
                "command": "module.remove",
                "args": {"name": name, "version": "1"},
            }
            for name in ("first", "fail", "cancelled", "next")
        ]
        with patch.object(files.manager, "unregister_module_async", side_effect=remove):
            serving = asyncio.create_task(controller.serve())
            try:
                requests.put({"chain_id": "chain", "commands": commands[:3]})
                requests.put(commands[3])
                await asyncio.wait_for(entered.wait(), 2)
                requests.put(
                    {"command_id": "bad-read", "command": "stats.module", "args": {}}
                )
                requests.put({"command_id": "state", "command": "stats.state"})
                await wait_until(lambda: responses.qsize() == 2)
                reads = {
                    response["command_id"]: response
                    for response in (responses.get_nowait(), responses.get_nowait())
                }
                self.assertEqual(reads["bad-read"]["error"]["code"], "invalid_request")
                self.assertEqual(
                    reads["state"]["data"]["current_command"]["command_id"], "first"
                )
                self.assertEqual(calls, ["first"])
                release.set()
                await wait_until(lambda: responses.qsize() == 4)
                results = [responses.get_nowait() for _ in range(4)]
                self.assertEqual(
                    [row["command_id"] for row in results],
                    ["first", "fail", "cancelled", "next"],
                )
                self.assertEqual(
                    [row["state"] for row in results],
                    ["succeeded", "failed", "cancelled", "succeeded"],
                )
                self.assertEqual(
                    [row["chain_id"] for row in results],
                    ["chain", "chain", "chain", None],
                )
                self.assertEqual(calls, ["first", "fail", "next"])
            finally:
                release.set()
                await controller.close()
                await asyncio.gather(serving, return_exceptions=True)

    async def test_maintenance_read_saturation_preserves_state_and_shutdown(self):
        files = DagWorkspace()
        self.addCleanup(files.close)
        requests, responses = queue.Queue(), queue.Queue()
        shutdown = asyncio.Event()
        controller = MaintenanceController(
            files.manager,
            SimpleNamespace(record_error=lambda *a, **k: None),
            requests,
            responses,
            shutdown_requested=shutdown,
            recovery_required=[],
            project_root=files.root,
        )
        started = []
        release = asyncio.Event()
        original = controller._execute

        async def execute(command):
            if command["command"] != "stats.state":
                started.append(command["command_id"])
                await release.wait()
            return await original(command)

        with patch.object(controller, "_execute", side_effect=execute):
            serving = asyncio.create_task(controller.serve())
            try:
                for index in range(4):
                    requests.put({"command_id": str(index), "command": "stats.modules"})
                await wait_until(lambda: len(started) == 4)
                for index in range(64):
                    requests.put(
                        {"command_id": f"queued-{index}", "command": "stats.modules"}
                    )
                await wait_until(controller._read_commands.full)
                requests.put({"command_id": "overflow", "command": "stats.modules"})
                requests.put({"command_id": "state", "command": "stats.state"})
                await wait_until(lambda: responses.qsize() == 2)
                overflow, state = responses.get_nowait(), responses.get_nowait()
                self.assertEqual(overflow["command_id"], "overflow")
                self.assertEqual(overflow["error"]["code"], "too_many_reads")
                self.assertEqual(state["command_id"], "state")
                self.assertIsNone(state["data"]["current_command"])
                self.assertEqual(len(started), 4)
                requests.put({"command": "server.shutdown"})
                await asyncio.wait_for(shutdown.wait(), 2)
            finally:
                await asyncio.wait_for(controller.close(), 5)
                await asyncio.gather(serving, return_exceptions=True)
            self.assertEqual(len(started), 4)
            self.assertTrue(all(task.done() for task in controller._tasks))
            self.assertTrue(controller._read_commands.empty())

    async def test_slow_maintenance_reads_and_failures_leave_control_responsive(self):
        files = DagWorkspace()
        self.addCleanup(files.close)
        template = files.write_template(files.template())
        for name, args, attribute, method in (
            (
                "stats.module",
                {"name": "worker", "version": "1"},
                files.manager,
                "inspect_module",
            ),
            (
                "stats.template",
                {"template_path": str(template)},
                None,
                "validate_template",
            ),
            ("stats.artifacts", {"experiment_id": "saved"}, None, "read"),
        ):
            with self.subTest(command=name):
                requests, responses = queue.Queue(), queue.Queue()
                controller = MaintenanceController(
                    files.manager,
                    SimpleNamespace(record_error=lambda *a, **k: None),
                    requests,
                    responses,
                    shutdown_requested=asyncio.Event(),
                    recovery_required=[],
                    project_root=files.root,
                )
                target = attribute or (
                    controller._assembler
                    if name == "stats.template"
                    else controller._experiment_reader
                )
                entered = asyncio.Event()
                release = threading.Event()
                loop = asyncio.get_running_loop()

                def blocking(
                    *args, loop=loop, entered=entered, release=release, **kwargs
                ):
                    loop.call_soon_threadsafe(entered.set)
                    if not release.wait(5):
                        raise TimeoutError("Test did not release read")
                    raise StorageUnavailable("read failed")

                async def delayed(*args, blocking=blocking, **kwargs):
                    return await asyncio.to_thread(blocking)

                replacement = blocking if method == "read" else delayed
                with patch.object(target, method, side_effect=replacement):
                    serving = asyncio.create_task(controller.serve())
                    try:
                        requests.put(
                            {"command_id": "slow", "command": name, "args": args}
                        )
                        await asyncio.wait_for(entered.wait(), 2)
                        requests.put({"command_id": "state", "command": "stats.state"})
                        await wait_until(
                            lambda responses=responses: not responses.empty()
                        )
                        self.assertEqual(responses.get_nowait()["command_id"], "state")
                        release.set()
                        await wait_until(
                            lambda responses=responses: not responses.empty()
                        )
                        failed = responses.get_nowait()
                        self.assertEqual(failed["command_id"], "slow")
                        self.assertEqual(failed["error"]["code"], "storage_error")
                        requests.put(
                            {"command_id": "after", "command": "stats.modules"}
                        )
                        await wait_until(
                            lambda responses=responses: not responses.empty()
                        )
                        response = responses.get_nowait()
                        self.assertEqual(response["command_id"], "after")
                        self.assertEqual(response["result"], "success")
                    finally:
                        release.set()
                        await controller.close()
                        await asyncio.gather(serving, return_exceptions=True)

    async def test_maintenance_close_waits_for_archive_and_active_mutation(self):
        files = DagWorkspace()
        self.addCleanup(files.close)
        files.module()
        loop = asyncio.get_running_loop()
        archive_started = asyncio.Event()
        archive_release = threading.Event()
        mutation_started = asyncio.Event()
        mutation_release = asyncio.Event()
        requests, responses = queue.Queue(), queue.Queue()
        controller = MaintenanceController(
            files.manager,
            SimpleNamespace(
                record_error=lambda *a, **k: None, record_event=lambda *a, **k: None
            ),
            requests,
            responses,
            shutdown_requested=asyncio.Event(),
            recovery_required=[],
            project_root=files.root,
        )

        def archive(*args):
            loop.call_soon_threadsafe(archive_started.set)
            if not archive_release.wait(5):
                raise TimeoutError("Test did not release archive")
            return True

        async def mutate(*args):
            mutation_started.set()
            await mutation_release.wait()
            return True

        with (
            patch.object(files.archives, "check_module_stored", side_effect=archive),
            patch.object(files.manager, "unregister_module_async", side_effect=mutate),
        ):
            serving = asyncio.create_task(controller.serve())
            closing = None
            try:
                requests.put(
                    {
                        "command_id": "read",
                        "command": "stats.module",
                        "args": {"name": "worker", "version": "1"},
                    }
                )
                requests.put(
                    {
                        "command_id": "mutation",
                        "command": "module.remove",
                        "args": {"name": "worker", "version": "1"},
                    }
                )
                await asyncio.wait_for(archive_started.wait(), 2)
                await asyncio.wait_for(mutation_started.wait(), 2)
                requests.put({"command_id": "state", "command": "stats.state"})
                await wait_until(lambda responses=responses: not responses.empty())
                self.assertEqual(
                    responses.get_nowait()["data"]["current_command"]["command_id"],
                    "mutation",
                )
                closing = asyncio.create_task(controller.close())
                await asyncio.sleep(0)
                self.assertFalse(closing.done())
                archive_release.set()
                await wait_until(
                    lambda: all(task.done() for task in controller._read_tasks)
                )
                self.assertFalse(closing.done())
                mutation_release.set()
                await asyncio.wait_for(closing, 5)
                self.assertEqual(responses.get_nowait()["command_id"], "mutation")
                self.assertTrue(all(task.done() for task in controller._tasks))
            finally:
                archive_release.set()
                mutation_release.set()
                if closing is not None:
                    await closing
                else:
                    await controller.close()
                await asyncio.gather(serving, return_exceptions=True)

    async def test_historical_read_failure_does_not_fail_selected_runner_q2_25(self):
        controller = ExperimentController.__new__(ExperimentController)
        runner = SimpleNamespace(
            get_state=lambda: {"experiment_id": "active"}, _fail=AsyncMock()
        )
        controller._runner = runner
        failure = LoggingStorageError("damaged history")
        failure.journal_failed = True
        controller._read_request = AsyncMock(side_effect=failure)
        for name in ("stats.artifacts", "stats.artifact"):
            response = await controller._execute_command(
                {
                    "command_id": str(uuid4()),
                    "command": name,
                    "args": {"experiment_id": "old"},
                }
            )
            self.assertEqual(response["error"]["code"], "journal_unavailable")
            self.assertEqual(runner.get_state()["experiment_id"], "active")
        runner._fail.assert_not_awaited()

    async def test_module_reads_route_in_both_modes(self):
        files = DagWorkspace()
        self.addCleanup(files.close)
        reference = files.module()
        run = ExperimentController.__new__(ExperimentController)
        run._module_manager = files.manager
        run._project_root = files.root
        maintenance = MaintenanceController(
            files.manager,
            SimpleNamespace(record_error=lambda *a, **k: None),
            None,
            None,
            shutdown_requested=asyncio.Event(),
            recovery_required=[],
            project_root=files.root,
        )
        for name, args in (
            ("stats.modules", {}),
            (
                "stats.module",
                {"name": reference["name"], "version": reference["version"]},
            ),
        ):
            request = {"command_id": str(uuid4()), "command": name, "args": args}
            expected = await run._read_request(request)
            response = await maintenance._execute(request)
            self.assertEqual(response["result"], "success")
            self.assertEqual(response["data"], expected)
