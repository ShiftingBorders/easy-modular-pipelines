"""Real files/SQLite/processes and HTTP-only package storage for the approved plan."""

from __future__ import annotations

import asyncio
import ctypes
import json
import multiprocessing
import os
import shutil
import signal
import tempfile
import time
from email import policy
from email.parser import BytesParser
from pathlib import Path
from queue import Empty
from unittest.mock import patch
from uuid import uuid4

import httpx
import yaml

from core.experimentcontroller import ExperimentController
from core.hashdb import HashDB
from core.modulemanager import ModuleManager
from core.runner_utils.experimentrunner import ExperimentRunner
from core.runner_utils.runtimeio import process_identity, read_json
from core.seaweed import SeaweedDB

REPOSITORY = Path(__file__).resolve().parents[2]
TEMP_ROOT = REPOSITORY / ".artifacts" / "tmp" / "dag-tests"


async def wait_until(predicate, *, timeout: float = 10):
    async with asyncio.timeout(timeout):
        while not (value := predicate()):
            await asyncio.sleep(0.01)
        return value


def process_running(pid: int) -> bool:
    if os.name == "nt":
        from ctypes import wintypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel.OpenProcess(0x100000, False, pid)
        if not handle:
            if ctypes.get_last_error() in (87, 1168):
                return False
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            return kernel.WaitForSingleObject(handle, 0) == 258
        finally:
            kernel.CloseHandle(handle)
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


def terminate_owned(identity: dict) -> None:
    pid = identity["pid"]
    if not process_running(pid) or process_identity(pid) != identity:
        return
    if os.name == "nt":
        from ctypes import wintypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [
            ctypes.POINTER(wintypes.FILETIME)
        ] * 4
        kernel.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        handle = kernel.OpenProcess(0x1001, False, pid)
        if not handle:
            return
        try:
            times = [wintypes.FILETIME() for _ in range(4)]
            if kernel.GetProcessTimes(
                handle, *(ctypes.byref(value) for value in times)
            ):
                created = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
                if created == identity["created_at_os"]:
                    kernel.TerminateProcess(handle, 99)
        finally:
            kernel.CloseHandle(handle)
    else:
        descriptor = os.pidfd_open(pid)
        try:
            if process_identity(pid) == identity:
                signal.pidfd_send_signal(descriptor, signal.SIGKILL)
        finally:
            os.close(descriptor)


class DagWorkspace:
    def __init__(self) -> None:
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=TEMP_ROOT)
        self.root = Path(self.temporary.name).resolve()
        self.objects = {}
        self.gates = []
        (self.root / "modules").mkdir()
        (self.root / "schema.json").write_text(
            json.dumps(
                {
                    "Mname": "VARCHAR(255) NOT NULL",
                    "MVersion": "VARCHAR(255) NOT NULL",
                    "MHash": "VARCHAR(255) NOT NULL",
                }
            ),
            encoding="utf-8",
        )
        self.hash_config = self.root / "hashes.json"
        self.hash_config.write_text(
            json.dumps({"schema_path": "schema.json", "db_path": "hashes.db"}),
            encoding="utf-8",
        )
        self.hashes = HashDB(self.hash_config)
        client = httpx.Client(
            base_url="http://filer.test", transport=httpx.MockTransport(self._filer)
        )
        with patch("core.seaweed.httpx.Client", return_value=client):
            self.archives = SeaweedDB("http://filer.test")
        self.manager = ModuleManager(
            self.root / "modules", self.hashes, self.archives, self.root / "work"
        )

    def _filer(self, request):
        path = request.url.path
        if request.method == "HEAD":
            return httpx.Response(200 if path in self.objects else 404)
        if request.method == "POST":
            message = BytesParser(policy=policy.default).parsebytes(
                f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode()
                + request.read()
            )
            self.objects[path] = next(message.iter_parts()).get_payload(decode=True)
            return httpx.Response(201)
        if request.method == "GET":
            return (
                httpx.Response(200, content=self.objects[path])
                if path in self.objects
                else httpx.Response(404)
            )
        if request.method == "DELETE":
            return httpx.Response(
                204 if self.objects.pop(path, None) is not None else 404
            )
        raise AssertionError(f"Unexpected storage request: {request.method}")

    def module(
        self,
        name="worker",
        version="1",
        defaults=None,
        *,
        implementation="full",
        source=None,
    ) -> dict:
        directory = self.root / "modules" / name / version
        directory.mkdir(parents=True)
        shutil.copy2(
            source or Path(__file__).with_name("dag_stage.py"), directory / "main.py"
        )
        commands = {"start": ["python", "-B", "main.py"]}
        (directory / "module.yaml").write_text(
            yaml.safe_dump(
                {
                    "schema_version": 2,
                    "name": name,
                    "version": version,
                    "role": "stage",
                    "implementation": implementation,
                    "commands": commands,
                    "defaults": {} if defaults is None else defaults,
                }
            ),
            encoding="utf-8",
        )
        self.manager.register_module(name, version, directory)
        return {
            "name": name,
            "version": version,
            "hash": self.hashes.get_module_hash(name, version),
        }

    def stage(
        self, module=None, *, settings=None, retries=0, on_exhausted="pause", timeout=10
    ) -> dict:
        return {
            "stage_id": str(uuid4()),
            "module": self.module() if module is None else module,
            "settings": {} if settings is None else settings,
            "timeout_seconds": timeout,
            "errors": {
                "retries": retries,
                "retry_delay_seconds": 0.01,
                "on_exhausted": on_exhausted,
            },
        }

    def template(
        self, stages=None, *, cycles=1, keep_attempts=1, resources=None
    ) -> dict:
        return {
            "schema_version": 2,
            "name": "test-dag",
            "cycles": cycles,
            "keep_attempts": keep_attempts,
            "start_timeout": 2,
            "runner_timeout_margin_seconds": 0.2,
            "stages": [self.stage()] if stages is None else stages,
            "services": [],
            "resources": [] if resources is None else resources,
            "unknown_state": {
                "timeout_seconds": 3,
                "on_timeout": "stop",
                "recovery_limit": 3,
                "on_recovery_limit": "stop",
            },
            "snapshots": {"mode": "off", "keep": 3},
            "storage": {"min_snapshot_free_bytes": 0},
            "logging": {
                "busy_timeout_seconds": 1,
                "max_event_bytes": None,
                "min_free_bytes": 0,
                "filtered_refresh_interval_seconds": 1,
            },
        }

    def write_template(self, template: dict, name="experiment.yaml") -> Path:
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(template, sort_keys=False), encoding="utf-8")
        return path

    def gate(self) -> Path:
        path = self.root / f"release-{uuid4()}"
        self.gates.append(path)
        return path

    def events(self, runner: ExperimentRunner) -> list[dict]:
        page = runner._journal.client.read_events(limit=1000)
        return [entry["event"] for entry in page["events"]]

    def close(self) -> None:
        self.archives.close()
        self.hashes.close_connection()
        if (
            self.root.is_symlink()
            or self.root.is_junction()
            or not self.root.resolve().is_relative_to(TEMP_ROOT.resolve())
        ):
            raise ValueError("Refusing to clean a workspace outside the test root.")
        for attempt in range(10):
            try:
                self.temporary.cleanup()
                break
            except OSError as error:
                # Windows may briefly retain delete-pending files after their last
                # handle closes. Do not hide persistent or unrelated cleanup errors.
                if getattr(error, "winerror", None) not in (5, 32, 145) or attempt == 9:
                    raise
                time.sleep(0.1)


class DagSession:
    def __init__(self, workspace: DagWorkspace, *, response_capacity=0) -> None:
        self.workspace = workspace
        context = multiprocessing.get_context("spawn")
        self.requests = context.Queue()
        self.responses = context.Queue(response_capacity)
        self.runner = ExperimentRunner(workspace.root, workspace.manager)
        self.controller = ExperimentController(
            workspace.root, self.runner, self.requests, self.responses
        )
        self.pending = {}
        self.received = []
        self.controller_task = None
        self.reader_task = None
        self.closed = False

    async def start(self) -> None:
        self.controller_task = asyncio.create_task(self.controller.serve())
        self.reader_task = asyncio.create_task(self._read())

    async def _read(self) -> None:
        while True:
            try:
                response = await asyncio.to_thread(self.responses.get, True, 0.1)
            except Empty:
                continue
            self.received.append(response)
            future = self.pending.get(response["command_id"])
            if future is not None and not future.done():
                future.set_result(response)

    def post(self, command: str, args=None, target=None):
        command_id = str(uuid4())
        future = asyncio.get_running_loop().create_future()
        self.pending[command_id] = future
        message = {
            "api_version": 1,
            "command_id": command_id,
            "command": command,
            "args": {} if args is None else args,
        }
        if target is not None:
            message["target"] = target
        self.requests.put_nowait(message)
        return future

    def chain(self, commands: list[dict]) -> list[asyncio.Future]:
        futures = []
        entries = []
        for command in commands:
            command_id = str(uuid4())
            future = asyncio.get_running_loop().create_future()
            self.pending[command_id] = future
            futures.append(future)
            entries.append({**command, "command_id": command_id})
        self.requests.put_nowait(
            {"api_version": 1, "chain_id": str(uuid4()), "commands": entries}
        )
        return futures

    async def send(self, command, args=None, target=None):
        return await asyncio.wait_for(
            asyncio.shield(self.post(command, args, target)), 15
        )

    async def launch(self, template: dict, *, paused=True) -> dict:
        result = await self.send(
            "run",
            {
                "template_path": str(self.workspace.write_template(template)),
                "delayed_start": paused,
            },
        )
        if result["result"] != "success":
            raise AssertionError(result)
        await wait_until(lambda: self.runner.get_state()["phase"] != "starting")
        return result

    async def ready_attempt(self) -> tuple[Path, dict]:
        async with asyncio.timeout(10):
            while True:
                state = self.runner._state
                attempt = None if state is None else state.active_attempt
                if attempt is not None:
                    directory = attempt.artifacts_directory
                    try:
                        ready = await asyncio.to_thread(read_json, directory / "ready.json")
                    except FileNotFoundError:
                        pass
                    else:
                        return directory, ready
                await asyncio.sleep(0.01)

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        cleanup_error = None
        for gate in self.workspace.gates:
            gate.touch(exist_ok=True)
        try:
            async with asyncio.timeout(8):
                await self.runner.stop()
        except (Exception, asyncio.CancelledError) as error:  # noqa: BLE001 - Cleanup still closes all owned resources.
            cleanup_error = error
        await self.controller.close()
        if self.reader_task is not None:
            self.reader_task.cancel()
        await asyncio.gather(
            *(
                task
                for task in (self.controller_task, self.reader_task)
                if task is not None
            ),
            return_exceptions=True,
        )
        await self.runner.close()
        identities = []
        for path in self.workspace.root.glob(
            "experiments/*/shared_artifacts/**/process.json"
        ):
            record = json.loads(path.read_text(encoding="utf-8"))
            identities.extend(
                record[key] for key in ("stage", "executor") if record.get(key)
            )
        for identity in identities:
            terminate_owned(identity)
        for identity in identities:
            await wait_until(
                lambda identity=identity: not process_running(identity["pid"]),
                timeout=5,
            )
        process = self.runner._stages._process
        if process is not None:
            await asyncio.to_thread(process.wait, timeout=5)
        for queue in (self.requests, self.responses):
            queue.cancel_join_thread()
            queue.close()
        if cleanup_error is not None:
            raise cleanup_error
