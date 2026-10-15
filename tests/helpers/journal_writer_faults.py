"""Test-only SQLite gates installed in the real spawned journal writer.

The parent wraps multiprocessing's process entry point rather than adding fault
switches to EMP. A gate is published at a SQL COMMIT boundary; the test can then
terminate the exact owned process or wait for the real supervisor to replace it.
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing.process
import os
import sqlite3
import threading
import time
from collections.abc import Callable
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit
from urllib.request import url2pathname
from uuid import uuid4

from core.journal.storage import SQLiteEventStore
from core.primitives.json_files import read_json, write_json
from core.primitives.processes import process_identity
from tests.helpers.dag import process_running, terminate_owned, wait_until


class CommitGate:
    """A parent-owned fault instruction with durable, one-use occurrence claims."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    async def entered(self, occurrence: int = 0, *, timeout: float = 30) -> dict:
        path = self.directory / f"entered-{occurrence}.json"
        await wait_until(path.is_file, timeout=timeout)
        return read_json(path)

    def release(self) -> None:
        (self.directory / "release").touch(exist_ok=True)


class WriterFaults:
    """Own gates and spawned writer handles; never signal an unverified PID."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.processes: list[multiprocessing.process.BaseProcess] = []
        self._by_pid: dict[int, multiprocessing.process.BaseProcess] = {}
        self.gates: list[CommitGate] = []
        self._patch = None

    def open(self) -> None:
        self.directory.mkdir(parents=True)
        original_start = multiprocessing.process.BaseProcess.start

        def start(process):
            target = process._target
            is_writer = getattr(target, "__module__", "").startswith("core.journal.")
            if is_writer:
                process._target = instrument_writer
                process._args = (
                    target,
                    process._args,
                    process._kwargs,
                    self.directory,
                )
                process._kwargs = {}
                self.processes.append(process)
            result = original_start(process)
            if is_writer:
                self._by_pid[process.pid] = process
            return result

        self._patch = patch.object(multiprocessing.process.BaseProcess, "start", start)
        self._patch.start()

    def arm(
        self,
        event_type: str,
        phase: str,
        *,
        occurrences: int = 1,
        action: str = "wait",
        request_id: str | None = None,
        stage_id: str | None = None,
    ) -> CommitGate:
        directory = self.directory / uuid4().hex
        directory.mkdir()
        gate = CommitGate(directory)
        self.gates.append(gate)
        write_json(
            self.directory / "armed.json",
            {
                "directory": str(directory),
                "event_type": event_type,
                "phase": phase,
                "occurrences": occurrences,
                "action": action,
                "request_id": request_id,
                "stage_id": stage_id,
            },
        )
        return gate

    def release_all(self) -> None:
        for gate in self.gates:
            gate.release()
        self.clear_storage_failure()

    def clear_storage_failure(self) -> None:
        (self.directory / "open-failure.json").unlink(missing_ok=True)

    def hold_start(self) -> CommitGate:
        directory = self.directory / uuid4().hex
        directory.mkdir()
        gate = CommitGate(directory)
        self.gates.append(gate)
        write_json(self.directory / "startup.json", {"directory": str(directory)})
        return gate

    def hold_quick_check(self) -> CommitGate:
        """Hold one real quick_check after an already running writer is ready."""
        directory = self.directory / uuid4().hex
        directory.mkdir()
        gate = CommitGate(directory)
        self.gates.append(gate)
        write_json(self.directory / "quick-check.json", {"directory": str(directory)})
        return gate

    def refuse_storage(self, database: Path, *, replacement_too: bool = False) -> None:
        write_json(
            self.directory / "open-failure.json",
            {"database": str(database), "replacement_too": replacement_too},
        )

    async def kill(self, identity: dict) -> None:
        if identity["pid"] == os.getpid():
            raise AssertionError("The writer must not be the test/client process.")
        owned = self._by_pid.get(identity["pid"])
        if owned is None:
            raise AssertionError("Refusing to kill a process outside this fixture.")
        terminate_owned(identity)
        await wait_until(lambda: not process_running(identity["pid"]), timeout=10)
        try:
            await asyncio.to_thread(owned.join, 5)
        except ValueError:
            # The supervisor can reap and close the handle before this join.
            pass

    def alive(self) -> list[int]:
        result = []
        for process in self.processes:
            try:
                if process.is_alive():
                    result.append(process.pid)
            except ValueError:
                # The production supervisor may already have reaped/closed it.
                continue
        return result

    async def close(self) -> None:
        self.release_all()
        if self._patch is not None:
            self._patch.stop()
        for process in self.processes:
            try:
                if process.is_alive():
                    identity_path = self.directory / f"writer-{process.pid}.json"
                    if not identity_path.is_file():
                        raise AssertionError(
                            "Writer started without its fixture handshake."
                        )
                    terminate_owned(read_json(identity_path)["process"])
                await asyncio.to_thread(process.join, 5)
                if process.is_alive():
                    raise AssertionError("Owned test writer did not exit.")
                process.close()
            except ValueError:
                # An already closed multiprocessing handle has been reaped.
                continue


def _claim_gate(control: Path, event: dict, phase: str) -> tuple[Path, dict] | None:
    try:
        fault = read_json(control / "armed.json")
    except FileNotFoundError:
        return None
    if fault["phase"] != phase or event["event_type"] != fault["event_type"]:
        return None
    if (
        fault["request_id"] is not None
        and event["data"].get("request_id") != fault["request_id"]
    ):
        return None
    if (
        fault["stage_id"] is not None
        and event["context"].get("stage_id") != fault["stage_id"]
    ):
        return None
    directory = Path(fault["directory"])
    if (directory / "release").exists():
        return None
    for occurrence in range(fault["occurrences"]):
        try:
            with (directory / f"claim-{occurrence}").open("x", encoding="utf-8"):
                pass
        except FileExistsError:
            continue
        write_json(
            directory / f"entered-{occurrence}.json",
            {"phase": phase, "process": process_identity(os.getpid()), "event": event},
        )
        return directory, fault
    return None


def _at_commit(control: Path, event: dict | None, phase: str) -> None:
    if event is None:
        return
    claimed = _claim_gate(control, event, phase)
    if claimed is None:
        return
    directory, fault = claimed
    if fault["action"] == "raise":
        raise sqlite3.IntegrityError("Injected writer request failure")
    _wait_release(directory)


def _wait_release(directory: Path) -> None:
    deadline = time.monotonic() + 60
    while not (directory / "release").exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(
                "Writer fixture gate was not released within 60 seconds."
            )
        time.sleep(0.01)


@contextmanager
def instrument_sqlite(control: Path):
    """Retain real SQL transactions and gate only the selected event's COMMIT.

    The command-result wrapper also covers confirmation of an existing canonical
    event, where the transaction inserts no row into events.
    """
    original_connect = sqlite3.connect
    original_command = SQLiteEventStore.append_command_result
    pending = threading.local()

    class ObservedConnection(sqlite3.Connection):
        _observed_event: dict | None = None

        def execute(self, sql, parameters=()):
            statement = " ".join(sql.upper().split())
            if statement == "PRAGMA QUICK_CHECK":
                instruction = control / "quick-check.json"
                if instruction.is_file():
                    directory = Path(read_json(instruction)["directory"])
                    try:
                        with (directory / "claim").open("x", encoding="utf-8"):
                            pass
                    except FileExistsError:
                        pass
                    else:
                        write_json(
                            directory / "entered-0.json",
                            {
                                "process": process_identity(os.getpid()),
                            },
                        )
                        _wait_release(directory)
            if statement.startswith("INSERT INTO EVENTS "):
                self._observed_event = json.loads(parameters[-1])
            event = self._observed_event or getattr(pending, "event", None)
            if statement == "COMMIT":
                _at_commit(control, event, "before_commit")
            result = super().execute(sql, parameters)
            if statement == "COMMIT":
                _at_commit(control, event, "after_commit")
            if statement in ("COMMIT", "ROLLBACK"):
                self._observed_event = None
            return result

    def connect(*args, **kwargs):
        failure_path = control / "open-failure.json"
        if failure_path.is_file():
            failure = read_json(failure_path)
            value = str(args[0])
            path = (
                Path(url2pathname(urlsplit(value).path))
                if value.startswith("file:")
                else Path(value)
            )
            old = Path(failure["database"])
            if path == old or failure["replacement_too"] and path.parent == old.parent:
                raise PermissionError("Injected journal open failure")
        if "factory" in kwargs:
            raise AssertionError(
                "Update the fixture for the explicitly selected SQLite factory."
            )
        return original_connect(*args, **kwargs, factory=ObservedConnection)

    def append_command(store, event):
        previous = getattr(pending, "event", None)
        pending.event = event
        try:
            return original_command(store, event)
        finally:
            pending.event = previous

    with ExitStack() as stack:
        stack.enter_context(patch.object(sqlite3, "connect", side_effect=connect))
        stack.enter_context(
            patch.object(SQLiteEventStore, "append_command_result", append_command)
        )
        yield


def instrument_writer(
    target: Callable, args: tuple, kwargs: dict, control: Path
) -> None:
    """Spawn-compatible wrapper; no child process receives an open logger."""
    write_json(
        control / f"writer-{os.getpid()}.json",
        {"process": process_identity(os.getpid())},
    )
    startup = control / "startup.json"
    if startup.is_file():
        directory = Path(read_json(startup)["directory"])
        if not (directory / "release").exists():
            write_json(
                directory / "entered-0.json", {"process": process_identity(os.getpid())}
            )
            _wait_release(directory)
    with instrument_sqlite(control):
        target(*args, **kwargs)


def exercise_storage_probe(database: Path, event: dict) -> None:
    """Validate fixture gates against existing storage before the writer exists."""
    from tests.helpers.logging_process import make_store

    store = make_store(database, open_mode="existing")
    try:
        store.open()
        if event["event_type"] == "command.result":
            store.append_command_result(event)
        else:
            store.append(event)
    finally:
        store.close()
