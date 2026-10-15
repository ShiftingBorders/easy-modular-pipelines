"""Test-owned writer instrumentation; no administrative commands enter EMP's IPC."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import sqlite3
import threading
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from core.journal.settings import _load_logging_settings
from core.journal.supervisor import discovery_path, standalone_directory
from core.models.journal_writer import JournalWriterSettings
from core.primitives.json_files import read_json, write_json


def _controlled_writer(
    config, directory, settings, instance, control, *, admin, observations
):
    from core.journal.writer import JournalWriter

    writer = JournalWriter(
        config, directory, settings, instance, control=control, managed=False
    )

    def observe(data):
        observations.send(data)
        failure = observations.recv()
        if failure is not None:
            raise failure

    def administer():
        patches = ExitStack()
        observed = None
        try:
            while True:
                command = admin.recv()
                try:
                    action = command["action"]
                    store = (
                        writer._reader if command.get("read_only") else writer._store
                    )
                    if action == "execute":
                        connection = store._connection
                        if command.get("read_only"):
                            connection = writer._reader._connection
                        cursor = connection.execute(
                            command["sql"], command.get("parameters", ())
                        )
                        result = cursor.fetchall()
                        cursor.close()
                    elif action == "space":
                        free = command["free"]

                        def disk_space(directory, free=free):
                            return SimpleNamespace(
                                free=free.get(str(directory), free["default"])
                                if isinstance(free, dict)
                                else free
                            )

                        observed = patches.enter_context(
                            patch(
                                "core.journal.storage.shutil.disk_usage",
                                side_effect=disk_space,
                            )
                        )
                        result = None
                    elif action == "method":
                        arguments = list(command.get("arguments", ()))
                        if command.get("connection"):
                            arguments.append(store._connection)
                        result = getattr(store, command["name"])(*arguments)
                    elif action == "boundary":
                        from core.journal.records import _read_boundary

                        result = _read_boundary(store._connection)
                    elif action == "transaction":
                        result = store._connection.in_transaction
                    elif action == "dump":
                        result = list(store._connection.iterdump())
                    elif action == "patch_storage":
                        patches.enter_context(
                            patch(
                                "core.journal.storage." + command["name"],
                                command["value"],
                            )
                        )
                        result = None
                    elif action == "method_failure":
                        patches.enter_context(
                            patch.object(
                                store, command["name"], side_effect=command["error"]
                            )
                        )
                        result = None
                    elif action == "snapshot_boundary":
                        original = writer._reader._validate_snapshot_source

                        def validate(source, original=original):
                            observe(None)
                            return original(source)

                        patches.enter_context(
                            patch.object(
                                writer._reader, "_validate_snapshot_source", validate
                            )
                        )
                        result = None
                    elif action == "backup_progress":
                        original_connect = sqlite3.connect

                        class ObservedConnection(sqlite3.Connection):
                            def backup(self, destination, **options):
                                return super().backup(
                                    destination,
                                    progress=lambda status, remaining, total: observe(
                                        (status, remaining, total)
                                    ),
                                    **options,
                                )

                        def connect(*args, original_connect=original_connect, **kwargs):
                            return original_connect(
                                *args, **kwargs, factory=ObservedConnection
                            )

                        patches.enter_context(patch.object(sqlite3, "connect", connect))
                        result = None
                    elif action == "publication_failure":
                        original_rename = Path.rename
                        failure = command["error"]

                        def rename(
                            path,
                            target,
                            failure=failure,
                            original_rename=original_rename,
                        ):
                            if path.name == "manifest.json.part":
                                raise failure
                            return original_rename(path, target)

                        patches.enter_context(patch.object(Path, "rename", rename))
                        result = None
                    elif action == "failure":
                        observed = Mock(wraps=store._connection)
                        getattr(observed, command["method"]).side_effect = command[
                            "error"
                        ]
                        patches.enter_context(
                            patch.object(store, "_connection", observed)
                        )
                        result = None
                    elif action == "calls":
                        result = [call.args for call in observed.call_args_list]
                    elif action == "restore":
                        patches.close()
                        result = None
                    elif action == "close_store":
                        store._report_local_failures = True
                        try:
                            store.close()
                        finally:
                            store._report_local_failures = False
                        result = None
                    else:
                        raise ValueError(action)
                    admin.send((True, result))
                except BaseException as error:  # noqa: BLE001 - Return injected interruptions to the parent test.
                    # Exception causes are not retained by pickle.
                    admin.send((False, error, error.__cause__))
        except (EOFError, OSError):
            pass
        finally:
            patches.close()
            admin.close()

    threading.Thread(target=administer, daemon=True).start()
    asyncio.run(writer.serve())


class WriterOwner:
    """Expose fixture-only SQL faults in the process that actually owns SQLite."""

    def __init__(self, config):
        self.config = config
        context = multiprocessing.get_context("spawn")
        self.parent, self.child = context.Pipe()
        self.control, child_control = context.Pipe()
        self.observations, child_observations = context.Pipe()
        configuration, _ = _load_logging_settings(config)
        self.directory = standalone_directory(configuration.db_path)
        self.database = configuration.db_path
        self.process = context.Process(
            target=_controlled_writer,
            args=(
                config,
                self.directory,
                JournalWriterSettings(),
                "test-writer",
                child_control,
            ),
            kwargs={"admin": self.child, "observations": child_observations},
        )

    def open(self):
        self.process.start()
        if not self.control.poll(15):
            raise TimeoutError("Fixture writer did not start.")
        status = json.loads(self.control.recv_bytes())
        if status["state"] != "ready":
            raise RuntimeError(status)
        self._stop_reports = threading.Event()

        def drain_reports():
            try:
                while not self._stop_reports.is_set():
                    if self.control.poll(0.1):
                        self.control.recv_bytes()
            except (EOFError, OSError):
                pass

        self._reports = threading.Thread(target=drain_reports, daemon=True)
        self._reports.start()
        write_json(
            discovery_path(self.database),
            {
                "runtime_directory": str(self.directory),
                "managed": False,
            },
        )
        document = read_json(self.config)
        identity = status["journal"]
        document["logging"].update(
            open_mode="existing",
            expected_journal={
                key: identity[key] for key in ("journal_id", "generation")
            },
        )
        write_json(self.config, document)
        # Keep the caller's path/context so reopen tests still exercise changes
        # to their original configuration file.
        return self

    def call(self, action, **values):
        self.parent.send({"action": action, **values})
        if not self.parent.poll(10):
            raise TimeoutError("Writer fixture did not answer its administration pipe.")
        reply = self.parent.recv()
        if not reply[0]:
            error = reply[1]
            error.__cause__ = reply[2]
            raise error
        return reply[1]

    def execute(self, sql, parameters=(), *, read_only=False):
        return self.call("execute", sql=sql, parameters=parameters, read_only=read_only)

    @contextmanager
    def fault(self, action, **values):
        self.call(action, **values)
        try:
            yield
        finally:
            self.call("restore")

    @contextmanager
    def observe(self, action, callback):
        stop = threading.Event()
        errors = []

        def listen():
            while not stop.is_set():
                if not self.observations.poll(0.1):
                    continue
                data = self.observations.recv()
                try:
                    callback(data)
                except BaseException as error:  # noqa: BLE001 - Preserve callback assertions across the fixture pipe.
                    errors.append(error)
                    self.observations.send(error)
                else:
                    self.observations.send(None)

        listener = threading.Thread(target=listen, daemon=True)
        self.call(action)
        listener.start()
        try:
            yield
        finally:
            self.call("restore")
            stop.set()
            listener.join(5)
        if errors:
            raise errors[0]

    @contextmanager
    def disk_space(self, free):
        self.call("space", free=free)
        calls = []
        try:
            yield calls
        finally:
            calls.extend(self.call("calls"))
            self.call("restore")

    @contextmanager
    def connection_failure(self, method, error, *, read_only=False):
        self.call("failure", method=method, error=error, read_only=read_only)
        try:
            yield
        finally:
            self.call("restore")

    def close(self):
        try:
            if self.process.is_alive():
                self.control.send_bytes(b'{"command":"stop"}')
                self.process.join(3)
                if self.process.is_alive():
                    self.process.terminate()
                    self.process.join(5)
        finally:
            self._stop_reports.set()
            self._reports.join(2)
            self.parent.close()
            self.child.close()
            self.control.close()
            self.observations.close()


async def run_crash_writer(mode, config, arguments):
    """Run the actual writer at the PID controlled by the existing crash fixture."""
    from core.journal.logger import OperationLogger
    from core.journal.writer import JournalWriter
    from tests.helpers.logging_process import announce, checkpoint

    configuration, context = _load_logging_settings(config)
    directory = standalone_directory(configuration.db_path)
    writer = JournalWriter(
        config, directory, configuration.writer_settings, uuid4().hex, managed=False
    )
    serving = asyncio.create_task(writer.serve())
    try:
        while writer._server is None:
            if serving.done():
                serving.result()
            await asyncio.sleep(0.01)
        write_json(
            discovery_path(configuration.db_path),
            {
                "runtime_directory": str(directory),
                "managed": False,
            },
        )
        client_config = config.parent / "crash-client.json"
        values = configuration.model_dump(mode="json")
        values.update(
            open_mode="existing",
            expected_journal={
                key: writer._journal_info[key] for key in ("journal_id", "generation")
            },
        )
        write_json(
            client_config,
            {"logging": values, "operation_context": context.model_dump()},
        )
        logger = OperationLogger(client_config)
        await asyncio.to_thread(logger.open)
        try:
            if mode in ("before_insert", "during_insert", "after_commit"):
                identifiers = [
                    await asyncio.to_thread(
                        logger.record_event, "test.prefix", {"number": index}
                    )
                    for index in range(20)
                ]
                announce({"confirmed_ids": identifiers})
                if mode == "before_insert":
                    await asyncio.to_thread(checkpoint, {"before_insert": True})
                if mode == "during_insert":
                    inserting = False

                    def trace(sql):
                        nonlocal inserting
                        inserting = sql.startswith("INSERT INTO events")

                    def progress():
                        if inserting:
                            checkpoint({"during_insert": True})
                        return 0

                    writer._store._connection.set_trace_callback(trace)
                    writer._store._connection.set_progress_handler(progress, 1)
                event_id = await asyncio.to_thread(
                    logger.record_event, "test.last", {"payload": "x" * 10000}
                )
                await asyncio.to_thread(checkpoint, {"confirmed_last": event_id})
                return
            if mode == "snapshot_crash":
                original_rename = Path.rename

                def after_copy(path, target):
                    result = original_rename(path, target)
                    if path.name == "journal.sqlite.part":
                        checkpoint({"phase": "database_copied"})
                    return result

                with patch.object(Path, "rename", after_copy):
                    await asyncio.to_thread(
                        logger.export_snapshot, Path(arguments[0]), min_free_bytes=0
                    )
                return
            if mode == "result_before_commit":

                def before_index(sql):
                    if sql.startswith("INSERT INTO command_results"):
                        checkpoint({"phase": "result_uncommitted"})

                writer._store._connection.set_trace_callback(before_index)
            event_id = await asyncio.to_thread(
                logger.record_command_result,
                arguments[0],
                {"origin": "runner"},
                author="runner",
                outcome="failed",
            )
            await asyncio.to_thread(
                checkpoint, {"phase": "result_committed", "event_id": event_id}
            )
        finally:
            await asyncio.to_thread(logger.close)
    finally:
        writer._closing.set()
        await serving
