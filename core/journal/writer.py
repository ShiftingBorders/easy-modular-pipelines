"""One process owns a SQLite journal; network/control stay outside the SQL thread."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import secrets
import sqlite3
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from core.journal.events import (
    JournalGenerationChanged,
    LoggingStateError,
    LoggingStorageError,
)
from core.journal.settings import _load_logging_settings
from core.journal.startup_diagnostics import StartupDiagnostics
from core.journal.storage import SQLiteEventStore
from core.models.journal_writer import (
    JournalAppend,
    JournalExport,
    JournalExportReceipt,
    JournalRequest,
    JournalWriterSettings,
    WriterRecoveryClock,
)
from core.primitives.file_lock import _open_file_lock
from core.primitives.framing import encode_frame, read_frame
from core.primitives.json_files import read_json, write_json
from core.primitives.processes import process_identity


def writer_error(error: BaseException) -> dict:
    cause = error.__cause__
    result = {
        "type": type(error).__name__,
        "message": str(error),
        "traceback": "".join(traceback.format_exception(error)),
        "cause_type": None if cause is None else type(cause).__name__,
        "cause_message": None if cause is None else str(cause),
        "journal_failed": getattr(error, "journal_failed", False),
    }
    for name in (
        "errno",
        "winerror",
        "filename",
        "filename2",
        "sqlite_errorcode",
        "sqlite_errorname",
    ):
        value = getattr(cause if cause is not None else error, name, None)
        if value is not None:
            result[name] = str(value) if isinstance(value, Path) else value
    if isinstance(error, JournalGenerationChanged):
        result.update(expected=error.expected, actual=error.actual)
    return result


class JournalWriter:
    """Own a database, a bounded request queue and a separate liveness channel."""

    def __init__(
        self,
        config_path: Path,
        runtime_directory: Path,
        settings: JournalWriterSettings,
        instance_id: str,
        *,
        control=None,
        managed: bool = True,
    ) -> None:
        self.config_path = config_path
        self.directory = runtime_directory
        self.settings = settings
        self.instance_id = instance_id
        self.control = control
        self.managed = managed
        self._queue = asyncio.Queue(maxsize=settings.queue_capacity)
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="journal-sql"
        )
        self._read_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="journal-read"
        )
        self._readers_waiting = 0
        self._active_read: tuple[str, float, str | None] | None = None
        self._startup = StartupDiagnostics(self.directory / "startup.jsonl", {})
        self._store = None
        self._reader = None
        self._server = None
        self._clients = set()
        self._tasks = set()
        self._closing = asyncio.Event()
        self._busy = False
        self._progress = time.monotonic()
        self._last_request = self._progress
        self._storage_error = None
        self._lease = None
        self._identity = None
        self._token = secrets.token_urlsafe(48)

    async def serve(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        consumer = monitor = None
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self._executor, self._open_storage)
            self._identity = process_identity(os.getpid())
            self._server = await asyncio.start_server(
                self._handle_client, "127.0.0.1", 0
            )
            self._publish_endpoint()
            try:
                self._send_control(self.status())
            except (EOFError, BrokenPipeError, OSError):
                self._detach_owner()
            consumer = asyncio.create_task(self._consume())
            monitor = asyncio.create_task(self._monitor(consumer))
            await self._closing.wait()
        except Exception as error:
            document = {"state": "open_failed", "error": writer_error(error)}
            write_json(self.directory / "startup-error.json", document)
            self._send_control(document)
            raise
        finally:
            await self._close(consumer, monitor)

    def _open_storage(self) -> None:
        from core.journal.supervisor import standalone_directory

        started = time.monotonic()
        self._startup.record("writer.open.begin", writer_instance_id=self.instance_id)
        configuration, context = _load_logging_settings(self.config_path)
        try:
            self._lease = _open_file_lock(
                standalone_directory(configuration.db_path) / "writer.lock"
            )
        except OSError as error:
            raise LoggingStateError(
                "Cannot acquire exclusive ownership of this journal writer."
            ) from error
        self._configuration = configuration
        self._context = context
        self._store = SQLiteEventStore._from_settings(configuration, context, False)
        self._store._startup_diagnostics = self._startup
        self._store._report_local_failures = False
        self._store.open()
        self._journal_info = self._store.get_journal_info()
        self._store._connection.set_progress_handler(self._sql_progress, 10000)
        reader_settings = configuration.model_copy(
            update={
                "open_mode": "existing",
                "expected_journal": {
                    key: self._journal_info[key] for key in ("journal_id", "generation")
                },
            }
        )
        self._reader = SQLiteEventStore._from_settings(reader_settings, context, True)
        self._reader._startup_diagnostics = self._startup
        self._reader.open()
        self._startup.record(
            "writer.open.end",
            writer_instance_id=self.instance_id,
            duration_seconds=time.monotonic() - started,
        )
        notice = self.directory / "replacement.json"
        if notice.exists():
            document = read_json(notice)
            if Path(document["target_path"]) != self._configuration.db_path:
                return
            event = {
                "schema_version": 2,
                "event_id": document["event_id"],
                "producer_instance_id": document["producer_instance_id"],
                "sequence_number": 1,
                "occurred_at": document["occurred_at"],
                "event_type": "journal.created_after_failure",
                "context": document["context"],
                "operation_id": None,
                "data": {
                    **document["data"],
                    "new_path": str(self._configuration.db_path),
                    "new_journal": self._journal_info,
                },
            }
            self._store._append_replayed(event)

    def _sql_progress(self) -> int:
        self._progress = time.monotonic()
        return 0

    def _publish_endpoint(self) -> None:
        token_path = self.directory / "token"
        token_path.write_text(self._token, encoding="utf-8")
        with suppress(OSError):
            token_path.chmod(0o600)
        document = {
            "protocol_version": 1,
            "writer_instance_id": self.instance_id,
            "process": self._identity,
            "managed": self.managed,
            "db_path": str(self._configuration.db_path),
            "journal": {
                key: self._journal_info[key] for key in ("journal_id", "generation")
            },
            "endpoint": {
                "host": "127.0.0.1",
                "port": self._server.sockets[0].getsockname()[1],
                "token_file": str(token_path),
            },
        }
        write_json(self.directory / "endpoint.json", document)
        if not self.managed:
            status_path = self.directory / "status.json"
            previous = read_json(status_path) if status_path.exists() else {}
            clock = WriterRecoveryClock.model_validate(previous)
            write_json(
                status_path,
                {
                    **previous,
                    **self.status(),
                    "clock_id": clock.clock_id or uuid4().hex,
                    "recovery_started_at": None,
                    "recovery_seconds": clock.elapsed(time.monotonic()),
                },
            )

    def _detach_owner(self) -> None:
        """Let surviving participants finish under the standalone idle policy."""
        from core.journal.supervisor import discovery_path

        if self.control is not None:
            self.control.close()
            self.control = None
        self.managed = False
        self._last_request = time.monotonic()
        self._publish_endpoint()
        write_json(
            discovery_path(self._configuration.db_path),
            {
                "runtime_directory": str(self.directory),
                "managed": False,
            },
        )

    def status(self) -> dict:
        active = self._active_read
        return {
            "state": "storage_unavailable" if self._storage_error else "ready",
            "process": self._identity,
            "writer_instance_id": self.instance_id,
            "queue_depth": self._queue.qsize(),
            "queue_capacity": self.settings.queue_capacity,
            "busy": self._busy,
            "read_queue_depth": max(0, self._readers_waiting - int(active is not None)),
            "active_read": None
            if active is None
            else {
                "command": active[0],
                "started_monotonic": active[1],
                "age_seconds": max(0, time.monotonic() - active[1]),
                "diagnostic_id": active[2],
            },
            "progress_at": self._progress,
            "db_path": str(self._configuration.db_path),
            "journal": self._journal_info,
            "error": self._storage_error,
        }

    def _send_control(self, document: dict) -> None:
        if self.control is not None:
            self.control.send_bytes(json.dumps(document).encode("utf-8"))

    async def _monitor(self, consumer: asyncio.Task) -> None:
        while not self._closing.is_set():
            if consumer.done():
                self._closing.set()
                if not consumer.cancelled() and consumer.exception() is not None:
                    self._storage_error = writer_error(consumer.exception())
                return
            if self.control is not None:
                try:
                    if self.control.poll():
                        command = json.loads(self.control.recv_bytes())
                        if command["command"] == "stop":
                            self._closing.set()
                    self._send_control(self.status())
                except (EOFError, BrokenPipeError, OSError):
                    if not self._closing.is_set():
                        try:
                            self._detach_owner()
                        except Exception as error:  # noqa: BLE001 - Failed ownership publication requires a fresh process.
                            self._closing.set()
                            self._storage_error = writer_error(error)
            if (
                not self.managed
                and not self._busy
                and not self._readers_waiting
                and self._queue.empty()
                and time.monotonic() - self._last_request
                >= self.settings.idle_timeout_seconds
            ):
                self._closing.set()
            await asyncio.sleep(self.settings.heartbeat_interval_seconds)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        if len(self._clients) >= self.settings.max_connections:
            writer.close()
            return
        self._clients.add(writer)
        self._tasks.add(task)
        try:
            while not self._closing.is_set():
                message = await read_frame(reader, validate=False)
                request = JournalRequest.model_validate(message)
                if not secrets.compare_digest(
                    request.token.encode("utf-8"), self._token.encode("ascii")
                ):
                    return
                self._last_request = time.monotonic()
                if request.call.command == "status":
                    reply = {"ok": True, "value": self.status()}
                elif request.call.command == "shutdown_standalone":
                    if self.managed:
                        reply = {
                            "ok": False,
                            "error": writer_error(
                                LoggingStateError(
                                    "Only the EMP owner may stop this writer."
                                )
                            ),
                        }
                    else:
                        reply = {"ok": True, "value": None}
                        self._closing.set()
                elif request.call.command.startswith(
                    ("read_", "export_")
                ) or request.call.command in ("open", "get_journal_info"):
                    self._readers_waiting += 1
                    self._last_request = time.monotonic()
                    try:
                        reply = await asyncio.get_running_loop().run_in_executor(
                            self._read_executor,
                            self._read_with_timing,
                            request,
                            time.monotonic(),
                        )
                    finally:
                        self._readers_waiting -= 1
                        self._last_request = time.monotonic()
                else:
                    self._last_request = time.monotonic()
                    future = asyncio.get_running_loop().create_future()
                    await self._queue.put((request, future))
                    reply = await asyncio.shield(future)
                delivered = time.monotonic()
                writer.write(encode_frame(reply, validate=False))
                await writer.drain()
                if request.call.command == "open":
                    self._startup.record(
                        "writer.reply.sent",
                        command="open",
                        writer_instance_id=self.instance_id,
                        diagnostic_id=request.diagnostic_id,
                        duration_seconds=time.monotonic() - delivered,
                    )
        except (OSError, ValueError, asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception as error:  # noqa: BLE001 - Recover from infrastructure failures rather than leaving a broken worker advertised.
            self._closing.set()
            self._storage_error = writer_error(error)
        finally:
            self._clients.discard(writer)
            self._tasks.discard(task)
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    async def _consume(self) -> None:
        while True:
            request, future = await self._queue.get()
            self._busy = True
            self._progress = time.monotonic()
            try:
                result = await asyncio.get_running_loop().run_in_executor(
                    self._executor, self._apply, request
                )
                if not future.done():
                    future.set_result(result)
            finally:
                self._busy = False
                self._progress = self._last_request = time.monotonic()
                self._queue.task_done()

    def _apply(self, request: JournalRequest) -> dict:
        try:
            expected = (
                None
                if request.expected_journal is None
                else request.expected_journal.model_dump()
            )
            actual = {
                key: self._journal_info[key] for key in ("journal_id", "generation")
            }
            if expected is not None and expected != actual:
                raise JournalGenerationChanged(expected, actual)
            if request.read_only and isinstance(request.call, JournalAppend):
                raise LoggingStateError("This journal client is read-only.")
            if self._storage_error is not None:
                self._reopen_storage()
            if isinstance(request.call, JournalAppend):
                self._store._check_free_space(
                    self._store.db_path.parent, request.min_free_bytes
                )
            value = self._dispatch(request.call, self._store)
            return {"ok": True, "value": value}
        except BaseException as error:  # noqa: BLE001 - Isolate even an interrupted SQL request.
            retryable = False
            if getattr(error, "journal_failed", False):
                cause = error.__cause__
                retryable = self.managed and not isinstance(
                    cause, (sqlite3.IntegrityError, ValueError, TypeError)
                )
                try:
                    self._reopen_storage()
                except Exception as reopen_error:  # noqa: BLE001 - Supervisor handles storage recovery.
                    self._storage_error = writer_error(reopen_error)
                    retryable = self.managed
            return {"ok": False, "error": writer_error(error), "retryable": retryable}

    def _read_with_timing(self, request: JournalRequest, queued: float) -> dict:
        """Expose read progress independently of the write worker and its queue."""
        started = time.monotonic()
        wait = max(0, started - queued)
        command = request.call.command
        self._active_read = (command, started, request.diagnostic_id)
        detailed = command == "open" or command.startswith("export_") or wait >= 0.05
        if detailed:
            self._startup.record(
                "writer.read.started",
                command=command,
                queue_wait_seconds=wait,
                writer_instance_id=self.instance_id,
                diagnostic_id=request.diagnostic_id,
            )
        try:
            return self._apply_read(request)
        finally:
            duration = time.monotonic() - started
            if detailed or duration >= 0.05:
                self._startup.record(
                    "writer.read.finished",
                    command=command,
                    queue_wait_seconds=wait,
                    duration_seconds=duration,
                    writer_instance_id=self.instance_id,
                    diagnostic_id=request.diagnostic_id,
                )
            self._active_read = None

    def _apply_read(self, request: JournalRequest) -> dict:
        try:
            if request.call.command == "open":
                return {"ok": True, "value": self._open_client(request)}
            actual = {
                key: self._journal_info[key] for key in ("journal_id", "generation")
            }
            if (
                request.expected_journal is not None
                and request.expected_journal.model_dump() != actual
            ):
                raise JournalGenerationChanged(
                    request.expected_journal.model_dump(), actual
                )
            return {"ok": True, "value": self._dispatch(request.call, self._reader)}
        except BaseException as error:  # noqa: BLE001 - A read failure must not kill the server.
            return {"ok": False, "error": writer_error(error)}

    def _reopen_storage(self) -> None:
        self._store.close()
        self._store.open()
        self._store._connection.set_progress_handler(self._sql_progress, 10000)
        self._storage_error = None

    def _open_client(self, request: JournalRequest) -> dict:
        """Check a fresh client's expected generation against current disk metadata."""
        expected = (
            request.expected_journal.model_dump()
            if request.expected_journal is not None
            else {key: self._journal_info[key] for key in ("journal_id", "generation")}
        )
        settings = self._configuration.model_copy(
            update={"open_mode": "existing", "expected_journal": expected}
        )
        probe = SQLiteEventStore._from_settings(settings, self._context, True)
        probe._startup_diagnostics = self._startup
        probe._startup_diagnostic_id = request.diagnostic_id
        try:
            probe.open()
            return probe.get_journal_info()
        finally:
            probe.close()

    def _dispatch(self, call, store):
        command = call.command
        if command == "export_filtered_source":
            from core.journal.filtered import _export_source_view

            target = self.directory / "read-views" / f"{call.operation_id}.jsonl"
            _export_source_view(store, call.previous, target)
            return str(target)
        if isinstance(call, JournalAppend):
            event = call.event.model_dump()
            if command == "append_command_result":
                return store.append_command_result(event)
            store._append_replayed(event)
            return event["event_id"]
        if isinstance(call, JournalExport):
            return self._export(call, store)
        return store._read_command(call)

    def _export(self, call: JournalExport, store) -> dict:
        target = Path(call.path)
        receipt = self.directory / "exports" / f"{call.operation_id}.json"
        if receipt.exists():
            previous = JournalExportReceipt.model_validate(read_json(receipt))
            if previous.call != call:
                raise ValueError("Export identity has different parameters.")
            if previous.result is not None:
                return previous.result
            if target.exists():
                return self._reconcile_export(call, store)
        elif target.exists():
            raise ValueError("Export destination already exists.")
        write_json(receipt, {"call": call.model_dump(), "result": None})
        store._export_request_id = call.operation_id
        try:
            if call.command == "export_snapshot":
                result = store.export_snapshot(
                    target, min_free_bytes=call.min_free_bytes
                )
            else:
                result = store.export_diagnostics(call.operation_ids, target)
        finally:
            store._export_request_id = None
        write_json(receipt, {"call": call.model_dump(), "result": result})
        return result

    def _reconcile_export(self, call: JournalExport, store) -> dict:
        """Recognize publication completed before the writer lost its reply."""
        from core.models.journal_diagnostics import JournalSnapshotManifest

        target = Path(call.path)
        if not (target / "manifest.json").exists():
            raise LoggingStorageError(
                f"Export was interrupted before publication; its partial files are retained at {target}."
            )
        if call.command == "export_diagnostics":
            manifest, _events, _commands = store._read_diagnostics(target)
            if manifest.diagnostics_id != call.operation_id:
                raise LoggingStorageError(
                    "Export destination belongs to another operation."
                )
        else:
            manifest = JournalSnapshotManifest.model_validate(
                read_json(target / "manifest.json")
            )
            if manifest.snapshot_id != call.operation_id:
                raise LoggingStorageError(
                    "Export destination belongs to another operation."
                )
            settings = self._configuration.model_copy(
                update={
                    "db_path": target / "journal.sqlite",
                    "open_mode": "existing",
                    "expected_journal": {
                        "journal_id": manifest.journal_id,
                        "generation": manifest.generation,
                    },
                }
            )
            snapshot = SQLiteEventStore._from_settings(settings, self._context, True)
            try:
                snapshot._open_snapshot()
                snapshot._check_snapshot_contents(manifest)
            finally:
                snapshot.close()
        return manifest.model_dump()

    async def _close(self, consumer, monitor) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        for writer in tuple(self._clients):
            writer.close()
        for task in [*self._tasks, consumer, monitor]:
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(task for task in [*self._tasks, consumer, monitor] if task is not None),
            return_exceptions=True,
        )
        if self._store is not None and not self._busy:
            await asyncio.get_running_loop().run_in_executor(
                self._executor, self._store.close
            )
        if self._reader is not None:
            await asyncio.get_running_loop().run_in_executor(
                self._read_executor, self._reader.close
            )
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._read_executor.shutdown(wait=False, cancel_futures=True)
        if self._lease is not None:
            self._lease.close()
        if self.control is not None:
            self.control.close()


def run_writer(
    config_path: Path,
    runtime_directory: Path,
    settings: JournalWriterSettings,
    instance_id: str,
    control=None,
    managed: bool = True,
) -> None:
    writer = JournalWriter(
        config_path,
        runtime_directory,
        settings,
        instance_id,
        control=control,
        managed=managed,
    )
    asyncio.run(writer.serve())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    arguments = parser.parse_args()
    configuration, _context = _load_logging_settings(arguments.config)
    run_writer(
        arguments.config,
        arguments.runtime,
        configuration.writer_settings,
        uuid4().hex,
        managed=False,
    )


if __name__ == "__main__":
    main()
