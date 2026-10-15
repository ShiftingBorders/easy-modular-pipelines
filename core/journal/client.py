"""Synchronous journal transport with stable requests across writer restarts."""

from __future__ import annotations

import select
import socket
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from core.journal.events import (
    JournalGenerationChanged,
    LoggingConfigurationError,
    LoggingStateError,
    LoggingStorageError,
    encode_event,
)
from core.journal.storage import SQLiteEventStore
from core.journal.supervisor import (
    discovery_path,
    ensure_standalone,
    owner_active,
    standalone_directory,
    stop_standalone,
)
from core.models.journal_writer import (
    JournalInformation,
    JournalReply,
    JournalRequest,
    WriterDiscovery,
    WriterEndpoint,
    WriterProcessStatus,
    WriterRecoveryClock,
)
from core.primitives.file_lock import _open_file_lock
from core.primitives.framing import encode_frame, read_frame_sync
from core.primitives.json_files import read_json
from core.primitives.processes import process_identity, process_running

if TYPE_CHECKING:
    from core.models.journal_records import JournalContext
    from core.models.journal_settings import JournalConfiguration


class JournalClient:
    """A process-local client; connection loss never changes an event envelope."""

    def __init__(
        self,
        config_path: Path,
        configuration: JournalConfiguration,
        context: JournalContext,
        read_only: bool = False,
        *,
        request_timeout_seconds: float | None = None,
    ) -> None:
        self.config_path = config_path
        self.configuration = configuration
        self.db_path = configuration.db_path
        self._timeout = float(configuration.busy_timeout_seconds)
        self._max_event_bytes = configuration.max_event_bytes
        self._diagnostic_context = context
        self._read_only = read_only
        self._socket = None
        self._socket_instance_id: str | None = None
        self._clock_lock = threading.Lock()
        self._clock_id: str | None = None
        self._clock_seen = 0.0
        self._recovery_total = 0.0
        self._cancelled = threading.Event()
        self._lock = threading.RLock()
        self._expected = configuration.expected_journal
        self._journal_id = self._generation = None
        self.endpoint_path = configuration.writer_endpoint_path
        self._managed = self.endpoint_path is not None
        self._failed = False
        # Optional telemetry can select a finite delivery budget. Ordinary
        # journal calls retain their COMMIT wait across managed recovery.
        self._request_timeout_seconds = request_timeout_seconds
        # This unopened store supplies only the existing independent emergency
        # diagnostic path. It never opens a client-side SQL connection.
        self._diagnostics = SQLiteEventStore._from_settings(
            configuration, context, read_only
        )

    def open(self) -> None:
        if self.endpoint_path is None:
            pointer = discovery_path(self.db_path)
            if pointer.exists():
                route = WriterDiscovery.model_validate(read_json(pointer))
                self._managed = route.managed
                self.endpoint_path = route.runtime_directory / "endpoint.json"
        if self._managed and self.endpoint_path is not None:
            status_path = self.endpoint_path.parent / "status.json"
            if status_path.exists() and read_json(status_path)["state"] == "stopped":
                self._managed = False
        if not self._managed and not self._read_only:
            if self.configuration.open_mode == "create" and self.db_path.exists():
                raise LoggingConfigurationError(
                    f"Journal already exists: {self.db_path}."
                )
            self.endpoint_path = ensure_standalone(
                self.config_path,
                self.db_path,
                self.configuration.writer_settings,
                configuration=self.configuration,
            )
            self._managed = read_json(discovery_path(self.db_path))["managed"]
        info = JournalInformation.model_validate(
            self._call({"command": "open"})
        ).model_dump()
        self._expected = {key: info[key] for key in ("journal_id", "generation")}
        self._journal_id, self._generation = info["journal_id"], info["generation"]

    def request_close(self) -> None:
        self._cancelled.set()
        connection = self._socket
        if connection is not None:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self) -> None:
        self.request_close()
        with self._lock:
            self._disconnect()

    def _disconnect(self) -> None:
        if self._socket is not None:
            self._socket.close()
            self._socket = None
        self._socket_instance_id = None

    def _endpoint(self) -> WriterEndpoint:
        if self.endpoint_path is None:
            raise LoggingStorageError("Journal endpoint is unavailable.")
        endpoint = WriterEndpoint.model_validate(read_json(self.endpoint_path))
        self._managed = endpoint.managed
        if self._managed:
            status_path = self.endpoint_path.parent / "status.json"
            if status_path.exists():
                status = read_json(status_path)
                if status["state"] == "stopped":
                    raise LoggingStateError(
                        "The journal owner stopped; reopen the client explicitly."
                    )
                if not status.get("history_available", True):
                    error = LoggingStorageError(
                        "Journal history is unavailable; the experiment is paused."
                    )
                    error.retryable = True
                    raise error
                if status["state"] == "storage_unavailable":
                    error = LoggingStorageError(
                        "Journal storage is unavailable; the experiment is paused."
                    )
                    error.retryable = True
                    raise error
                if status["state"] in ("starting", "restarting", "quiescing"):
                    raise ConnectionError("Journal is recovering.")
        return endpoint

    def _call(self, call: dict):
        with self._lock:
            recovery_started = None
            deadline = (
                None
                if self._request_timeout_seconds is None
                else time.monotonic() + self._request_timeout_seconds
            )
            while True:
                if self._cancelled.is_set():
                    raise LoggingStateError("Journal client is closing.")
                if deadline is not None and time.monotonic() >= deadline:
                    raise LoggingStorageError(
                        "Optional journal delivery was not confirmed within its time budget."
                    )
                try:
                    reply = self._exchange(call, deadline)
                    if reply.ok:
                        return reply.value
                    if reply.retryable and deadline is not None:
                        raise LoggingStorageError(
                            "Optional journal delivery was not confirmed; it will not be replayed."
                        )
                    if not reply.retryable:
                        try:
                            self._raise_error(reply.error)
                        except (
                            LoggingStorageError,
                            LoggingConfigurationError,
                        ) as error:
                            self._report_failure(
                                error, call["command"], call.get("event")
                            )
                            raise
                    self._disconnect()
                except (ConnectionError, OSError, EOFError) as error:
                    self._disconnect()
                    if self._read_only and not self._managed:
                        return self._read_offline(call)
                    if self._read_only:
                        failure = LoggingStorageError(
                            "Journal writer is temporarily unavailable."
                        )
                        failure.retryable = True
                        raise failure from error
                    if deadline is not None:
                        raise LoggingStorageError(
                            "Optional journal delivery was not confirmed; it will not be replayed."
                        ) from error
                    if not self._managed or not owner_active(self.endpoint_path.parent):
                        self.endpoint_path = ensure_standalone(
                            self.config_path,
                            self.db_path,
                            self.configuration.writer_settings,
                            configuration=self.configuration,
                            expected=self._expected,
                        )
                        self._managed = read_json(discovery_path(self.db_path))[
                            "managed"
                        ]
                    if recovery_started is None:
                        recovery_started = time.monotonic()
                    if (
                        not self._managed
                        and time.monotonic() - recovery_started
                        >= self.configuration.writer_settings.recovery_timeout_seconds
                    ):
                        failure = LoggingStorageError(
                            "Journal recovery exceeded 60 seconds."
                        )
                        raise failure from error
                except LoggingStorageError as error:
                    if (
                        self._read_only
                        or not self._managed
                        or not getattr(error, "retryable", False)
                    ):
                        raise
                    # A paused EMP retains the exact request until storage and
                    # required history are available, or the caller closes it.
                    self._disconnect()
                self._cancelled.wait(0.05)

    def _read_offline(self, call: dict):
        """An expired standalone writer must not turn a reader into a launcher."""
        request = JournalRequest.model_validate(
            {"token": "", "read_only": True, "call": call}
        )
        settings = self.configuration.model_copy(
            update={"open_mode": "existing", "expected_journal": self._expected}
        )
        reader = SQLiteEventStore._from_settings(
            settings, self._diagnostic_context, True
        )
        try:
            reader.open()
            return reader._read_command(request.call)
        finally:
            reader.close()

    def _exchange(self, call: dict, deadline: float | None) -> JournalReply:
        """Send one attempt; the caller owns replay and storage-error policy."""
        endpoint = self._endpoint()
        if self._socket_instance_id != endpoint.writer_instance_id:
            self._disconnect()
        if self._socket is None:
            self._socket = socket.create_connection(
                (endpoint.endpoint.host, endpoint.endpoint.port), timeout=1
            )
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._socket_instance_id = endpoint.writer_instance_id
        token = Path(endpoint.endpoint.token_file).read_text(encoding="utf-8")
        request = JournalRequest.model_validate(
            {
                "token": token,
                "expected_journal": self._expected,
                "read_only": self._read_only,
                "min_free_bytes": self.configuration.min_free_bytes,
                "call": call,
            }
        )
        if deadline is not None:
            self._socket.settimeout(max(0.001, min(1, deadline - time.monotonic())))
        self._socket.sendall(encode_frame(request.model_dump(), validate=False))
        return self._receive_reply(deadline)

    def _receive_reply(self, deadline: float | None) -> JournalReply:
        next_probe = time.monotonic() + 0.5
        while not self._cancelled.is_set():
            remaining = 0.1 if deadline is None else deadline - time.monotonic()
            if remaining <= 0:
                raise LoggingStorageError(
                    "Optional journal delivery was not confirmed within its time budget."
                )
            if select.select([self._socket], [], [], min(0.1, remaining))[0]:
                return JournalReply.model_validate(read_frame_sync(self._socket))
            if self._managed:
                # Consult owner status without waiting for the SQL queue.
                self._endpoint()
            elif time.monotonic() >= next_probe:
                next_probe = time.monotonic() + 0.5
                self._check_standalone_progress()
        raise LoggingStateError("Journal request cancelled by client closure.")

    def _check_standalone_progress(self) -> None:
        try:
            status = self.get_status()
            stalled = (
                status["busy"]
                and time.monotonic() - status["progress_at"]
                > self.configuration.writer_settings.stall_timeout_seconds
            )
            if not stalled:
                return
        except (OSError, EOFError, ValueError):
            pass
        with _open_file_lock(standalone_directory(self.db_path) / "start.lock"):
            endpoint = WriterEndpoint.model_validate(read_json(self.endpoint_path))
            if endpoint.managed:
                self._managed = True
            else:
                stop_standalone(endpoint)
        raise ConnectionError("Standalone journal writer stopped responding.")

    @staticmethod
    def _raise_error(document: dict) -> None:
        name, message = document["type"], document["message"]
        if name == "JournalGenerationChanged":
            raise JournalGenerationChanged(document["expected"], document["actual"])
        kinds = {
            "ValueError": ValueError,
            "ValidationError": ValueError,
            "TypeError": TypeError,
            "LoggingStateError": LoggingStateError,
            "LoggingConfigurationError": LoggingConfigurationError,
            "LoggingStorageError": LoggingStorageError,
        }
        error = kinds.get(name, LoggingStorageError)(message)
        error.journal_failed = document.get("journal_failed", False)
        cause = document.get("cause_type")
        if cause in ("OperationalError", "IntegrityError", "DatabaseError"):
            error.__cause__ = getattr(sqlite3, cause)(document["cause_message"])
        elif cause in ("OSError", "PermissionError", "FileNotFoundError"):
            import builtins

            error.__cause__ = getattr(builtins, cause)(document["cause_message"])
        if error.__cause__ is not None:
            for key in (
                "errno",
                "winerror",
                "filename",
                "filename2",
                "sqlite_errorcode",
                "sqlite_errorname",
            ):
                if key in document:
                    setattr(error.__cause__, key, document[key])
        if document.get("traceback"):
            error.add_note(document["traceback"])
        raise error

    def get_status(self) -> dict:
        endpoint = WriterEndpoint.model_validate(read_json(self.endpoint_path))
        token = Path(endpoint.endpoint.token_file).read_text(encoding="utf-8")
        with socket.create_connection(
            (endpoint.endpoint.host, endpoint.endpoint.port), timeout=1
        ) as connection:
            connection.sendall(
                encode_frame({"token": token, "call": {"command": "status"}})
            )
            reply = JournalReply.model_validate(read_frame_sync(connection))
            if not reply.ok:
                self._raise_error(reply.error)
            return WriterProcessStatus.model_validate(reply.value).model_dump(
                mode="json"
            )

    def recovery_seconds(self) -> float:
        if self.endpoint_path is None:
            return self._recovery_total
        try:
            status = read_json(self.endpoint_path.parent / "status.json")
        except FileNotFoundError:
            return self._recovery_total
        clock = WriterRecoveryClock.model_validate(status)
        duration = clock.elapsed(time.monotonic())
        with self._clock_lock:
            if self._clock_id != clock.clock_id:
                self._clock_id = clock.clock_id
                self._clock_seen = 0
            self._recovery_total += max(0, duration - self._clock_seen)
            self._clock_seen = duration
            return self._recovery_total

    def append(self, event) -> None:
        if self._read_only:
            raise LoggingStateError("This journal client is read-only.")
        encode_event(event, self._max_event_bytes)
        self._call({"command": "append", "event": event})

    def append_command_result(self, event) -> str:
        if self._read_only:
            raise LoggingStateError("This journal client is read-only.")
        encode_event(event, self._max_event_bytes)
        return self._call({"command": "append_command_result", "event": event})

    def get_journal_info(self) -> dict:
        return JournalInformation.model_validate(
            self._call({"command": "get_journal_info"})
        ).model_dump()

    def read_events(self, checkpoint=None, *, limit=100, view="raw") -> dict:
        return self._call(
            {
                "command": "read_events",
                "checkpoint": checkpoint,
                "limit": limit,
                "view": view,
            }
        )

    def read_changes(self, checkpoint=None, *, limit=100) -> dict:
        return self._call(
            {"command": "read_changes", "checkpoint": checkpoint, "limit": limit}
        )

    def read_event_batch(self, event_ids=None, *, limit=1000, before=None) -> dict:
        return self._call(
            {
                "command": "read_event_batch",
                "event_ids": event_ids,
                "limit": limit,
                "before": before,
            }
        )

    def read_command_result(self, request_id) -> dict | None:
        return self._call({"command": "read_command_result", "request_id": request_id})

    def export_snapshot(self, destination, *, min_free_bytes=0) -> dict:
        return self._call(
            {
                "command": "export_snapshot",
                "path": str(destination),
                "operation_id": uuid4().hex,
                "min_free_bytes": min_free_bytes,
            }
        )

    def export_diagnostics(self, operation_ids, destination) -> dict:
        return self._call(
            {
                "command": "export_diagnostics",
                "path": str(destination),
                "operation_id": uuid4().hex,
                "operation_ids": operation_ids,
            }
        )

    @contextmanager
    def _filtered_source(self, previous):
        """Read an immutable export; the live SQL snapshot stays in the writer."""
        path = Path(
            self._call(
                {
                    "command": "export_filtered_source",
                    "operation_id": uuid4().hex,
                    "previous": None if previous is None else previous.model_dump(),
                }
            )
        )
        try:
            with path.open(encoding="utf-8") as source:
                yield source
            self.get_journal_info()
        finally:
            path.unlink(missing_ok=True)

    def _storage_failure(self, error, action, event=None):
        return self._diagnostics._storage_failure(error, action, event)

    def _report_failure(self, error, action, event=None) -> None:
        self._diagnostics._report_failure(error, action, event)


def _make_journal_store(
    config_path: Path,
    settings: JournalConfiguration,
    context: JournalContext,
    read_only: bool,
    *,
    request_timeout_seconds: float | None = None,
) -> SQLiteEventStore | JournalClient:
    """Use IPC for live journals and direct read-only access for offline history."""
    if read_only:
        endpoint_path = settings.writer_endpoint_path
        pointer = discovery_path(settings.db_path)
        if endpoint_path is None and pointer.exists():
            endpoint_path = (
                Path(read_json(pointer)["runtime_directory"]) / "endpoint.json"
            )
        active = False
        if endpoint_path is not None:
            status_path = endpoint_path.parent / "status.json"
            if status_path.exists():
                active = read_json(status_path)["state"] != "stopped"
            elif endpoint_path.exists():
                identity = read_json(endpoint_path)["process"]
                try:
                    active = (
                        process_running(identity["pid"])
                        and process_identity(identity["pid"]) == identity
                    )
                except OSError:
                    active = False
        if not active:
            return SQLiteEventStore._from_settings(settings, context, True)
    return JournalClient(
        config_path,
        settings,
        context,
        read_only,
        request_timeout_seconds=request_timeout_seconds,
    )
