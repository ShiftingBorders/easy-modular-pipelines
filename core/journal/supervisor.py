"""Managed writer supervision and coordinated standalone startup."""

from __future__ import annotations

import json
import multiprocessing
import os
import socket
import subprocess
import threading
import time
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from core.journal.events import LoggingStateError, LoggingStorageError
from core.journal.settings import _load_logging_settings
from core.models.journal_writer import (
    JournalWriterSettings,
    WriterDiscovery,
    WriterEndpoint,
    WriterProcessStatus,
)
from core.primitives.file_lock import _open_file_lock
from core.primitives.framing import encode_frame, read_frame_sync
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import JsonObject, copy_json_object
from core.primitives.processes import process_identity, process_running

if TYPE_CHECKING:
    from core.models.journal_settings import JournalConfiguration


def discovery_path(database: Path) -> Path:
    # Aliases must share ownership metadata without rewriting configured paths.
    database = database.resolve()
    return database.with_name(database.name + ".writer.json")


def standalone_directory(database: Path) -> Path:
    database = database.resolve()
    return database.parent / ("." + database.name + ".writer")


class JournalSupervisor:
    """Own one child and monitor it independently of all journal client calls."""

    def __init__(
        self,
        config_path: Path,
        *,
        runtime_directory: Path,
        settings: JournalWriterSettings | None = None,
    ) -> None:
        if not config_path.is_absolute() or not runtime_directory.is_absolute():
            raise ValueError("Writer configuration/runtime paths must be absolute.")
        self.config_path = config_path
        self.directory = runtime_directory
        self.settings = settings or JournalWriterSettings()
        self._settings_explicit = settings is not None
        self.client_config_path = self.directory / "client.json"
        self.endpoint_path = self.directory / "endpoint.json"
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._mutex = threading.RLock()
        self._thread = None
        self._process = None
        self._control = None
        self._lease = None
        self._document = None
        self._replacement_pending = False
        self._status = {
            "clock_id": uuid4().hex,
            "state": "stopped",
            "process": None,
            "writer_instance_id": None,
            "queue_depth": 0,
            "queue_capacity": self.settings.queue_capacity,
            "busy": False,
            "failure_detected_at": 0.0,
            "restarts": 0,
            "history_available": True,
            "recovery_started_at": None,
            "recovery_seconds": 0.0,
            "error": None,
        }

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        configuration, context = _load_logging_settings(self.config_path)
        if not self._settings_explicit:
            self.settings = configuration.writer_settings
            self._status["queue_capacity"] = self.settings.queue_capacity
        deadline = time.monotonic() + self.settings.recovery_timeout_seconds
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lease = _open_file_lock(self.directory / "owner.lock")
        self._document = {
            "logging": configuration.model_dump(mode="json"),
            "operation_context": context.model_dump(),
        }
        self._original_path = configuration.db_path
        write_json(
            self.client_config_path,
            {
                **self._document,
                "logging": {
                    **self._document["logging"],
                    "writer_endpoint_path": str(self.endpoint_path),
                },
            },
        )
        self._stop.clear()
        self._ready.clear()
        self._change(state="starting", db_path=str(configuration.db_path))
        with _wait_start_lock(configuration.db_path, deadline, self._stop):
            pointer = discovery_path(configuration.db_path)
            if pointer.exists():
                previous = WriterDiscovery.model_validate(read_json(pointer))
                endpoint = previous.runtime_directory / "endpoint.json"
                if endpoint.exists():
                    old = WriterEndpoint.model_validate(read_json(endpoint))
                    orphaned = (
                        previous.runtime_directory == self.directory
                        or not owner_active(previous.runtime_directory)
                    )
                    stop_standalone(old, orphaned=orphaned)
            write_json(
                pointer, {"runtime_directory": str(self.directory), "managed": True}
            )
        self._thread = threading.Thread(
            target=self._watch, name="journal-supervisor", daemon=True
        )
        self._thread.start()
        if not self._ready.wait(max(0, deadline - time.monotonic())):
            self._change(
                state="storage_unavailable",
                error={
                    "message": "Journal writer did not become ready within its recovery budget.",
                },
            )

    def get_status(self) -> dict:
        with self._mutex:
            result = copy_json_object(self._status, "writer status")
        started = result["recovery_started_at"]
        if started is not None:
            result["recovery_seconds"] += time.monotonic() - started
        return result

    def _change(self, **values) -> None:
        with self._mutex:
            self._status.update(values)
            document = dict(self._status)
        # Only transitions are published; heartbeat/queue samples stay in RAM.
        try:
            write_json(self.directory / "status.json", document)
        except OSError:
            # RAM status and supervision remain available with a failed disk.
            pass

    def _spawn(self) -> None:
        from core.journal.writer import run_writer

        worker_path = self.directory / "worker.json"
        document = {
            **self._document,
            "logging": {
                **self._document["logging"],
                "writer_endpoint_path": None,
                "writer_settings": self.settings.model_dump(),
            },
        }
        write_json(worker_path, document)
        (self.directory / "startup-error.json").unlink(missing_ok=True)
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        instance = uuid4().hex
        process = context.Process(
            target=run_writer,
            args=(worker_path, self.directory, self.settings, instance, child),
            name="journal-writer",
            daemon=False,
        )
        self._control = parent
        self._process = process
        try:
            process.start()
        finally:
            child.close()
        self._change(writer_instance_id=instance, process=process_identity(process.pid))

    def _watch(self) -> None:
        deadline = time.monotonic() + self.settings.recovery_timeout_seconds
        failures = 0
        try:
            while not self._stop.is_set():
                try:
                    if self._process is not None:
                        self._stop_worker()
                    self._spawn()
                    failure = self._receive_worker(deadline)
                except Exception as error:  # noqa: BLE001 - Supervision must survive worker/startup faults.
                    from core.journal.writer import writer_error

                    # EOF is the ordinary signature of a killed worker. It says
                    # nothing about readability of the database it left behind.
                    failure = {"state": "restart", "error": writer_error(error)}
                if self._stop.is_set():
                    break
                if self._status["recovery_started_at"] is None:
                    deadline = time.monotonic() + self.settings.recovery_timeout_seconds
                self._begin_recovery(failure.get("error"))
                try:
                    self._stop_worker()
                except Exception as error:  # noqa: BLE001 - Keep supervising a child whose exit is unconfirmed.
                    self._change(
                        state="storage_unavailable", error={"message": str(error)}
                    )
                    self._ready.set()
                    self._stop.wait(1)
                    continue
                if failure.get("state") == "open_failed" and not self._replace_storage(
                    failure["error"]
                ):
                    self._ready.set()
                    self._stop.wait(5)
                    continue
                if time.monotonic() >= deadline:
                    self._change(state="storage_unavailable")
                    self._ready.set()
                    self._stop.wait(1)
                    deadline = time.monotonic() + self.settings.recovery_timeout_seconds
                delays = self.settings.restart_delays_seconds
                delay = delays[min(failures, len(delays) - 1)] if delays else 0.1
                self._stop.wait(delay)
                failures += 1
                with self._mutex:
                    self._status["restarts"] += 1
        finally:
            self._stop_worker()
            self._ready.set()

    def _receive_worker(self, deadline: float) -> dict:
        last_report = time.monotonic()
        worker_ready = False
        while not self._stop.is_set():
            if self._control.poll(self.settings.heartbeat_interval_seconds):
                packet = json.loads(self._control.recv_bytes(1048576))
                last_report = time.monotonic()
                if packet["state"] == "open_failed":
                    return packet
                packet = WriterProcessStatus.model_validate(packet).model_dump(
                    mode="json"
                )
                if packet["state"] == "storage_unavailable":
                    return {"state": "restart", "error": packet["error"]}
                worker_ready = True
                if self._status["state"] != "ready":
                    self._became_ready(packet)
                else:
                    with self._mutex:
                        self._status.update(
                            {
                                key: value
                                for key, value in packet.items()
                                if key != "state"
                            }
                        )
                if (
                    packet["busy"]
                    and time.monotonic() - packet["progress_at"]
                    > self.settings.stall_timeout_seconds
                ):
                    return {
                        "state": "restart",
                        "error": {
                            "message": "Journal storage stopped making progress."
                        },
                    }
            if not self._process.is_alive():
                return {
                    "state": "restart",
                    "error": {
                        "message": f"Writer exited with code {self._process.exitcode}."
                    },
                }
            if not worker_ready and time.monotonic() > deadline:
                return {
                    "state": "restart",
                    "error": {"message": "Writer startup deadline expired."},
                }
            if worker_ready and time.monotonic() - last_report > 1.5:
                return {
                    "state": "restart",
                    "error": {"message": "Writer heartbeat expired."},
                }
        return {"state": "stopped"}

    def _became_ready(self, packet: dict) -> None:
        now = time.monotonic()
        previous = self._status["recovery_started_at"]
        elapsed = self._status["recovery_seconds"] + (
            0 if previous is None else now - previous
        )
        identity = {key: packet["journal"][key] for key in ("journal_id", "generation")}
        self._document["logging"].update(
            open_mode="existing",
            expected_journal=identity,
            db_path=packet["db_path"],
            writer_endpoint_path=str(self.endpoint_path),
        )
        write_json(self.client_config_path, self._document)
        self._change(
            **packet,
            recovery_started_at=previous if self._replacement_pending else None,
            recovery_seconds=self._status["recovery_seconds"]
            if self._replacement_pending
            else elapsed,
            ready_at=now,
            history_available=not self._replacement_pending,
        )
        self._ready.set()

    def _begin_recovery(self, error: dict | None) -> None:
        now = time.monotonic()
        self._change(
            state="restarting",
            error=error,
            failure_detected_at=now,
            recovery_started_at=self._status["recovery_started_at"] or now,
        )

    def _replace_storage(self, error: dict) -> bool:
        if self._replacement_pending:
            self._change(
                state="storage_unavailable", error=error, history_available=False
            )
            return False
        old = Path(self._document["logging"]["db_path"])
        # An invalid client generation is not a broken journal.
        if error.get("type") in (
            "JournalGenerationChanged",
            "LoggingStateError",
            "ValueError",
            "TypeError",
        ):
            self._change(state="storage_unavailable", error=error)
            return False
        replacement = old.with_name(f"events-{uuid4().hex}.sqlite")
        notice = {
            "event_id": uuid4().hex,
            "producer_instance_id": uuid4().hex,
            "target_path": str(replacement),
            "context": {"source": "journal_supervisor", "process_id": os.getpid()},
            "occurred_at": datetime.now(UTC).isoformat(timespec="microseconds"),
            "data": {
                "previous_path": str(old),
                "previous_journal": self._document["logging"]["expected_journal"],
                "reason": error,
            },
        }
        try:
            write_json(self.directory / "replacement.json", notice)
        except OSError:
            self._change(state="storage_unavailable", error=error)
            return False
        self._document["logging"].update(
            db_path=str(replacement), open_mode="create", expected_journal=None
        )
        self._replacement_pending = True
        return True

    def _stop_worker(self) -> None:
        process, control = self._process, self._control
        if process is None:
            return
        try:
            if process.is_alive():
                with suppress(OSError, EOFError):
                    control.send_bytes(b'{"command":"stop"}')
                process.join(0.3)
            if process.is_alive():
                process.terminate()
                process.join(2)
            if process.is_alive():
                process.kill()
                process.join(2)
            if process.is_alive():
                raise LoggingStorageError(
                    "Previous writer termination was not confirmed."
                )
        finally:
            if control is not None:
                control.close()
        process.close()
        self._process = self._control = None

    def _request_stop(self) -> None:
        """Wake startup and supervision without releasing ownership prematurely."""
        self._stop.set()
        self._ready.set()

    def close(self) -> None:
        if self._lease is None and (
            self._thread is None or not self._thread.is_alive()
        ):
            return
        self._change(state="stopped")
        self._request_stop()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(6)
            if self._thread.is_alive():
                raise LoggingStorageError("Journal supervisor did not stop.")
        if self._lease is not None:
            self._lease.close()
            self._lease = None
        elapsed = self.get_status()["recovery_seconds"]
        self._change(
            state="stopped",
            process=None,
            busy=False,
            queue_depth=0,
            recovery_started_at=None,
            recovery_seconds=elapsed,
        )


def _wait_start_lock(
    database: Path, deadline: float, stop: threading.Event | None = None
):
    """Coordinate launchers without turning brief native-lock contention into failure."""
    while True:
        if stop is not None and stop.is_set():
            raise LoggingStateError("Journal startup was cancelled.")
        try:
            return _open_file_lock(standalone_directory(database) / "start.lock")
        except OSError as error:
            if time.monotonic() >= deadline:
                raise LoggingStorageError(
                    "Timed out coordinating journal writer startup."
                ) from error
            time.sleep(0.05)


def ensure_standalone(
    config_path: Path,
    database: Path,
    settings: JournalWriterSettings,
    *,
    configuration: JournalConfiguration | None = None,
    expected: JsonObject | None = None,
) -> Path:
    """Serialize independent launches; the worker outlives the calling application."""
    directory = standalone_directory(database)
    endpoint_path = directory / "endpoint.json"
    deadline = time.monotonic() + settings.recovery_timeout_seconds
    lease = _wait_start_lock(database, deadline)
    try:
        pointer = discovery_path(database)
        if pointer.exists():
            route = WriterDiscovery.model_validate(read_json(pointer))
            directory = route.runtime_directory
            endpoint_path = directory / "endpoint.json"
            if route.managed and owner_active(directory):
                return endpoint_path
        if endpoint_path.exists():
            endpoint = WriterEndpoint.model_validate(read_json(endpoint_path))
            identity = endpoint.process.model_dump()
            try:
                if (
                    process_running(identity["pid"])
                    and process_identity(identity["pid"]) == identity
                ):
                    if endpoint.managed:
                        stop_standalone(endpoint, orphaned=True)
                    else:
                        return endpoint_path
            except (OSError, ProcessLookupError):
                pass
        loaded, context = _load_logging_settings(config_path)
        configuration = configuration or loaded
        document = {
            "logging": configuration.model_dump(mode="json"),
            "operation_context": context.model_dump(),
        }
        document["logging"]["writer_endpoint_path"] = None
        if expected is not None:
            document["logging"].update(open_mode="existing", expected_journal=expected)
        document["logging"]["writer_settings"] = settings.model_dump()
        worker_config = directory / "worker.json"
        write_json(worker_config, document)
        (directory / "startup-error.json").unlink(missing_ok=True)
        process = _launch_standalone(worker_config, directory)
        return _wait_standalone(process, database, directory, deadline)
    finally:
        lease.close()


def _launch_standalone(worker_config: Path, directory: Path) -> subprocess.Popen:
    """Launch independently of the caller and reap its eventual exit."""
    from core.primitives.processes import module_process_arguments

    arguments, environment = module_process_arguments(
        [
            "python",
            "-B",
            "-m",
            "core.journal.writer",
            "--config",
            str(worker_config),
            "--runtime",
            str(directory),
        ]
    )
    with (directory / "writer.stderr.log").open("ab") as output:
        process = subprocess.Popen(
            arguments,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=output,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            start_new_session=os.name != "nt",
        )
    threading.Thread(
        target=process.wait, name="journal-launcher-reaper", daemon=True
    ).start()
    return process


def _wait_standalone(
    process: subprocess.Popen, database: Path, directory: Path, deadline: float
) -> Path:
    """Publish a ready writer or reap a failed launch within the startup budget."""
    endpoint_path = directory / "endpoint.json"
    while time.monotonic() < deadline:
        if endpoint_path.exists():
            endpoint = WriterEndpoint.model_validate(read_json(endpoint_path))
            identity = endpoint.process.model_dump()
            try:
                if (
                    process_running(identity["pid"])
                    and process_identity(identity["pid"]) == identity
                ):
                    write_json(
                        discovery_path(database),
                        {"runtime_directory": str(directory), "managed": False},
                    )
                    return endpoint_path
            except OSError:
                pass
        failure = directory / "startup-error.json"
        if failure.exists():
            from core.journal.client import JournalClient

            # Startup publishes its failure before closing SQLite/executors.
            # Reap the owned child before returning its error to the caller.
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            JournalClient._raise_error(read_json(failure)["error"])
        if process.poll() is not None:
            raise LoggingStorageError(
                "Standalone journal writer exited during startup."
            )
        time.sleep(0.025)
    process.kill()
    process.wait(timeout=5)
    raise LoggingStorageError("Standalone journal writer did not become ready.")


def owner_active(directory: Path) -> bool:
    """A held native owner lease, rather than a stale status file, proves ownership."""
    path = directory / "owner.lock"
    if not path.exists():
        return False
    try:
        lease = _open_file_lock(path)
    except OSError:
        return True
    lease.close()
    return False


def stop_standalone(endpoint: WriterEndpoint, *, orphaned: bool = False) -> None:
    """Stop an exact standalone instance, or an orphan proven by the caller's owner lease."""
    import psutil

    if endpoint.managed and not orphaned:
        raise LoggingStorageError("A managed writer can only be stopped by its owner.")
    identity = endpoint.process.model_dump()
    if (
        not process_running(identity["pid"])
        or process_identity(identity["pid"]) != identity
    ):
        return
    try:
        with socket.create_connection(
            (endpoint.endpoint.host, endpoint.endpoint.port), timeout=0.5
        ) as connection:
            connection.sendall(
                encode_frame(
                    {
                        "token": Path(endpoint.endpoint.token_file).read_text(
                            encoding="utf-8"
                        ),
                        "call": {"command": "shutdown_standalone"},
                    }
                )
            )
            read_frame_sync(connection)
    except (OSError, EOFError, ValueError):
        pass
    deadline = time.monotonic() + 0.5
    while time.monotonic() < deadline:
        if not process_running(identity["pid"]):
            return
        time.sleep(0.025)
    try:
        process = psutil.Process(identity["pid"])
        if process_identity(identity["pid"]) == identity:
            process.kill()
            process.wait(timeout=5)
    except psutil.NoSuchProcess:
        return
