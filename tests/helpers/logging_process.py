"""Fixtures and controlled child processes for the approved logging test plan."""

import json
import os
import queue
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRATCH_ROOT = PROJECT_ROOT / ".artifacts" / "tmp" / "logging-tests"
STORE_OPTIONS = {
    "busy_timeout_seconds": 5,
    "max_event_bytes": None,
    "min_free_bytes": 0,
    "open_mode": "create",
    "expected_journal": None,
}


def journal_identity(path: Path) -> dict:
    """Read fixture identity independently of production validation/read methods."""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        journal_id, generation = db.execute(
            "SELECT journal_id, generation FROM journal_info"
        ).fetchone()
    return {"journal_id": journal_id, "generation": generation}


def make_store(path: Path, **options):
    from core.logger_utils.storage import SQLiteEventStore

    settings = {**STORE_OPTIONS, **options}
    if settings["open_mode"] == "existing" and "expected_journal" not in options:
        settings["expected_journal"] = journal_identity(path)
    return SQLiteEventStore(path, **settings)


def existing_settings(config: Path) -> Path:
    document = json.loads(config.read_text(encoding="utf-8"))
    path = Path(document["logging"]["db_path"])
    if not path.is_absolute():
        path = config.parent / path
    document["logging"].update(
        open_mode="existing", expected_journal=journal_identity(path)
    )
    config.write_text(json.dumps(document), encoding="utf-8")
    return config


def checkpoint_for(client, cursor: int) -> dict:
    info = client.get_journal_info()
    return {
        "journal_id": info["journal_id"],
        "generation": info["generation"],
        "cursor": cursor,
    }


def cleanup_directory(directory: tempfile.TemporaryDirectory) -> None:
    """Retry only Windows removal of an already-empty directory, never a locked file."""
    root = Path(directory.name).resolve()
    if root.parent != SCRATCH_ROOT.resolve():
        raise ValueError(
            "Test cleanup must remain inside the logging scratch directory."
        )
    for attempt in range(3):
        try:
            directory.cleanup()
            return
        except OSError as error:
            # Windows can report a pending deletion as ERROR_DIR_NOT_EMPTY briefly.
            if os.name != "nt" or error.winerror != 145 or attempt == 2:
                raise
            failed_path = Path(error.filename).resolve()
            if not failed_path.is_relative_to(root):
                raise
            # Give Windows a bounded chance to finish pending file deletions.
            # Never retry if actual files remain or a file deletion itself failed.
            time.sleep(0.05)
            try:
                has_entries = any(failed_path.iterdir())
            except FileNotFoundError:
                has_entries = False
            if has_entries:
                raise


def write_settings(directory: Path, *, context: dict | None = None, **settings) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    document = {
        "logging": {
            "db_path": "events.db",
            **STORE_OPTIONS,
            "filtered_refresh_interval_seconds": 1,
            **settings,
        },
        "operation_context": {"source": "module", "run_id": "run-1"}
        if context is None
        else context,
    }
    path = directory / "settings.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    if settings.get("open_mode") == "existing" and "expected_journal" not in settings:
        existing_settings(path)
    return path


def event_fixture(number: int = 1) -> dict:
    """An independent literal envelope, without production serializers or constants."""
    return {
        "schema_version": 2,
        "event_id": f"event-{number}",
        "producer_instance_id": "producer-1",
        "sequence_number": number,
        "occurred_at": "2026-09-10T01:02:03.000000+00:00",
        "event_type": "test.observed",
        "context": {"source": "module", "run_id": "run-1"},
        "operation_id": None,
        "data": {"number": number},
    }


def read_database(path: Path) -> list[dict]:
    """Inspect committed data without using any journal read/validation implementation."""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        return [
            json.loads(row[0])
            for row in db.execute("SELECT event_json FROM events ORDER BY cursor")
        ]


class LoggingProcess:
    """Launch through uv, then control the exact Python child identified by its handshake."""

    def __init__(
        self,
        mode: str,
        config: Path,
        *arguments: str,
        module_name: str = "tests.helpers.logging_process",
    ) -> None:
        self.mode = mode
        self.config = config
        self.arguments = arguments
        self.module_name = module_name
        self.process = None
        self.pid = None
        self.output = queue.Queue()
        self.error_file = None
        self.reader = None
        self.deadline = 0.0

    def start(self) -> None:
        executable = shutil.which("uv")
        if executable is None:
            raise RuntimeError("Process tests require the project's uv executable.")
        self.error_file = (self.config.parent / f"{self.mode}-stderr.log").open(
            "w", encoding="utf-8"
        )
        self.deadline = time.monotonic() + 30
        self.process = subprocess.Popen(
            [
                executable,
                "run",
                "--locked",
                "python",
                "-u",
                "-m",
                self.module_name,
                self.mode,
                str(self.config),
                *self.arguments,
            ],
            cwd=PROJECT_ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.error_file,
            text=True,
            encoding="utf-8",
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        self.reader = threading.Thread(target=self._read_output, daemon=True)
        self.reader.start()
        self.pid = self.receive()["pid"]

    def _read_output(self) -> None:
        for line in self.process.stdout:
            self.output.put(line)
        self.output.put(None)

    def receive(self) -> dict:
        try:
            line = self.output.get(timeout=max(0.01, self.deadline - time.monotonic()))
        except queue.Empty as error:
            raise TimeoutError(
                f"Child {self.mode} exceeded the 30-second watchdog."
            ) from error
        if line is None:
            raise RuntimeError(
                f"Child {self.mode} closed stdout; inspect its stderr log."
            )
        return json.loads(line)

    def send(self, command: str = "continue") -> None:
        self.process.stdin.write(command + "\n")
        self.process.stdin.flush()

    def kill_writer(self) -> None:
        if self.pid is None or self.process.poll() is not None:
            raise RuntimeError("The owned writer is not running.")
        os.kill(self.pid, signal.SIGTERM if os.name == "nt" else signal.SIGKILL)

    def wait(self) -> int:
        return self.process.wait(timeout=max(0.01, self.deadline - time.monotonic()))

    def close(self) -> None:
        if self.process is not None:
            if self.process.poll() is None:
                try:
                    # uv can still be reaping a Python child whose stdout closed.
                    self.process.wait(timeout=0.25)
                except subprocess.TimeoutExpired:
                    pass
            if self.process.poll() is None:
                try:
                    if self.pid is not None:
                        self.kill_writer()
                except ProcessLookupError:
                    pass
                except PermissionError:
                    # Windows reports access denied for an already-exited PID.
                    # Accept only proof that our own uv launcher has also exited.
                    self.process.wait(timeout=2)
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            if self.reader is not None:
                self.reader.join(timeout=5)
            self.process.stdin.close()
            self.process.stdout.close()
        if self.error_file is not None:
            self.error_file.close()


def announce(data: dict) -> None:
    print(json.dumps(data, ensure_ascii=True), flush=True)


def checkpoint(data: dict) -> None:
    announce(data)
    if not sys.stdin.readline():
        raise RuntimeError("Parent closed the control channel.")


def main() -> None:
    mode, config_text, *arguments = sys.argv[1:]
    config = Path(config_text)
    announce({"pid": os.getpid()})

    if mode == "import":
        with (
            patch.object(
                Path, "open", side_effect=AssertionError("Unexpected file open")
            ),
            patch.object(
                sqlite3, "connect", side_effect=AssertionError("Unexpected DB open")
            ),
        ):
            from core.logger import OperationLogger
            from core.logger_utils.storage import SQLiteEventStore

            OperationLogger(config)
            SQLiteEventStore(config.parent / "not-created.db", **STORE_OPTIONS)
        announce({"imported": True})
        return

    from core.logger import OperationLogger

    if mode == "read":
        with OperationLogger(config) as logger:
            records = logger.read_events(limit=1000)["events"]
        with closing(sqlite3.connect(config.parent / "events.db")) as db:
            integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        announce({"records": records, "integrity": integrity})
        return

    if mode == "schema_crash":
        original_connect = sqlite3.connect

        class SchemaConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                result = super().execute(sql, parameters)
                if sql.startswith("CREATE TABLE events"):
                    checkpoint({"schema_uncommitted": True})
                return result

        with (
            patch.object(
                sqlite3,
                "connect",
                side_effect=lambda *args, **kwargs: original_connect(
                    *args, **kwargs, factory=SchemaConnection
                ),
            ),
            OperationLogger(config),
        ):
            pass
        return

    if mode == "cwd":
        os.chdir(arguments[0])

    with OperationLogger(config) as logger:
        if mode == "lock":
            from core.logger_utils.events import LoggingStateError, LoggingStorageError

            confirmed = logger.record_event("test.before")
            result = {"confirmed_id": confirmed}
            with closing(
                sqlite3.connect(config.parent / "events.db", isolation_level=None)
            ) as blocker:
                blocker.execute("BEGIN IMMEDIATE")
                try:
                    logger.record_event("test.blocked")
                except LoggingStorageError as error:
                    result["storage_error"] = type(error.__cause__).__name__
                try:
                    logger.record_event("test.rejected")
                except LoggingStateError:
                    result["blocked_after_failure"] = True
                result["read_ids"] = [
                    record["event"]["event_id"]
                    for record in logger.read_events()["events"]
                ]
                blocker.execute("ROLLBACK")
            logger.close()
            logger.open()
            logger.record_event("test.after")
            announce(result)
            return
        if mode in ("cwd", "write"):
            count = int(arguments[0]) if mode == "write" else 1
            ids = [
                logger.record_event("test.observed", {"number": index})
                for index in range(count)
            ]
            announce({"ids": ids, "context": logger.get_context()})
            return
        if mode == "operation":
            with logger.operation("node_attempt", "prepare") as operation:
                checkpoint({"operation_id": operation.get_operation_id()})
                logger.record_event("test.continued", operation=operation)
            announce({"completed": True})
            return
        if mode in ("before_insert", "during_insert", "after_commit"):
            ids = [
                logger.record_event("test.prefix", {"number": index})
                for index in range(20)
            ]
            announce({"confirmed_ids": ids})
            if mode == "before_insert":
                checkpoint({"before_insert": True})
            if mode == "during_insert":
                connection = logger._store._connection
                inserting_event = False

                def trace_statement(sql: str) -> None:
                    nonlocal inserting_event
                    inserting_event = sql.startswith("INSERT INTO events")

                def progress() -> int:
                    if inserting_event:
                        checkpoint({"during_insert": True})
                    return 0

                # Health checks also execute SQL; stop inside INSERT, not those reads.
                connection.set_trace_callback(trace_statement)
                connection.set_progress_handler(progress, 1)
            event_id = logger.record_event("test.last", {"payload": "x" * 10000})
            checkpoint({"confirmed_last": event_id})
            return
        if mode == "fork":
            from core.logger_utils.events import LoggingStateError

            child = os.fork()
            if child == 0:
                try:
                    for call in (logger.get_context, logger.read_events, logger.close):
                        try:
                            call()
                        except LoggingStateError:
                            continue
                        os._exit(2)
                    with OperationLogger(Path(arguments[0])) as child_logger:
                        child_logger.record_event("test.child")
                    os._exit(0)
                except BaseException:  # noqa: BLE001 - Report child failure through its exit code.
                    os._exit(3)
            _, status = os.waitpid(child, 0)
            announce({"child_exit": os.waitstatus_to_exitcode(status)})
            return
        raise ValueError(f"Unknown test process mode: {mode}")


if __name__ == "__main__":
    main()
