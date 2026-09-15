"""Full-context fixtures and subprocess checkpoints for approved groups A, C, E, F."""

import json
import os
import sqlite3
import sys
import threading
from pathlib import Path
from unittest.mock import patch

from tests.helpers.logging_process import (
    STORE_OPTIONS,
    announce,
    checkpoint,
    make_store,
    write_settings,
)

BASE_CONTEXT = {
    "experiment_id": "experiment-current:experiment-parent",
    "previous_experiment_id": "experiment-parent",
    "run_id": "run-current:run-parent",
    "previous_run_id": "run-parent",
    "dag_revision_id": "dag-2",
    "template_revision_id": "template-2",
    "stage_id": "stage-A",
    "stage_execution_id": "execution-A-3",
    "stage_position": 2,
    "cycle_id": "cycle-3",
    "cycle_number": 3,
    "attempt_id": "attempt-A-3-1",
    "attempt_number": 1,
    "service_id": "service-A",
    "service_instance_id": "service-instance-A",
    "command_id": "command-A",
    "command_chain_id": "chain-A",
    "source": "runner",
    "module_name": "prepare",
    "module_version": "1.0",
    "module_hash": "a" * 64,
}
TEMPLATE_YAML = "# original comment\nstages: []\n"


def write_context_settings(
    directory: Path, *, context: dict | None = None, **settings
) -> Path:
    return write_settings(
        directory,
        context=dict(BASE_CONTEXT) if context is None else context,
        **settings,
    )


def context_event(number: int = 1) -> dict:
    """Literal envelope; expected results do not depend on production constants."""
    return {
        "schema_version": 1,
        "event_id": f"context-event-{number}",
        "producer_instance_id": "context-producer",
        "sequence_number": number,
        "occurred_at": "2026-09-14T01:02:03.000000+00:00",
        "event_type": "example.observed",
        "context": dict(BASE_CONTEXT),
        "operation_id": None,
        "data": {"number": number},
    }


def main() -> None:
    mode, config_text, *arguments = sys.argv[1:]
    config = Path(config_text)
    announce({"pid": os.getpid()})

    if mode == "import":
        with (
            patch.object(
                Path, "open", side_effect=AssertionError("Unexpected file I/O")
            ),
            patch.object(
                sqlite3, "connect", side_effect=AssertionError("Unexpected DB I/O")
            ),
        ):
            from core.logger import OperationLogger
            from core.logger_utils.storage import SQLiteEventStore

            OperationLogger(config)
            SQLiteEventStore(config.parent / "unopened.db", **STORE_OPTIONS)
            from core.logger_utils.filtered import FilteredJournal

            FilteredJournal(config, config.parent / "view.db")
        announce({"imported": True})
        return

    from core.logger import OperationLogger
    from core.logger_utils.events import LoggingStateError
    from core.logger_utils.storage import SQLiteEventStore

    if mode in ("restore_before_commit", "restore_after_commit"):
        manifest = json.loads(Path(arguments[0]).read_text(encoding="utf-8"))
        settings = json.loads(config.read_text(encoding="utf-8"))["logging"]
        path = Path(settings["db_path"])
        if not path.is_absolute():
            path = config.parent / path
        original_connect = sqlite3.connect

        class RestoreConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                result = super().execute(sql, parameters)
                if mode == "restore_before_commit" and sql.startswith(
                    "INSERT INTO journal_restorations"
                ):
                    checkpoint({"phase": "restore_uncommitted"})
                return result

        def connect(*args, **kwargs):
            return original_connect(*args, **kwargs, factory=RestoreConnection)

        with patch.object(sqlite3, "connect", side_effect=connect):
            store = make_store(
                path,
                open_mode="existing",
                expected_journal=settings["expected_journal"],
            )
            result = store.complete_restore(
                manifest,
                new_generation=arguments[1],
                restoration_id=arguments[2],
                diagnostics=Path(arguments[3]),
            )
        checkpoint({"phase": "restore_committed", "result": result})
        return

    if mode in ("view_reader", "view_crash"):
        from core.logger_utils.filtered import FilteredJournal

        view = FilteredJournal(config, Path(arguments[0]))
        view.open()
        try:
            if mode == "view_crash":
                original = view._write_entry

                def pending_publication(entry):
                    original(entry)
                    checkpoint({"phase": "view_uncommitted"})

                with patch.object(
                    view, "_write_entry", side_effect=pending_publication
                ):
                    view.refresh()
                return
            announce({"ready": True})
            for line in sys.stdin:
                if line.strip() == "stop":
                    return
                page = view.read_events()
                announce(
                    {
                        "ids": [entry["event"]["event_id"] for entry in page["events"]],
                        "publication_id": page["publication_id"],
                        "source": page["source"],
                    }
                )
        finally:
            view.close()
        return

    with OperationLogger(config) as logger:
        if mode == "fork":
            from core.logger_utils.filtered import FilteredJournal

            view = FilteredJournal(config, config.parent / "unopened-fork-view.db")
            child = os.fork()
            if child == 0:
                try:
                    for action in (
                        logger.get_context,
                        logger.get_journal_info,
                        logger.read_events,
                        logger.read_changes,
                        lambda: logger.read_command_result("request"),
                        lambda: logger.export_diagnostics(
                            ["operation"], config.parent / "forbidden-diagnostics"
                        ),
                        lambda: logger.export_snapshot(
                            config.parent / "forbidden", min_free_bytes=0
                        ),
                        logger.close,
                        view.open,
                        view.refresh,
                        view.read_events,
                        lambda: view.run(threading.Event()),
                        view.close,
                    ):
                        try:
                            action()
                        except LoggingStateError:
                            continue
                        os._exit(2)
                    with OperationLogger(Path(arguments[0])) as own:
                        own.record_event("child.owned")
                    os._exit(0)
                except BaseException:  # noqa: BLE001 - Child reports via exit status.
                    os._exit(3)
            _, status = os.waitpid(child, 0)
            announce(
                {"child_exit": os.waitstatus_to_exitcode(status), "child_pid": child}
            )
            return

        if mode in ("result_before_commit", "result_after_commit"):
            request_id = arguments[0]
            if mode == "result_before_commit":

                def before_index(sql: str) -> None:
                    if sql.startswith("INSERT INTO command_results"):
                        checkpoint({"phase": "result_uncommitted"})

                logger._store._connection.set_trace_callback(before_index)
            result_id = logger.record_command_result(
                request_id, {"origin": "runner"}, author="runner", outcome="failed"
            )
            checkpoint({"phase": "result_committed", "event_id": result_id})
            return

        if mode == "snapshot_crash":
            original_rename = Path.rename

            def after_database_copy(path, target):
                result = original_rename(path, target)
                if path.name == "journal.sqlite.part":
                    checkpoint({"phase": "database_copied"})
                return result

            with patch.object(Path, "rename", after_database_copy):
                logger.export_snapshot(Path(arguments[0]), min_free_bytes=0)
            announce({"exported": True})
            return

        if mode != "worker":
            raise ValueError(f"Unknown v2 process mode: {mode}")
        announce({"ready": True, "context": logger.get_context()})
        for line in sys.stdin:
            command = json.loads(line)
            if command["action"] == "stop":
                return
            announce({"attempting": command["action"]})
            if command["action"] == "write":
                identifiers = [
                    logger.record_event("v2.process", {"index": index})
                    for index in range(command["count"])
                ]
                announce({"ids": identifiers})
            elif command["action"] == "result":
                result_id = logger.record_command_result(
                    command["request_id"],
                    command["response"],
                    author=command["author"],
                    outcome=command["outcome"],
                )
                announce({"event_id": result_id})
            else:
                raise ValueError("Unknown worker command.")


if __name__ == "__main__":
    main()
