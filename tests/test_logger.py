"""Approved LIFE, CONTEXT, OP, and EVENT scenarios for the public logger API."""

import asyncio
import json
import os
import socket
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import LoggingStateError
from tests.helpers.logging_process import (
    SCRATCH_ROOT,
    LoggingProcess,
    cleanup_directory,
    existing_settings,
    read_database,
    write_settings,
)


class OperationLoggerTests(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.config = write_settings(self.folder)
        self.db_path = self.folder / "events.db"
        self.logger = OperationLogger(self.config)
        self.addCleanup(self.logger.close)
        self.logger.open()

    def test_import_has_no_file_or_database_side_effects(self):
        """LIFE-01: a fresh process imports helpers and constructs clients without I/O."""
        config = self.folder / "missing-settings.json"
        process = LoggingProcess("import", config)
        self.addCleanup(process.close)
        process.start()
        self.assertEqual(process.receive(), {"imported": True})
        self.assertEqual(process.wait(), 0)
        self.assertFalse(config.exists())
        self.assertFalse((self.folder / "not-created.db").exists())

    def test_open_close_and_reload_settings(self):
        """LIFE-02: repeated lifecycle calls are explicit and reopening reloads the file."""
        with self.assertRaises(LoggingStateError):
            self.logger.open()
        self.logger.record_event("test.old")
        self.logger.close()
        self.logger.close()
        for call in (
            self.logger.read_events,
            self.logger.get_context,
            lambda: self.logger.record_event("test.closed"),
        ):
            with self.assertRaises(LoggingStateError):
                call()
        document = {
            "logging": {
                **json.loads(self.config.read_text(encoding="utf-8"))["logging"],
                "db_path": "new/events.db",
            },
            "operation_context": {"run_id": "run-2"},
        }
        self.config.write_text(json.dumps(document), encoding="utf-8")
        with self.logger:
            self.logger.record_event("test.new")
        self.assertEqual(
            [e["event_type"] for e in read_database(self.db_path)], ["test.old"]
        )
        self.assertEqual(
            read_database(self.folder / "new/events.db")[0]["context"]["run_id"],
            "run-2",
        )

    def test_reopen_changes_source_and_preserves_events_and_cursor(self):
        """LIFE-03: restart resets the source counter, not stored history or local cursor."""
        for _ in range(3):
            self.logger.record_event("test.before")
        before = read_database(self.db_path)
        self.logger.close()
        self.logger.open()
        for _ in range(2):
            self.logger.record_event("test.after")
        page = self.logger.read_events()["events"]
        self.assertEqual([record["cursor"] for record in page], [1, 2, 3, 4, 5])
        self.assertEqual([record["event"] for record in page[:3]], before)
        self.assertEqual(
            [record["event"]["sequence_number"] for record in page], [1, 2, 3, 1, 2]
        )
        self.assertNotEqual(
            page[0]["event"]["producer_instance_id"],
            page[-1]["event"]["producer_instance_id"],
        )

    def test_parent_context_is_explicit_and_detached(self):
        """CONTEXT-01, DATA-02: active operations do not introduce an implicit parent."""
        with self.logger.operation("root", "registration") as root:
            child_context = root.get_child_context()
            for name in ("first", "second"):
                with self.logger.operation("child", name, context=child_context):
                    pass
            with self.logger.operation("independent", "independent"):
                pass
            self.assertEqual(
                child_context["parent_operation_id"], root.get_operation_id()
            )
        starts = [
            e
            for e in read_database(self.db_path)
            if e["event_type"] == "operation.started"
        ]
        self.assertEqual(
            [e["data"]["parent_operation_id"] for e in starts],
            [None, root.get_operation_id(), root.get_operation_id(), None],
        )
        context = self.logger.get_context()
        context["run_id"] = "mutated"
        self.assertEqual(self.logger.get_context()["run_id"], "run-1")

    def test_writer_identity_is_local_even_with_exported_context(self):
        """CONTEXT-02: settings and explicit overrides cannot substitute the parent's PID/host."""
        self.logger.close()
        document = json.loads(self.config.read_text(encoding="utf-8"))
        document["operation_context"].update(
            process_id=999999,
            host_name="parent-host",
            parent_operation_id="parent-attempt",
            node_id="node-1",
            node_execution_id="execution-1",
            attempt_number=1,
            module_name="prepare",
            module_version="v1",
        )
        self.config.write_text(json.dumps(document), encoding="utf-8")
        process = LoggingProcess("write", existing_settings(self.config), "1")
        self.addCleanup(process.close)
        process.start()
        context = process.receive()["context"]
        self.assertEqual(process.wait(), 0)
        self.assertEqual(context["process_id"], process.pid)
        self.assertNotEqual(context["process_id"], os.getpid())
        self.assertEqual(context["host_name"], socket.gethostname())
        for name in (
            "run_id",
            "node_id",
            "node_execution_id",
            "parent_operation_id",
            "source",
            "module_name",
            "module_version",
        ):
            self.assertEqual(context[name], document["operation_context"][name])
        self.logger.open()
        with self.logger.operation(
            "test", "local", context={"process_id": 999999, "host_name": "foreign"}
        ):
            pass
        self.assertEqual(
            read_database(self.db_path)[-1]["context"]["process_id"], os.getpid()
        )

    def test_clients_and_service_contexts_do_not_mix(self):
        """CONTEXT-03: independent journals retain their run/service identity."""
        second_config = write_settings(self.folder / "second")
        with OperationLogger(second_config) as second:
            self.logger.record_event("test.first")
            second.record_event("test.second", context={"run_id": "run-2"})
            second.record_event(
                "service.heartbeat",
                context={"run_id": None, "participant_instance_id": "redis-1"},
            )
        self.assertEqual(read_database(self.db_path)[0]["context"]["run_id"], "run-1")
        events = read_database(second_config.parent / "events.db")
        self.assertEqual(events[0]["context"]["run_id"], "run-2")
        self.assertIsNone(events[1]["context"]["run_id"])
        self.assertEqual(events[1]["context"]["participant_instance_id"], "redis-1")

    def test_start_timing_and_monotonic_duration(self):
        """OP-01: start is explicit and a backward wall clock does not change duration."""
        operation = self.logger.operation("type", "name")
        self.assertEqual(read_database(self.db_path), [])
        times = [
            datetime(2026, 9, 10, 12, tzinfo=UTC),
            datetime(2026, 9, 10, 11, tzinfo=UTC),
        ]
        with (
            patch(
                "core.logger.time.monotonic_ns", side_effect=[1000000000, 1250000000]
            ),
            patch("core.logger.datetime") as clock,
        ):
            clock.now.side_effect = times
            with operation:
                self.assertEqual(len(read_database(self.db_path)), 1)
        events = read_database(self.db_path)
        self.assertEqual(events[1]["data"]["duration_ms"], 250)
        self.assertEqual(
            [e["occurred_at"] for e in events],
            [t.isoformat(timespec="microseconds") for t in times],
        )
        immediate = self.logger.start_operation("type", "manual")
        self.assertEqual(len(read_database(self.db_path)), 3)
        self.assertNotEqual(immediate.get_operation_id(), operation.get_operation_id())
        self.logger.finish_operation(immediate)

    def test_exception_outcomes_preserve_original_objects(self):
        """OP-02: automatic outcomes distinguish failure and cancellation without suppression."""
        for error, status in (
            (ValueError("failure"), "failed"),
            (KeyboardInterrupt(), "cancelled"),
            (SystemExit(2), "cancelled"),
        ):
            with self.subTest(error=type(error).__name__):
                with (
                    self.assertRaises(type(error)) as caught,
                    self.logger.operation("test", "exception"),
                ):
                    raise error
                self.assertIs(caught.exception, error)
                events = read_database(self.db_path)[-3:]
                self.assertEqual(
                    [e["event_type"] for e in events],
                    ["operation.started", "error.recorded", "operation.finished"],
                )
                self.assertEqual(events[-1]["data"]["status"], status)

    def test_actual_async_task_cancellation_is_recorded(self):
        """OP-02: cancel an actual waiting task using an explicit synchronization point."""

        async def scenario():
            ready = asyncio.Event()
            blocker = asyncio.Event()

            async def worker():
                with self.logger.operation("test", "async"):
                    ready.set()
                    await blocker.wait()

            task = asyncio.create_task(worker())
            await ready.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        asyncio.run(scenario())
        self.assertEqual(read_database(self.db_path)[-1]["data"]["status"], "cancelled")

    def test_manual_outcomes_and_repeated_completion(self):
        """OP-03: one manual terminal event, explicit reason, no duplicate completion or entry."""
        for status in ("succeeded", "failed", "cancelled"):
            operation = self.logger.start_operation("manual", status)
            event_id = self.logger.finish_operation(
                operation, status=status, reason_code="chosen", attributes={"result": 7}
            )
            event = read_database(self.db_path)[-1]
            self.assertEqual(event["event_id"], event_id)
            self.assertEqual(event["data"]["status"], status)
            self.assertEqual(event["data"]["reason_code"], "chosen")
            self.assertEqual(event["data"]["attributes"], {"result": 7})
            with self.assertRaises(LoggingStateError):
                self.logger.finish_operation(operation)
            with self.assertRaises(LoggingStateError), operation:
                self.fail("A completed operation must not be reentered")
        with (
            self.logger.operation("managed", "managed") as managed,
            self.assertRaises(LoggingStateError),
        ):
            self.logger.finish_operation(managed)
        manual = self.logger.start_operation("manual", "invalid")
        with self.assertRaises(ValueError):
            self.logger.finish_operation(manual, status="unknown")
        self.logger.finish_operation(manual)
        terminals = [
            e
            for e in read_database(self.db_path)
            if e["event_type"] == "operation.finished"
        ]
        self.assertEqual(len(terminals), 5)

    def test_stale_foreign_and_finished_handles_are_rejected(self):
        """OP-04, OP-05: close/reopen never fabricates completion or revives old handles."""
        unfinished = self.logger.start_operation("test", "unfinished")
        self.logger.close()
        self.logger.open()
        self.assertEqual(len(read_database(self.db_path)), 1)
        with self.assertRaises(LoggingStateError):
            self.logger.finish_operation(unfinished)
        other_config = write_settings(self.folder / "other")
        with OperationLogger(other_config) as other:
            foreign = other.start_operation("test", "foreign")
            with self.assertRaises(LoggingStateError):
                self.logger.record_event("test.invalid", operation=foreign)
        current = self.logger.start_operation("test", "current")
        with self.assertRaises(ValueError):
            self.logger.record_event("test.invalid", operation=current, context={})
        self.logger.finish_operation(current)
        for call in (
            lambda: self.logger.record_event("test.invalid", operation=current),
            lambda: self.logger.record_error(ValueError(), operation=current),
            lambda: self.logger.record_resources(
                {"tokens": {"value": 1, "unit": "token"}}, operation=current
            ),
        ):
            with self.assertRaises(LoggingStateError):
                call()
        self.assertEqual(len(read_database(self.db_path)), 3)
        self.logger.record_event("test.valid")
        self.assertEqual(len(read_database(self.db_path)), 4)

    def test_dag_dependencies_and_retries_are_plain_facts(self):
        """EVENT-01: A's retries share execution identity; A is B's dependency, not parent."""
        with self.logger.operation("experiment", "run") as run:
            self.logger.record_event(
                "run.snapshot", {"edges": [["A", "B"]]}, operation=run
            )
            for attempt_number in (1, 2):
                context = {
                    **run.get_child_context(),
                    "node_id": "A",
                    "node_execution_id": "exec-A",
                    "attempt_number": attempt_number,
                }
                attempt = self.logger.start_operation(
                    "node_attempt", "A", context=context
                )
                self.logger.finish_operation(
                    attempt, status="failed" if attempt_number == 1 else "succeeded"
                )
            self.logger.record_event(
                "node.transfer",
                {"from": "exec-A", "to": "exec-B", "output": "artifact-1"},
                operation=run,
            )
            with self.logger.operation(
                "node_attempt",
                "B",
                context={
                    **run.get_child_context(),
                    "node_id": "B",
                    "node_execution_id": "exec-B",
                    "attempt_number": 1,
                },
            ):
                pass
        starts = [
            e
            for e in read_database(self.db_path)
            if e["event_type"] == "operation.started"
        ][1:]
        self.assertEqual(
            [e["context"]["node_execution_id"] for e in starts],
            ["exec-A", "exec-A", "exec-B"],
        )
        self.assertEqual([e["context"]["attempt_number"] for e in starts], [1, 2, 1])
        self.assertEqual(len({e["operation_id"] for e in starts}), 3)
        self.assertEqual(
            {e["data"]["parent_operation_id"] for e in starts}, {run.get_operation_id()}
        )
        transfers = [
            e for e in read_database(self.db_path) if e["event_type"] == "node.transfer"
        ]
        self.assertEqual(
            transfers[0]["data"],
            {"from": "exec-A", "to": "exec-B", "output": "artifact-1"},
        )

    def test_recorded_error_can_be_handled_without_failing_operation(self):
        """EVENT-02, EVENT-03: explicit error correlation, traceback, and successful recovery."""
        with self.logger.operation("test", "handled") as operation:
            try:
                raise ValueError("bad input")
            except ValueError as error:
                error_id = self.logger.record_error(
                    error,
                    operation=operation,
                    error_code="invalid_input",
                    include_traceback=True,
                )
                self.assertEqual(
                    self.logger.record_error(
                        error, operation=operation, error_id=error_id
                    ),
                    error_id,
                )
                self.logger.record_error(
                    RuntimeError("follow-up"),
                    operation=operation,
                    caused_by_error_id=error_id,
                )
                with self.assertRaises(ValueError):
                    self.logger.record_error(
                        error, error_id="same", caused_by_error_id="same"
                    )
        events = read_database(self.db_path)
        first, second, third = events[1:4]
        self.assertEqual(first["data"]["error_type"], "builtins.ValueError")
        self.assertEqual(first["data"]["message"], "bad input")
        self.assertEqual(first["data"]["error_code"], "invalid_input")
        self.assertIn("ValueError: bad input", first["data"]["traceback"])
        self.assertIsNone(second["data"]["traceback"])
        self.assertEqual(first["data"]["error_id"], second["data"]["error_id"])
        self.assertNotEqual(first["event_id"], second["event_id"])
        self.assertEqual(third["data"]["caused_by_error_id"], error_id)
        self.assertEqual(events[-1]["data"]["status"], "succeeded")

    def test_automatic_error_ids_and_unprintable_exceptions(self):
        """EVENT-03: automatic observations are independent; a broken __str__ is representable."""
        with (
            self.assertRaises(ValueError),
            self.logger.operation("test", "outer") as outer,
            self.logger.operation("test", "inner", context=outer.get_child_context()),
        ):
            raise ValueError("shared exception")
        errors = [
            e
            for e in read_database(self.db_path)
            if e["event_type"] == "error.recorded"
        ]
        self.assertEqual(len({e["data"]["error_id"] for e in errors}), 2)

        class UnprintableError(Exception):
            def __str__(self):
                raise ValueError("formatting failed")

        self.logger.record_error(UnprintableError())
        self.assertEqual(
            read_database(self.db_path)[-1]["data"]["message"],
            "[exception message unavailable]",
        )

    def test_resources_preserve_measurement_semantics(self):
        """EVENT-04, EVENT-05: explicit measurement kinds, scopes, units, zero, and no roll-up."""
        with self.logger.operation("parent", "parent") as parent:  # noqa: SIM117 - Keep the tested parent/child scopes visible.
            with self.logger.operation(
                "child", "child", context=parent.get_child_context()
            ) as child:
                self.logger.record_resources(
                    {"tokens": {"value": 0, "unit": "token"}}, operation=child
                )
                for kind in ("delta", "total", "gauge", "peak"):
                    self.logger.record_resources(
                        {
                            "custom": {
                                "value": 2.5,
                                "unit": "unit",
                                "kind": kind,
                                "scope": "operation",
                                "estimated": True,
                                "attributes": {"provider": "local"},
                            }
                        },
                        operation=child,
                    )
        for scope in ("process", "service"):
            self.logger.record_resources(
                {
                    "memory": {
                        "value": 100,
                        "unit": "byte",
                        "kind": "gauge",
                        "scope": scope,
                    }
                },
                context={"participant_instance_id": "redis-1"},
            )
        events = [
            e
            for e in read_database(self.db_path)
            if e["event_type"] == "resources.recorded"
        ]
        self.assertEqual(len(events), 7)
        self.assertFalse(
            any(e["operation_id"] == parent.get_operation_id() for e in events)
        )
        self.assertEqual(
            events[0]["data"]["resources"]["tokens"],
            {
                "value": 0,
                "unit": "token",
                "kind": "delta",
                "scope": "operation",
                "estimated": False,
            },
        )
        self.assertEqual(
            [e["data"]["resources"]["custom"]["kind"] for e in events[1:5]],
            ["delta", "total", "gauge", "peak"],
        )
        self.assertEqual(
            events[1]["data"]["resources"]["custom"]["attributes"],
            {"provider": "local"},
        )
        with self.assertRaises(ValueError):
            self.logger.record_resources({"tokens": {"value": 1, "unit": "token"}})

    def test_invalid_resources_are_rejected_without_poisoning_client(self):
        """EVENT-04, EVENT-08: invalid resource input never writes or consumes a sequence."""
        with self.logger.operation("test", "resources") as operation:
            invalid = (
                {},
                {"x": 1},
                {"x": {"value": -1, "unit": "byte"}},
                {"": {"value": 1, "unit": "byte"}},
            )
            for resources in invalid:
                with self.assertRaises((TypeError, ValueError)):
                    self.logger.record_resources(resources, operation=operation)
            for field, value in (
                ("value", True),
                ("value", float("nan")),
                ("value", float("inf")),
                ("value", "1"),
                ("unit", ""),
                ("kind", "wrong"),
                ("scope", "wrong"),
                ("estimated", 1),
                ("attributes", []),
                ("extra", 1),
            ):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises((TypeError, ValueError)),
                ):
                    self.logger.record_resources(
                        {"x": {"value": 1, "unit": "byte", field: value}},
                        operation=operation,
                    )
            self.logger.record_resources(
                {"x": {"value": 1, "unit": "byte"}}, operation=operation
            )
        self.assertEqual(
            [e["sequence_number"] for e in read_database(self.db_path)], [1, 2, 3]
        )

    def test_progress_preserves_counts_and_rejects_invalid_values(self):
        """EVENT-06: zero/unknown totals are valid; invalid counts and units are rejected."""
        for completed, total in ((0, 0), (3, 10), (10, 10), (3, None)):
            self.logger.record_progress(completed, total=total, stage="loading")
        for completed, total in (
            (11, 10),
            (-1, 10),
            (True, 10),
            (float("nan"), 10),
            (1, float("inf")),
        ):
            with self.assertRaises((TypeError, ValueError)):
                self.logger.record_progress(completed, total=total)
        with self.assertRaises(ValueError):
            self.logger.record_progress(0, unit="")
        self.assertEqual(
            [
                (e["data"]["completed"], e["data"]["total"])
                for e in read_database(self.db_path)
            ],
            [(0, 0), (3, 10), (10, 10), (3, None)],
        )

    def test_artifacts_register_metadata_without_file_io(self):
        """EVENT-07, EVENT-08: registering metadata does not read/create/accept an artifact."""
        original_open = Path.open

        def guard_artifact(path, *args, **kwargs):
            self.assertNotEqual(path.name, "result.json")
            return original_open(path, *args, **kwargs)

        with self.logger.operation("test", "artifact") as operation:
            with patch.object(Path, "open", guard_artifact):
                artifact_id = self.logger.record_artifact(
                    "artifacts\\result.json",
                    "output",
                    size_bytes=17,
                    content_hash="sha256:example",
                    operation=operation,
                )
            self.assertEqual(len(read_database(self.db_path)), 2)
            for value in (
                "/absolute",
                "C:\\absolute",
                "C:relative",
                "\\\\server\\share\\file",
                "../outside",
                "x/../outside",
                ".",
            ):
                with self.subTest(path=value), self.assertRaises(ValueError):
                    self.logger.record_artifact(value, "output", operation=operation)
            for size in (-1, 1.5, True):
                with self.assertRaises(ValueError):
                    self.logger.record_artifact(
                        "result", "output", size_bytes=size, operation=operation
                    )
        event = read_database(self.db_path)[1]
        self.assertEqual(
            event["data"],
            {
                "artifact_id": artifact_id,
                "path": "artifacts/result.json",
                "purpose": "output",
                "size_bytes": 17,
                "content_hash": "sha256:example",
            },
        )
        self.assertFalse((self.folder / "artifacts").exists())

    def test_event_snapshots_ids_and_reserved_kinds(self):
        """DATA-02, DATA-05, EVENT-08: detached data and typed events keep an intact sequence."""
        attributes = {"nested": ["initial"]}
        operation = self.logger.operation("test", "snapshot", attributes=attributes)
        attributes["nested"].append("changed")
        with operation:
            payload = {"nested": [1]}
            event_id = self.logger.record_event(
                "custom.first", payload, operation=operation
            )
            payload["nested"].append(2)
            for kind in (
                "operation.started",
                "operation.finished",
                "error.recorded",
                "resources.recorded",
                "progress.recorded",
                "artifact.recorded",
            ):
                with self.assertRaises(ValueError):
                    self.logger.record_event(kind)
            with self.assertRaises(TypeError):
                self.logger.record_event("custom.bad", {"tuple": (1, 2)})
            self.logger.record_event(
                "custom.second", {"text": "Привет 🌍"}, operation=operation
            )
        events = read_database(self.db_path)
        self.assertEqual(events[0]["data"]["attributes"], {"nested": ["initial"]})
        self.assertEqual(events[1]["event_id"], event_id)
        self.assertEqual(events[1]["data"], {"nested": [1]})
        self.assertEqual([e["sequence_number"] for e in events], [1, 2, 3, 4])

    def test_four_threads_preserve_every_event_and_sequence(self):
        """STORE-06: four synchronized writers produce exactly 100 distinct ordered events."""
        barrier = Barrier(4)

        def write_events(worker):
            barrier.wait(timeout=10)
            return [
                self.logger.record_event(
                    "test.thread", {"worker": worker, "index": index}
                )
                for index in range(25)
            ]

        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(write_events, range(4)))
        events = read_database(self.db_path)
        self.assertEqual(len(events), 100)
        self.assertEqual(
            {e["event_id"] for e in events},
            {identifier for ids in results for identifier in ids},
        )
        self.assertEqual(len({e["event_id"] for e in events}), 100)
        self.assertEqual([e["sequence_number"] for e in events], list(range(1, 101)))
        self.assertEqual(
            {(e["data"]["worker"], e["data"]["index"]) for e in events},
            {(worker, index) for worker in range(4) for index in range(25)},
        )
