"""CONFIG-01..05 and DATA-01..05 from the approved logging plan."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from core.logger import OperationLogger
from core.logger_utils.events import (
    LoggingConfigurationError,
    copy_json_object,
    encode_event,
    load_logging_config,
    validate_context,
)
from tests.helpers.logging_process import (
    SCRATCH_ROOT,
    LoggingProcess,
    cleanup_directory,
    event_fixture,
    read_database,
    write_settings,
)


class LoggingConfigurationTests(unittest.TestCase):
    def setUp(self) -> None:
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.config = write_settings(self.folder, db_path="logs/events.db")

    def test_constructor_validates_absolute_path_without_io(self):
        """CONFIG-01, LIFE-01: construction validates arguments without opening resources."""
        with (
            patch.object(Path, "open") as file_open,
            patch("sqlite3.connect") as connect,
        ):
            OperationLogger(self.config)
            OperationLogger(str(self.config))
            for value in (None, 42):
                with self.subTest(value=value), self.assertRaises(TypeError):
                    OperationLogger(value)
            for value in ("", "relative.json", str(self.folder / "bad\x00.json")):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    OperationLogger(value)
            file_open.assert_not_called()
            connect.assert_not_called()

    def test_configured_paths_do_not_depend_on_child_cwd(self):
        """CONFIG-02: relative and absolute configured paths survive a different cwd."""
        other = self.folder / "other"
        other.mkdir()
        for configured in ("logs/events.db", str(self.folder / "absolute.db")):
            with self.subTest(path=configured):
                self.config = write_settings(self.folder, db_path=configured)
                process = LoggingProcess("cwd", self.config, str(other))
                self.addCleanup(process.close)
                process.start()
                self.assertEqual(len(process.receive()["ids"]), 1)
                self.assertEqual(process.wait(), 0)
                expected = self.folder / configured
                self.assertEqual(len(read_database(expected)), 1)
                self.assertFalse((other / "logs").exists())

    def test_windows_ambiguous_paths_are_rejected(self):
        """CONFIG-02: Windows drive-relative/root-relative settings are not anchored to cwd."""
        if os.name != "nt":
            self.skipTest("Windows-specific path syntax.")
        for value in ("C:events.db", "\\events.db"):
            with self.subTest(path=value):
                write_settings(self.folder, db_path=value)
                with self.assertRaises(LoggingConfigurationError):
                    OperationLogger(self.config).open()
        self.assertFalse((self.folder / "logs").exists())

    def test_invalid_files_preserve_cause_without_creating_database(self):
        """CONFIG-03: read/JSON/Unicode/type failures have an actionable configuration error."""
        for contents in (b"{", b"\xff", b"[]", b"null", b"7"):
            with self.subTest(contents=contents):
                self.config.write_bytes(contents)
                with self.assertRaises(LoggingConfigurationError) as caught:
                    OperationLogger(self.config).open()
                self.assertIsNotNone(caught.exception.__cause__)
                self.assertFalse((self.folder / "logs").exists())
        for path in (self.folder / "missing.json", self.folder):
            with (
                self.subTest(path=path),
                self.assertRaises(LoggingConfigurationError) as caught,
            ):
                OperationLogger(path).open()
            self.assertIsInstance(caught.exception.__cause__, OSError)
        failure = PermissionError("Controlled config read failure")
        with (
            patch.object(Path, "open", side_effect=failure),
            self.assertRaises(LoggingConfigurationError) as caught,
        ):
            OperationLogger(self.config).open()
        self.assertIs(caught.exception.__cause__, failure)

    def test_required_and_unknown_configuration_fields(self):
        """CONFIG-04: reject missing/typo fields while permitting other module settings."""
        invalid = (
            {},
            {"logging": {}},
            {"logging": []},
            {"logging": {"db_path": "logs/events.db", "typo": True}},
            {
                "logging": {"db_path": "logs/events.db"},
                "operation_context": {"typo": "x"},
            },
        )
        for document in invalid:
            with self.subTest(document=document):
                self.config.write_text(json.dumps(document), encoding="utf-8")
                with self.assertRaises(LoggingConfigurationError):
                    OperationLogger(self.config).open()
                self.assertFalse((self.folder / "logs").exists())
        document = {
            "logging": {"db_path": "logs/events.db"},
            "model": {"temperature": 0.5},
        }
        self.config.write_text(json.dumps(document), encoding="utf-8")
        with OperationLogger(self.config) as logger:
            logger.record_event("test.settings")
        self.assertNotIn("model", read_database(self.folder / "logs/events.db")[0])

    def test_settings_defaults_and_limits(self):
        """CONFIG-05: exercise both accepted boundaries and each invalid setting category."""
        path, timeout, size, _ = load_logging_config(self.config)
        self.assertEqual(
            (path, timeout, size), (self.folder / "logs/events.db", 5, 1048576)
        )
        for key, accepted, rejected in (
            (
                "busy_timeout_seconds",
                (0.05, 60),
                (0, -1, 60.001, True, "5", float("nan"), float("inf")),
            ),
            (
                "max_event_bytes",
                (1024, 16777216),
                (1023, 16777217, True, "1024", 1024.5, float("nan"), float("inf")),
            ),
        ):
            for value in accepted:
                with self.subTest(key=key, value=value):
                    write_settings(self.folder, **{key: value})
                    values = load_logging_config(self.config)
                    self.assertEqual(
                        values[1 if key == "busy_timeout_seconds" else 2], value
                    )
            for value in rejected:
                with self.subTest(key=key, value=value):
                    write_settings(self.folder, **{key: value})
                    with self.assertRaises(LoggingConfigurationError):
                        OperationLogger(self.config).open()
                    self.assertFalse((self.folder / "events.db").exists())


class LoggingEventValidationTests(unittest.TestCase):
    def test_json_values_are_preserved_and_copied(self):
        """DATA-01, DATA-02: preserve Unicode/scalars and detach nested mutable input."""
        original = {"text": "Привет 🌍", "array": [None, True, 7, 2.5, {"name": "x"}]}
        copied = copy_json_object(original, "data")
        self.assertEqual(copied, original)
        original["array"][-1]["name"] = "changed"
        self.assertEqual(copied["array"][-1]["name"], "x")

    def test_non_json_and_invalid_unicode_are_rejected(self):
        """DATA-01: never coerce invalid keys, objects, nonfinite numbers, or cycles."""
        cycle = []
        cycle.append(cycle)
        for value in (
            {1: "x"},
            {"x": (1, 2)},
            {"x": object()},
            {"x": cycle},
            {"x": float("nan")},
            {"x": float("inf")},
            {"x": "\ud800"},
        ):
            with (
                self.subTest(value_type=type(value)),
                self.assertRaises((TypeError, ValueError)),
            ):
                copy_json_object(value, "data")

    def test_depth_is_measured_from_complete_envelope(self):
        """DATA-01, DATA-04: deepest value at 32 is accepted; 33 is rejected."""
        event = event_fixture()
        nested = None
        for _ in range(30):
            nested = [nested]
        event["data"] = {"nested": nested}
        self.assertEqual(json.loads(encode_event(event, 4096)), event)
        event["data"]["nested"] = [nested]
        with self.assertRaises(ValueError):
            encode_event(event, 4096)

    def test_context_fields_and_types(self):
        """DATA-03: accept known optional fields and reject invalid values or unknown names."""
        context = dict.fromkeys(
            (
                "run_id",
                "node_id",
                "node_execution_id",
                "runner_session_id",
                "service_instance_id",
                "module_name",
                "module_version",
                "module_hash",
                "configuration_hash",
                "worker_id",
                "host_name",
                "source",
                "parent_operation_id",
            ),
            "example",
        )
        context.update(attempt_number=1, process_id=10)
        self.assertEqual(validate_context(context), context)
        nullable = dict.fromkeys(context)
        self.assertEqual(validate_context(nullable), nullable)
        for field in ("attempt_number", "process_id"):
            for value in (0, -1, True, 1.5, "1"):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises((TypeError, ValueError)),
                ):
                    validate_context({field: value})
        for context in (
            {"run_id": " "},
            {"run_id": 1},
            {"run_id": "x\x00"},
            {"typo": "x"},
        ):
            with (
                self.subTest(context=context),
                self.assertRaises((TypeError, ValueError)),
            ):
                validate_context(context)

    def test_envelope_fields_utc_and_sequence(self):
        """DATA-04, DATA-05: validate literal envelope identities without payload dispatch."""
        event = event_fixture()
        self.assertEqual(json.loads(encode_event(event, 4096)), event)
        for field, value in (
            ("schema_version", 2),
            ("schema_version", True),
            ("event_id", ""),
            ("producer_instance_id", 1),
            ("event_type", ""),
            ("operation_id", 3),
            ("sequence_number", 0),
            ("sequence_number", True),
            ("sequence_number", 2**63),
            ("occurred_at", "2026-09-10T01:02:03"),
            ("occurred_at", "2026-09-10T01:02:03+01:00"),
            ("data", []),
        ):
            with (
                self.subTest(field=field, value=value),
                self.assertRaises((TypeError, ValueError)),
            ):
                encode_event({**event, field: value}, 4096)
        for changed in (
            {key: value for key, value in event.items() if key != "data"},
            {**event, "extra": 1},
        ):
            with self.assertRaises(ValueError):
                encode_event(changed, 4096)
        for kind in ("custom.first", "custom.second"):
            changed = {**event, "event_type": kind, "data": {"free": [1, 2]}}
            self.assertEqual(json.loads(encode_event(changed, 4096)), changed)

    def test_size_limit_counts_complete_utf8_envelope(self):
        """DATA-04: exact byte boundary includes Unicode and all metadata."""
        event = event_fixture()
        event["data"] = {"text": "Ж🌍" * 200}
        encoded = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
        byte_count = len(encoded.encode("utf-8"))
        self.assertEqual(encode_event(event, byte_count), encoded)
        with self.assertRaises(ValueError):
            encode_event(event, byte_count - 1)
