"""Approved A1-A8, B2/B7/B8: explicit settings and the single event contract."""

import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

from core.logger import OperationLogger
from core.logger_utils.events import (
    LoggingConfigurationError,
    copy_json_object,
    encode_event,
    load_logging_settings,
    validate_checkpoint,
    validate_context,
    validate_journal_identity,
)
from core.logger_utils.filtered import FilteredJournal
from core.logger_utils.storage import SQLiteEventStore
from tests.helpers.logging_fixtures import BASE_CONTEXT
from tests.helpers.logging_process import (
    SCRATCH_ROOT,
    STORE_OPTIONS,
    LoggingProcess,
    cleanup_directory,
    event_fixture,
    write_settings,
)


class LoggingConfigurationTests(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.config = write_settings(self.folder, db_path="logs/events.db")

    def test_constructors_validate_paths_without_io_or_threads(self):
        """A1: all three constructors are inert."""
        with (
            patch.object(Path, "open") as opened,
            patch("sqlite3.connect") as connected,
            patch.object(threading.Thread, "start") as started,
        ):
            OperationLogger(self.config)
            SQLiteEventStore(self.folder / "events.db", **STORE_OPTIONS)
            FilteredJournal(self.config, self.folder / "view.db")
            for bad in ("", "relative.json", str(self.folder / "bad\x00.json")):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    OperationLogger(bad)
            for bad in (None, 42):
                with self.subTest(bad=bad), self.assertRaises(TypeError):
                    OperationLogger(bad)
            opened.assert_not_called()
            connected.assert_not_called()
            started.assert_not_called()

    def test_every_logging_field_is_required_and_unknown_fields_are_rejected(self):
        """A2: validation precedes creation, with no defaults or legacy selector."""
        original = json.loads(self.config.read_text(encoding="utf-8"))
        documents = []
        for field in original["logging"]:
            document = json.loads(json.dumps(original))
            del document["logging"][field]
            documents.append((field, document))
        for field in ("typo", "schema_version"):
            document = json.loads(json.dumps(original))
            document["logging"][field] = 1
            documents.append((field, document))
        documents.extend(
            (
                ("root", {}),
                ("logging-list", {"logging": []}),
                ("context", {**original, "operation_context": {"typo": "x"}}),
            )
        )
        for label, document in documents:
            with self.subTest(label=label):
                self.config.write_text(json.dumps(document), encoding="utf-8")
                with self.assertRaises(LoggingConfigurationError):
                    OperationLogger(self.config).open()
                self.assertFalse((self.folder / "logs").exists())

    def test_explicit_value_boundaries(self):
        """A2/B7: null disables only event size; boolean never substitutes a number."""
        for field, accepted, rejected in (
            ("busy_timeout_seconds", (0.01, 60), (0, -1, 60.1, True, "5", None)),
            ("max_event_bytes", (None, 1, 16777217), (0, -1, True, 1.5, "1")),
            ("min_free_bytes", (0, 1, 67108864), (-1, True, 1.5, "0", None)),
            (
                "filtered_refresh_interval_seconds",
                (0.01, 1, 600),
                (0, -1, True, "1", None),
            ),
        ):
            for value in accepted:
                with self.subTest(field=field, value=value):
                    config = write_settings(self.folder, **{field: value})
                    settings, _ = load_logging_settings(config)
                    self.assertEqual(settings[field], value)
            for value in (*rejected, float("nan"), float("inf")):
                with self.subTest(field=field, value=value):
                    config = write_settings(self.folder, **{field: value})
                    with self.assertRaises(LoggingConfigurationError):
                        load_logging_settings(config)
        self.assertFalse((self.folder / "events.db").exists())

    def test_expected_identity_is_explicit_normalized_and_detached(self):
        """A6: expected identity is exactly two UUIDs."""
        first, second = uuid4(), uuid4()
        identity = {"journal_id": str(first).upper(), "generation": str(second)}
        config = write_settings(
            self.folder, open_mode="existing", expected_journal=identity
        )
        settings, _ = load_logging_settings(config)
        self.assertEqual(
            settings["expected_journal"],
            {"journal_id": first.hex, "generation": second.hex},
        )
        identity["generation"] = "changed"
        for value in (
            None,
            {},
            {"journal_id": first.hex},
            {"journal_id": first.hex, "generation": 1},
            {"journal_id": first.hex, "generation": "invalid"},
            {"journal_id": first.hex, "generation": second.hex, "extra": 1},
        ):
            with self.subTest(value=value):
                config = write_settings(
                    self.folder, open_mode="existing", expected_journal=value
                )
                with self.assertRaises(LoggingConfigurationError):
                    load_logging_settings(config)
        config = write_settings(
            self.folder,
            expected_journal={"journal_id": first.hex, "generation": second.hex},
        )
        with self.assertRaises(LoggingConfigurationError):
            load_logging_settings(config)

    def test_invalid_configuration_preserves_cause(self):
        """A2: unreadable/malformed files cannot create a database."""
        for text in ("{", "[]", '{"logging": NaN}'):
            self.config.write_text(text, encoding="utf-8")
            with self.assertRaises(LoggingConfigurationError) as caught:
                OperationLogger(self.config).open()
            self.assertIsNotNone(caught.exception.__cause__)
        error = PermissionError("controlled config failure")
        with (
            patch.object(Path, "open", side_effect=error),
            self.assertRaises(LoggingConfigurationError) as caught,
        ):
            OperationLogger(self.config).open()
        self.assertIs(caught.exception.__cause__, error)
        self.assertFalse((self.folder / "logs").exists())

    def test_paths_survive_a_different_process_working_directory(self):
        """A3: relative paths are anchored to the containing configuration."""
        other = self.folder / "different-cwd"
        other.mkdir()
        for configured in ("logs/events.db", str(self.folder / "absolute.db")):
            with self.subTest(path=configured):
                config = write_settings(self.folder, db_path=configured)
                child = LoggingProcess("cwd", config, str(other))
                self.addCleanup(child.close)
                child.start()
                self.assertEqual(len(child.receive()["ids"]), 1)
                self.assertEqual(child.wait(), 0)
                target = Path(configured)
                if not target.is_absolute():
                    target = self.folder / target
                self.assertTrue(target.exists())
                self.assertEqual(list(other.iterdir()), [])

    @unittest.skipUnless(os.name == "nt", "Windows path syntax")
    def test_ambiguous_windows_paths_are_rejected(self):
        """A3: root-relative and drive-relative paths must not use caller cwd."""
        for value in (r"C:relative.db", r"\root-relative.db"):
            with self.subTest(value=value):
                config = write_settings(self.folder, db_path=value)
                with self.assertRaises(LoggingConfigurationError):
                    load_logging_settings(config)

    def test_version_controlled_initial_values_are_explicit(self):
        """A8: constructor defaults are data and never rescue an incomplete runtime JSON."""
        defaults = json.loads(
            (
                Path(__file__).resolve().parents[2] / "default_settings/logging.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            defaults,
            {
                "busy_timeout_seconds": 5,
                "max_event_bytes": None,
                "min_free_bytes": 67108864,
                "filtered_refresh_interval_seconds": 1,
            },
        )
        self.config.write_text('{"logging":{"db_path":"missing.db"}}', encoding="utf-8")
        with self.assertRaises(LoggingConfigurationError):
            load_logging_settings(self.config)


class ExtendedContextTests(unittest.TestCase):
    def test_full_context_and_nulls_preserve_distinct_ids(self):
        """B2: stage/cycle/attempt/service and lineage are independent."""
        copied = validate_context(BASE_CONTEXT)
        self.assertEqual(copied, BASE_CONTEXT)
        copied["run_id"] = "changed"
        self.assertNotEqual(copied, BASE_CONTEXT)
        self.assertEqual(
            validate_context(dict.fromkeys(BASE_CONTEXT)), dict.fromkeys(BASE_CONTEXT)
        )

    def test_invalid_context_counters_and_identifiers(self):
        """B2/B8: no coercion of identifiers or positive integer counters."""
        for field in BASE_CONTEXT:
            values = (
                (0, -1, True, 1.5, "1")
                if field in ("cycle_number", "attempt_number", "stage_position")
                else ("", " ", 1, "bad\x00")
            )
            for value in values:
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises((ValueError, TypeError)),
                ):
                    validate_context({field: value})

    def test_checkpoint_shape_and_uuid_validation(self):
        """D1: checkpoint counters and ownership cannot be confused."""
        identity = {"journal_id": uuid4().hex, "generation": uuid4().hex}
        for key in ("cursor", "change_cursor"):
            checkpoint = {**identity, key: 0}
            self.assertEqual(validate_checkpoint(checkpoint, key), checkpoint)
            for value in (-1, True, 1.5, "1", 2**63):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    validate_checkpoint({**identity, key: value}, key)
        self.assertEqual(validate_journal_identity(identity), identity)
        self.assertIsInstance(UUID(identity["generation"]), UUID)


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
                "participant_instance_id",
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
            ("schema_version", 1),
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
