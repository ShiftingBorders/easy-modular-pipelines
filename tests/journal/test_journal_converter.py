"""Approved C1-C3: standalone conversion, isolated failures, and normal adoption."""

import hashlib
import importlib
import json
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.journal.events import LoggingError
from core.journal.records import _decode_row
from tests.helpers.logging_fixtures import context_event
from tests.helpers.logging_process import (
    PROJECT_ROOT,
    SCRATCH_ROOT,
    cleanup_directory,
    make_store,
)
from tests.journal.test_checkpoint_request_ids import checkpoint_event, legacy_database

SCRIPT = PROJECT_ROOT / "scripts/convert_journal_request_ids.py"


def logical_tables(path):
    """Retain original cursor/text and auxiliary rows, independently of reports."""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("BEGIN")
        events = [
            (*row[:4], _decode_row(row, db).encoded_event)
            for row in db.execute("SELECT * FROM events ORDER BY cursor")
        ]
        return {
            "events": events,
            **{
                table: db.execute(f"SELECT * FROM {table} ORDER BY {order}").fetchall()
                for table, order in (
                    ("journal_info", "singleton"),
                    ("journal_changes", "change_cursor"),
                    ("command_results", "request_id"),
                    ("journal_restorations", "restoration_id"),
                    ("sqlite_sequence", "name"),
                )
            },
        }


class JournalConverterTests(unittest.TestCase):
    def setUp(self):
        SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=SCRATCH_ROOT)
        self.addCleanup(cleanup_directory, temporary)
        self.folder = Path(temporary.name)
        self.source = self.folder / "source.sqlite"
        self.destination = self.folder / "compact.sqlite"

    def converter(self):
        self.assertTrue(
            SCRIPT.is_file(), "The planned standalone converter is missing."
        )
        return importlib.import_module("scripts.convert_journal_request_ids")

    def make_source(self, count=70):
        legacy_database(self.source)
        store = make_store(self.source, open_mode="existing")
        self.addCleanup(store.close)
        store.open()
        store._connection.executemany(
            "INSERT INTO sqlite_sequence VALUES (?, ?)",
            [("events", 100), ("journal_changes", 200)],
        )
        store._connection.execute("PRAGMA wal_autocheckpoint=0")
        for number in range(1, count + 1):
            if number == count // 2 + 1:
                manifest = store.export_snapshot(
                    self.folder / "old-snapshot", min_free_bytes=0
                )
                store.close()
                store.complete_restore(
                    manifest, restoration_id=uuid4().hex, new_generation=uuid4().hex
                )
                store.open()
                store._connection.execute("PRAGMA wal_autocheckpoint=0")
                marker = context_event(800000)
                marker.update(
                    event_type="experiment.restored",
                    producer_instance_id="restoration-producer",
                )
                store.append(marker)
            event = checkpoint_event(number, 1000 + number)
            store.append(event)
            if number == 10:
                store._connection.execute(
                    "UPDATE events SET event_json=? WHERE event_id=?",
                    (json.dumps(event, ensure_ascii=True, indent=2), event["event_id"]),
                )
        for number, author in ((900000, "participant"), (900001, "runner")):
            result = context_event(number)
            result["event_type"] = "command.result"
            result["context"].update(
                request_id="request-1",
                participant_id="participant-A",
                participant_instance_id="instance-A",
            )
            result["data"] = {
                "request_id": "request-1",
                "author": author,
                "outcome": "succeeded",
                "response": {"value": "complete"},
                "ignored": None,
                "supersedes": [],
            }
            store.append_command_result(result)
        store._connection.execute("UPDATE sqlite_sequence SET seq=seq+1000")
        return store

    def test_cli_preserves_wal_history_and_original_text(self):
        """C1: convert a live WAL view from another cwd, preserving every logical row."""
        self.converter()
        source = self.make_source()
        before = logical_tables(self.source)
        files = [self.source, Path(str(self.source) + "-wal")]
        hashes = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in files}
        cwd = self.folder / "unrelated cwd"
        cwd.mkdir()
        result = subprocess.run(
            [
                "uv",
                "run",
                "--project",
                str(PROJECT_ROOT),
                "python",
                "-B",
                str(SCRIPT),
                "--source",
                str(self.source),
                "--destination",
                str(self.destination),
            ],
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=90,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(logical_tables(self.destination), before)
        self.assertEqual(logical_tables(self.source), before)
        self.assertEqual(
            {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in files},
            hashes,
        )
        self.assertEqual(summary["boundary"], source.read_events()["boundary"])
        self.assertEqual(summary["source_format"], 2)
        self.assertEqual(summary["destination_format"], 3)
        self.assertEqual(summary["event_count"], len(before["events"]))
        self.assertEqual(summary["destination_bytes"], self.destination.stat().st_size)
        self.assertGreater(summary["source_wal_bytes"], 0)
        self.assertLess(
            summary["destination_bytes"], summary["source_snapshot_bytes"] / 2
        )
        self.assertEqual(
            summary["source_logical_sha256"], summary["destination_logical_sha256"]
        )
        self.assertEqual(
            json.loads(Path(summary["report_path"]).read_text(encoding="utf-8")),
            summary,
        )
        self.assertFalse(Path(str(self.destination) + "-wal").exists())
        with closing(sqlite3.connect(self.destination)) as db:
            self.assertEqual(db.execute("PRAGMA journal_mode").fetchone()[0], "delete")
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(
                db.execute(
                    "SELECT depth FROM checkpoint_request_ids WHERE event_id='context-event-36'"
                ).fetchone()[0],
                0,
            )

    def test_failures_and_interruption_never_publish_partial_output(self):
        """C2: source isolation, explicit capture boundary, and no-overwrite publication."""
        converter = self.converter()
        source = self.make_source(count=2)
        before = logical_tables(self.source)
        for source_path, destination in (
            (Path("relative.sqlite"), self.destination),
            (self.source, Path("relative.sqlite")),
            (self.source, self.source),
        ):
            with (
                self.subTest(paths=(source_path, destination)),
                self.assertRaises((ValueError, OSError)),
            ):
                converter.convert_journal(source_path, destination)
        self.destination.write_bytes(b"owned by another caller")
        with self.assertRaises(FileExistsError):
            converter.convert_journal(self.source, self.destination)
        self.assertEqual(self.destination.read_bytes(), b"owned by another caller")
        self.destination.unlink()
        for method, failure in (
            ("_check_space", OSError("disk full")),
            ("_copy_events", OSError("write failed")),
            ("_verify_conversion", ValueError("content differs")),
        ):
            with (
                self.subTest(failure=method),
                patch.object(converter, method, side_effect=failure),
                self.assertRaises(type(failure)),
            ):
                converter.convert_journal(self.source, self.destination)
            self.assertFalse(self.destination.exists())
            self.assertEqual(logical_tables(self.source), before)
        publish = converter._publish

        def raced(staged, destination):
            destination.write_bytes(b"created during conversion")
            publish(staged, destination)

        with (
            patch.object(converter, "_publish", side_effect=raced),
            self.assertRaises(FileExistsError),
        ):
            converter.convert_journal(self.source, self.destination)
        self.assertEqual(self.destination.read_bytes(), b"created during conversion")
        self.destination.unlink()
        capture = converter._capture_source

        def write_after_capture(original, captured):
            result = capture(original, captured)
            source.append(checkpoint_event(1000000, 1003))
            return result

        with patch.object(
            converter, "_capture_source", side_effect=write_after_capture
        ):
            summary = converter.convert_journal(self.source, self.destination)
        self.assertEqual(logical_tables(self.destination), before)
        self.assertLess(
            summary["boundary"]["cursor"], source.read_events()["boundary"]["cursor"]
        )
        converted_again = self.folder / "again.sqlite"
        with self.assertRaises(ValueError):
            converter.convert_journal(self.destination, converted_again)
        source.close()
        before_interruption = logical_tables(self.source)
        interrupted = self.folder / "interrupted.sqlite"
        code = (
            "import os,sys; from pathlib import Path; "
            "from unittest.mock import patch; from scripts import convert_journal_request_ids as c; "
            "p=patch.object(c,'_verify_conversion',side_effect=lambda *a:os._exit(23)); p.start(); "
            "c.convert_journal(Path(sys.argv[1]),Path(sys.argv[2]))"
        )
        result = subprocess.run(
            [
                "uv",
                "run",
                "--locked",
                "python",
                "-B",
                "-c",
                code,
                str(self.source),
                str(interrupted),
            ],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=45,
        )
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertFalse(interrupted.exists())
        self.assertEqual(logical_tables(self.source), before_interruption)
        invalid = self.folder / "invalid.sqlite"
        shutil.copyfile(self.source, invalid)
        with closing(sqlite3.connect(invalid)) as db:
            db.execute(
                "UPDATE events SET event_json='{' WHERE cursor=(SELECT min(cursor) FROM events)"
            )
            db.commit()
        with self.assertRaises((ValueError, LoggingError)):
            converter.convert_journal(invalid, self.folder / "invalid-output.sqlite")
        self.assertFalse((self.folder / "invalid-output.sqlite").exists())

    def test_converted_copy_supports_replay_append_and_restore(self):
        """C3: adopting the copy retains sequence allocation and generation boundaries."""
        converter = self.converter()
        source = self.make_source(count=8)
        before = logical_tables(self.source)
        sequence = dict(before["sqlite_sequence"])["events"]
        converter.convert_journal(self.source, self.destination)
        converted = make_store(self.destination, open_mode="existing")
        self.addCleanup(converted.close)
        converted.open()
        self.assertEqual(
            converted.read_command_result("request-1"),
            source.read_command_result("request-1"),
        )
        converted._append_replayed(checkpoint_event(1, 1001))
        self.assertEqual(
            converted.read_events()["boundary"]["event_count"], len(before["events"])
        )
        converted.append(checkpoint_event(1000000, 3000))
        self.assertEqual(converted.read_events()["boundary"]["cursor"], sequence + 1)
        snapshot = self.folder / "new-snapshot"
        manifest = converted.export_snapshot(snapshot, min_free_bytes=0)
        converted.close()
        restored_path = self.folder / "restored.sqlite"
        shutil.copyfile(snapshot / "journal.sqlite", restored_path)
        restored = make_store(restored_path, open_mode="existing")
        self.addCleanup(restored.close)
        restored.complete_restore(
            manifest, restoration_id=uuid4().hex, new_generation=uuid4().hex
        )
        restored.open()
        restored.append(checkpoint_event(1000001, 3001))
        self.assertEqual(
            restored._connection.execute(
                "SELECT depth FROM checkpoint_request_ids WHERE event_id='context-event-1000001'"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(logical_tables(self.source), before)
