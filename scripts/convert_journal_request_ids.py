"""Convert a format-2 journal into a verified, separate format-3 database.

Run through uv with absolute --source and --destination paths. The source can
remain active, but the result represents only the committed captured boundary.
Stop writers before capturing a copy intended to replace an experiment journal.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from itertools import zip_longest
from pathlib import Path
from uuid import uuid4

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

# Direct script execution also works when the caller's cwd is outside checkout.
from core.journal.checkpoint_request_ids import (
    _prepare_checkpoint,
    _reconstruct_event,
)
from core.journal.events import LoggingError
from core.journal.records import _read_boundary, _read_journal_info
from core.journal.schema import (
    _APPLICATION_ID,
    STORAGE_SCHEMA_VERSION,
    _create_tables,
)
from core.journal.storage import SQLiteEventStore
from core.models.journal_checkpoint_ids import CheckpointIdHistory
from core.primitives.json_files import write_json
from core.primitives.json_values import JsonObject

_SCRATCH_ROOT = REPOSITORY_ROOT / ".artifacts/tmp/journal-conversion"
_REPORT_ROOT = REPOSITORY_ROOT / ".artifacts/logs/journal-conversion"
_FREE_RESERVE = 64 * 1024 * 1024
_TABLE_ORDER = (
    ("journal_info", "singleton"),
    ("events", "cursor"),
    ("command_results", "request_id"),
    ("journal_changes", "change_cursor"),
    ("journal_restorations", "restoration_id"),
    ("sqlite_sequence", "name"),
)


def _progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _validate_paths(source: Path, destination: Path) -> tuple[Path, Path]:
    if not source.is_absolute() or not destination.is_absolute():
        raise ValueError("Source and destination must be absolute paths.")
    if not source.is_file():
        raise FileNotFoundError(f"Source journal does not exist: {source}")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"Destination already exists: {destination}")
    source, destination = source.resolve(), destination.resolve()
    if source == destination:
        raise ValueError("Source and destination must be different files.")
    if not destination.parent.is_dir():
        raise FileNotFoundError(
            f"Destination directory does not exist: {destination.parent}"
        )
    return source, destination


def _check_space(requirements: list[tuple[Path, int]]) -> None:
    """Sum simultaneous storage requirements when paths share a filesystem."""
    volumes: dict[int, tuple[Path, int]] = {}
    for directory, required in requirements:
        device = directory.stat().st_dev
        previous = volumes.get(device, (directory, 0))[1]
        volumes[device] = directory, previous + required
    for directory, required in volumes.values():
        if shutil.disk_usage(directory).free < required + _FREE_RESERVE:
            raise OSError(f"Insufficient free space for conversion at {directory}.")


def _reader(path: Path, connection: sqlite3.Connection) -> SQLiteEventStore:
    """Configure a closed reader from metadata already read through SQLite."""
    info = _read_journal_info(connection)
    reader = SQLiteEventStore(
        path,
        busy_timeout_seconds=5,
        max_event_bytes=None,
        min_free_bytes=0,
        open_mode="existing",
        read_only=True,
        expected_journal={key: info[key] for key in ("journal_id", "generation")},
    )
    reader._report_local_failures = False
    return reader


def _preflight_space(source: Path, destination: Path) -> None:
    """Check room for both copies before starting the potentially large backup."""
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as live:
        if live.execute("PRAGMA user_version").fetchone()[0] != 2:
            raise ValueError(
                "The converter requires a storage-format-2 source journal."
            )
        size = (
            live.execute("PRAGMA page_size").fetchone()[0]
            * live.execute("PRAGMA page_count").fetchone()[0]
        )
    _check_space([(_SCRATCH_ROOT, size), (destination.parent, size * 2)])


def _capture_source(source: Path, captured: Path) -> JsonObject:
    """Pin a committed read view, including WAL, then release the live source."""
    initial_identity = source.stat()
    with closing(
        sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, isolation_level=None)
    ) as live:
        live.execute("PRAGMA query_only=ON")
        live.execute("BEGIN")
        if live.execute("PRAGMA user_version").fetchone()[0] != 2:
            raise ValueError(
                "The converter requires a storage-format-2 source journal."
            )
        _reader(source, live)._check_schema(live)
        boundary = _read_boundary(live).model_dump()
        size = (
            live.execute("PRAGMA page_size").fetchone()[0]
            * live.execute("PRAGMA page_count").fetchone()[0]
        )
        _check_space([(captured.parent, size)])
        wal = Path(str(source) + "-wal")
        observed = {
            "boundary": boundary,
            "source_bytes": source.stat().st_size,
            "source_wal_bytes": wal.stat().st_size if wal.exists() else 0,
        }
        with closing(sqlite3.connect(captured, isolation_level=None)) as backup:
            backup.execute("PRAGMA locking_mode=EXCLUSIVE")
            backup.execute("PRAGMA synchronous=FULL")
            live.backup(backup, pages=256, sleep=0.01)
            if backup.execute("PRAGMA journal_mode=DELETE").fetchone()[0] != "delete":
                raise ValueError("Captured journal must be independent of WAL files.")
        live.execute("ROLLBACK")
    current = source.stat()
    if (initial_identity.st_dev, initial_identity.st_ino) != (
        current.st_dev,
        current.st_ino,
    ):
        raise ValueError("Source path changed during capture; retry the conversion.")
    return observed


def _open_snapshot(path: Path) -> SQLiteEventStore:
    """Open an owned, closed copy; the caller closes the returned reader."""
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as metadata:
        reader = _reader(path, metadata)
    reader._open_snapshot()
    return reader


def _validate_snapshot(reader: SQLiteEventStore) -> None:
    connection = reader._connection
    if connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
        raise ValueError("Journal failed SQLite integrity validation.")
    reader._validate_snapshot_source(connection)


def _copy_table(
    source: sqlite3.Connection, target: sqlite3.Connection, table: str, order: str
) -> None:
    """Copy original values, including JSON text formatting, one row at a time."""
    for row in source.execute(f"SELECT * FROM {table} ORDER BY {order}"):
        placeholders = ",".join("?" for _ in row)
        target.execute(f"INSERT INTO {table} VALUES ({placeholders})", row)


def _anchor_after_restoration(
    target: sqlite3.Connection,
    history: CheckpointIdHistory,
    event: JsonObject,
    boundary: int,
) -> CheckpointIdHistory:
    if history.base_event_id is None or not boundary:
        return history
    previous = target.execute(
        "SELECT cursor FROM events WHERE event_id=?", (history.base_event_id,)
    ).fetchone()[0]
    if previous > boundary:
        return history
    return CheckpointIdHistory.model_validate(
        {
            **history.model_dump(),
            "base_event_id": None,
            "depth": 0,
            "ids": event["data"]["used_request_ids"],
        }
    )


def _copy_events(
    source: sqlite3.Connection, target: sqlite3.Connection, directory: Path
) -> None:
    restoration_boundary = 0
    for count, row in enumerate(
        source.execute("SELECT * FROM events ORDER BY cursor"), 1
    ):
        event = json.loads(row[4])
        if event["event_type"] == "experiment.restored":
            restoration_boundary = row[0]
        encoded, history = _prepare_checkpoint(target, event, row[4])
        if history is not None:
            history = _anchor_after_restoration(
                target, history, event, restoration_boundary
            )
        target.execute(
            "INSERT INTO events (cursor,event_id,producer_instance_id,sequence_number,event_json,payload_encoding) "
            "VALUES (?,?,?,?,?,?)",
            (*row[:4], encoded, "json" if history is None else "checkpoint_ids"),
        )
        if history is not None:
            values = history.model_dump()
            values["ids_json"] = json.dumps(values.pop("ids"), separators=(",", ":"))
            values["event_cursor"] = row[0]
            columns = ",".join(values)
            placeholders = ",".join("?" for _ in values)
            target.execute(
                f"INSERT INTO checkpoint_request_ids ({columns}) VALUES ({placeholders})",
                tuple(values.values()),
            )
        if count % 1000 == 0:
            _check_space([(directory, 0)])
            _progress(f"Converted {count} events")


def _write_destination(source: sqlite3.Connection, staged: Path) -> None:
    with closing(sqlite3.connect(staged, isolation_level=None)) as target:
        target.execute("PRAGMA journal_mode=DELETE")
        target.execute("PRAGMA synchronous=FULL")
        target.execute("PRAGMA foreign_keys=ON")
        target.execute("BEGIN IMMEDIATE")
        _create_tables(target)
        target.execute(f"PRAGMA application_id={_APPLICATION_ID}")
        target.execute(f"PRAGMA user_version={STORAGE_SCHEMA_VERSION}")
        target.execute("DELETE FROM journal_info")
        _copy_table(source, target, "journal_info", "singleton")
        _copy_events(source, target, staged.parent)
        for table, order in _TABLE_ORDER[2:-1]:
            _copy_table(source, target, table, order)
        target.execute("DELETE FROM sqlite_sequence")
        _copy_table(source, target, "sqlite_sequence", "name")
        target.execute("COMMIT")


def _compare_table(
    source: sqlite3.Connection,
    target: sqlite3.Connection,
    table: str,
    order: str,
    source_digest,
    target_digest,
) -> int:
    source_digest.update((table + "\n").encode("ascii"))
    target_digest.update((table + "\n").encode("ascii"))
    count = 0
    with (
        closing(source.execute(f"SELECT * FROM {table} ORDER BY {order}")) as old,
        closing(target.execute(f"SELECT * FROM {table} ORDER BY {order}")) as new,
    ):
        for count, (before, after) in enumerate(zip_longest(old, new), 1):
            if table == "events" and after is not None:
                after = _reconstruct_event(target, after)
            if before != after:
                raise ValueError(f"Verification mismatch in {table} at row {count}.")
            for digest, row in ((source_digest, before), (target_digest, after)):
                digest.update(
                    json.dumps(row, ensure_ascii=True, separators=(",", ":")).encode(
                        "ascii"
                    )
                    + b"\n"
                )
            if table == "events" and count % 1000 == 0:
                _progress(f"Verified {count} events")
    return count


def _verify_conversion(source: Path, staged: Path) -> JsonObject:
    """Reopen through the regular reader and compare the entire logical history."""
    with closing(_open_snapshot(source)) as old, closing(_open_snapshot(staged)) as new:
        _validate_snapshot(new)
        old_digest, new_digest = hashlib.sha256(), hashlib.sha256()
        counts = {
            table: _compare_table(
                old._connection, new._connection, table, order, old_digest, new_digest
            )
            for table, order in _TABLE_ORDER
        }
        boundary = _read_boundary(new._connection).model_dump()
        if boundary != _read_boundary(old._connection).model_dump():
            raise ValueError("Converted journal has a different read boundary.")
        full, delta = new._connection.execute(
            "SELECT count(*) FILTER (WHERE depth=0), count(*) FILTER (WHERE depth>0) "
            "FROM checkpoint_request_ids"
        ).fetchone()
        return {
            "boundary": boundary,
            "table_counts": counts,
            "event_count": counts["events"],
            "source_logical_sha256": old_digest.hexdigest(),
            "destination_logical_sha256": new_digest.hexdigest(),
            "anchor_count": full,
            "delta_count": delta,
            "sequence_high_water": dict(
                new._connection.execute("SELECT name,seq FROM sqlite_sequence")
            ),
        }


def _publish(staged: Path, destination: Path) -> None:
    """Publish a closed, verified file without overwriting a concurrent destination."""
    with staged.open("r+b") as stream:
        os.fsync(stream.fileno())
    # Both names are on the destination filesystem. link() atomically refuses an
    # existing name on Windows and POSIX, unlike POSIX rename/replace.
    os.link(staged, destination)
    staged.unlink()
    if os.name != "nt":
        descriptor = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _remove_work_directory(work: Path) -> None:
    """Remove an owned tree, allowing Windows to finish pending file deletion."""
    scratch = _SCRATCH_ROOT.resolve()
    if not scratch.is_relative_to(REPOSITORY_ROOT) or work.resolve().parent != scratch:
        raise ValueError("Refusing cleanup outside the conversion scratch directory.")
    try:
        shutil.rmtree(work)
        return
    except OSError as error:
        if (
            os.name != "nt"
            or getattr(error, "winerror", None) != 145
            or error.filename is None
            or Path(error.filename).resolve() != work.resolve()
        ):
            raise
    # Retry only removal of the empty root, never another recursive deletion.
    for attempt in range(3):
        time.sleep(0.05)
        try:
            work.rmdir()
            return
        except FileNotFoundError:
            return
        except OSError as error:
            if getattr(error, "winerror", None) != 145 or attempt == 2:
                raise


def _cleanup(work: Path, staged: Path | None) -> None:
    """Remove only owned scratch files; cleanup cannot mask the primary outcome."""
    try:
        if staged is not None:
            for suffix in ("", "-journal", "-wal", "-shm"):
                Path(str(staged) + suffix).unlink(missing_ok=True)
        _remove_work_directory(work)
    except (OSError, ValueError) as error:
        _progress(f"Temporary conversion files could not be removed: {error}")


def _write_report(
    source: Path,
    destination: Path,
    captured: Path,
    staged: Path,
    observed: JsonObject,
    verification: JsonObject,
    run_id: str,
    started: float,
) -> JsonObject:
    """Retain verification evidence before attempting final file publication."""
    report_path = _REPORT_ROOT / run_id / "report.json"
    source_size = captured.stat().st_size
    destination_size = staged.stat().st_size
    report = {
        **observed,
        **verification,
        "source": str(source),
        "destination": str(destination),
        "source_format": 2,
        "destination_format": STORAGE_SCHEMA_VERSION,
        "source_snapshot_bytes": source_size,
        "destination_bytes": destination_size,
        "reduction_bytes": source_size - destination_size,
        "reduction_percent": round(100 * (1 - destination_size / source_size), 3),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "report_path": str(report_path),
    }
    report_path.parent.mkdir(parents=True, exist_ok=False)
    write_json(report_path, report)
    return report


def convert_journal(source: Path, destination: Path) -> JsonObject:
    """Create a verified copy; never replace the source or start a journal writer."""
    started = time.monotonic()
    source, destination = _validate_paths(source, destination)
    _SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
    _preflight_space(source, destination)
    run_id = uuid4().hex
    work = _SCRATCH_ROOT / run_id
    work.mkdir()
    staged = None
    try:
        _progress("Capturing committed source history (including WAL)...")
        captured = work / "source.sqlite"
        observed = _capture_source(source, captured)
        source_size = captured.stat().st_size
        _check_space([(destination.parent, source_size * 2), (work, 0)])
        _progress("Validating captured source...")
        with closing(_open_snapshot(captured)) as original:
            _validate_snapshot(original)
            descriptor, filename = tempfile.mkstemp(
                prefix=f".journal-conversion-{run_id}-",
                suffix=".partial.sqlite",
                dir=destination.parent,
            )
            os.close(descriptor)
            staged = Path(filename)
            _progress("Writing compact journal...")
            _write_destination(original._connection, staged)
        _progress("Verifying complete logical history...")
        verification = _verify_conversion(captured, staged)
        if verification["boundary"] != observed["boundary"]:
            raise ValueError(
                "Captured history does not match the selected source boundary."
            )
        report = _write_report(
            source,
            destination,
            captured,
            staged,
            observed,
            verification,
            run_id,
            started,
        )
        _publish(staged, destination)
        return report
    finally:
        _cleanup(work, staged)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", required=True, type=Path, help="Absolute format-2 SQLite path"
    )
    parser.add_argument(
        "--destination", required=True, type=Path, help="Absolute new output path"
    )
    arguments = parser.parse_args()
    try:
        report = convert_journal(arguments.source, arguments.destination)
    except (OSError, ValueError, TypeError, sqlite3.Error, LoggingError) as error:
        print(f"Conversion failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print(
            "Conversion interrupted; the source journal was preserved.", file=sys.stderr
        )
        return 130
    # Keep piped output portable across Windows code pages; the report file is UTF-8.
    print(json.dumps(report, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
