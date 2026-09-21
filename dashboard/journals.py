"""Local experiment discovery and incremental reads through the logger's public API."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from uuid import uuid4

from core.historycache import HistoryCacheBusy, HistoryCacheLimit, JournalHistoryCache
from core.logger_utils.events import LoggingError
from core.runner_utils.runtimeio import read_json
from dashboard.api_client import SystemAPIError
from dashboard.projections import compact_event, project_scope


def read_object(path: Path, maximum: int = 33554432) -> dict:
    return read_json(path, max_bytes=maximum)


def cache_experiment(
    settings: dict, identifier: str, target: dict | None = None
) -> dict:
    """One bounded process job; only plain progress data crosses the process boundary."""
    journals = LocalJournals(settings)
    try:
        dataset = journals.load(identifier, force=True, window=False, target=target)
        if "cache" not in dataset:
            raise SystemAPIError(
                "journal_unavailable",
                dataset.get("error") or "The journal is unavailable.",
            )
        cached, boundary = dataset["cached_through"], dataset["target_boundary"]
        complete = (
            cached["cursor"] >= boundary["cursor"]
            and cached["change_cursor"] >= boundary["change_cursor"]
        )
        return {
            "experiment_id": identifier,
            "pid": os.getpid(),
            "complete": complete,
            **{key: dataset[key] for key in ("cached_through", "target_boundary")},
        }
    except Exception as error:  # noqa: BLE001 - A failed experiment must not break the worker pool.
        return {
            "experiment_id": identifier,
            "pid": os.getpid(),
            "complete": False,
            "error": {
                "code": getattr(error, "code", "cache_failed"),
                "message": str(error),
            },
        }
    finally:
        journals.close()


class LocalJournals:
    def __init__(self, settings: dict) -> None:
        self.settings = settings
        self.project = settings["project_root"]
        self.state_directory = settings["state_directory"] / "readers"
        self.max_events = settings["history_max_events"]
        self.max_bytes = settings["history_max_bytes"]
        self.interval = min(settings["refresh_seconds"], 5)
        self._cache: dict[str, dict] = {}
        self._snapshots: dict[str, dict] = {}
        self.window_events = settings.get("history_window_events", 1000)
        self._lock = threading.RLock()
        self._experiment_locks: dict[str, threading.RLock] = {}

    def registry(self) -> dict[str, Path]:
        if self.project is None:
            return {}
        registry_path = self.project / "experiments.json"
        if not registry_path.exists():
            return {}
        document = read_object(registry_path)
        base = (self.project / "experiments").resolve()
        if not base.is_relative_to(self.project):
            raise ValueError("Experiment directory escapes the configured project.")
        result = {}
        for identifier, folder in document.items():
            if (
                not isinstance(folder, str)
                or Path(folder).name != folder
                or folder in {".", ".."}
            ):
                raise ValueError("Invalid experiment registry entry.")
            directory = (base / folder).resolve()
            if not directory.is_relative_to(base):
                raise ValueError("Registered experiment escapes the project.")
            result[identifier] = directory
        return result

    def load(
        self,
        identifier: str,
        *,
        force: bool = False,
        build: bool = True,
        window: bool = True,
        target: dict | None = None,
    ) -> dict:
        with self._lock:
            lock = self._experiment_locks.setdefault(identifier, threading.RLock())
        with lock:
            directory = self.registry().get(identifier)
            if directory is None:
                raise SystemAPIError("not_found", "Unknown experiment.", 404)
            try:
                return self._load_directory(
                    identifier, directory, force, build, window, target
                )
            except SystemAPIError:
                raise
            except HistoryCacheBusy as error:
                raise SystemAPIError("cache_busy", str(error)) from error
            except HistoryCacheLimit as error:
                raise SystemAPIError("history_limit", str(error), 413) from error
            except (
                OSError,
                TypeError,
                ValueError,
                LoggingError,
                sqlite3.Error,
            ) as error:
                raise SystemAPIError(
                    "journal_unavailable",
                    f"Cannot read experiment {identifier}: {error}",
                ) from error

    def _load_directory(
        self,
        identifier: str,
        directory: Path,
        force: bool,
        build: bool,
        window: bool,
        target: dict | None,
    ) -> dict:
        identity_path = self.safe_path(directory, "runner/journal.json")
        state_path = self.safe_path(directory, "runner/state.json")
        state = read_object(state_path) if state_path.exists() else {}
        if state and state.get("experiment_id") != identifier:
            raise ValueError("Experiment metadata belongs to a different experiment.")
        if not identity_path.exists():
            return {
                "experiment_id": identifier,
                "directory": directory,
                "state": state,
                "entries": [],
                "identity": None,
                "complete": False,
                "error": "No journal has been published for this experiment.",
                "refreshed": time.monotonic(),
            }
        identity = read_object(identity_path)
        database = self.safe_path(directory, "journals/events.sqlite")
        status = database.stat()
        file_key = (status.st_dev, status.st_ino)
        previous = self._cache.get(identifier)
        if previous and (
            previous["identity"] != identity or previous["file_key"] != file_key
        ):
            previous["reader"].close()
            previous = None
        if (
            previous
            and build
            and not force
            and time.monotonic() - previous["checked"] < self.interval
        ):
            return self._snapshots[identifier]
        config_path = self._reader_configuration(identifier, state, database, identity)
        if previous is None:
            reader = JournalHistoryCache(
                config_path.with_suffix(".cache.sqlite"),
                config_path,
                identity,
                file_key,
                identifier,
                self.window_events,
                self.max_bytes,
                self.max_events,
            )
            previous = {"reader": reader, "identity": identity, "file_key": file_key}
        reader = previous["reader"]
        if build:
            publication = reader.refresh(
                state, compact_event, project_scope, target=target, window=window
            )
        else:
            publication = reader.observe(target)
        if read_object(identity_path) != identity:
            raise ValueError("Journal generation changed while reading history.")
        old = self._snapshots.get(identifier)
        previous["checked"] = time.monotonic()
        self._cache[identifier] = previous
        if (
            old
            and old.get("version") == publication["version"]
            and old["state"] == state
            and old["identity"] == identity
            and old["complete"] == publication["complete"]
            and old.get("boundary") == publication.get("boundary")
            and old.get("target_boundary") == publication.get("target_boundary")
        ):
            return old
        # Only the configured RAM window is detached for legacy reader consumers.
        available = publication.get("cache_available", True)
        entries = reader.events(list(reader.window)) if window and available else []
        snapshot = {
            "experiment_id": identifier,
            "directory": directory,
            "identity": identity,
            "state": state,
            "entries": entries,
            **publication,
            "error": None if available else "The history cache is being initialized.",
            "refreshed": time.monotonic(),
        }
        if available:
            snapshot["cache"] = reader
        self._snapshots[identifier] = snapshot
        return snapshot

    def _reader_configuration(
        self, identifier: str, state: dict, database: Path, identity: dict
    ) -> Path:
        logging = state.get("template", {}).get("logging")
        if not isinstance(logging, dict):
            raise TypeError("Recorded logging settings are missing.")
        configuration = {
            "logging": {
                **logging,
                "db_path": str(database),
                "open_mode": "existing",
                "expected_journal": identity,
            },
            "operation_context": {"source": "dashboard", "experiment_id": identifier},
        }
        self.state_directory.mkdir(parents=True, exist_ok=True)
        path = self.state_directory / (
            hashlib.sha256(identifier.encode()).hexdigest() + ".json"
        )
        encoded = json.dumps(configuration, ensure_ascii=False)
        if not path.exists() or path.read_text(encoding="utf-8") != encoded:
            temporary = path.with_name(f".{path.stem}-{uuid4().hex}.tmp")
            temporary.write_text(encoded, encoding="utf-8")
            temporary.replace(path)
        return path

    def close(self) -> None:
        with self._lock:
            for identifier, entry in self._cache.items():
                with self._experiment_locks[identifier]:
                    entry["reader"].close()
            self._cache.clear()
            self._snapshots.clear()

    def read_view(self, dataset: dict, reader, *args) -> dict:
        """Keep a multi-query response within its advertised cache publication."""
        identifier = dataset["experiment_id"]
        with self._experiment_locks[identifier]:
            current = self._snapshots.get(identifier)
            version = dataset["cache"].query(
                "SELECT value FROM metadata WHERE key='version'"
            )
            if (
                current is not dataset
                or not version
                or int(version[0][0]) != dataset["version"]
            ):
                raise SystemAPIError(
                    "history_changed",
                    "The history publication changed; refresh the selection.",
                    409,
                )
            try:
                return dataset["cache"].read_view(
                    dataset["version"], reader, dataset, *args
                )
            except HistoryCacheLimit as error:
                raise SystemAPIError("history_limit", str(error), 413) from error
            except (LoggingError, OSError, sqlite3.Error) as error:
                raise SystemAPIError("journal_unavailable", str(error)) from error

    def page(self, dataset: dict, view: str, params: dict) -> dict:
        cache = dataset["cache"]
        run_id = params.get("run_id")
        scope = [
            dataset["identity"],
            run_id,
            view,
            params.get("view", "effective"),
            params.get("revision"),
        ]
        position = [0, "", ""]
        if params.get("cursor"):
            try:
                cursor = json.loads(params["cursor"])
                if (
                    cursor["scope"] != scope
                    or cursor["version"] != dataset["version"]
                    or time.time() - cursor["at"] > 300
                ):
                    raise ValueError("History changed.")
                position = cursor["position"]
                if len(position) != 3 or type(position[0]) is not int:
                    raise ValueError("Invalid cursor position.")
            except (KeyError, TypeError, ValueError) as error:
                raise SystemAPIError(
                    "history_changed", "Refresh this history publication.", 409
                ) from error
        limit = int(params.get("limit", 200))
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000.")
        if view == "events":
            return self._event_page(dataset, params, scope, position, limit)
        if view == "runs":
            return self._run_page(dataset, params, scope, position, limit)
        selection, args = "kind=?", [view]
        if run_id:
            selection += " AND run_id=?"
            args.append(run_id)
        if view == "measurements" and params.get("revision"):
            selection += " AND revision=?"
            args.append(params["revision"])
        total = cache.query(
            "SELECT COUNT(*) FROM records WHERE " + selection, tuple(args)
        )[0][0]
        rows = cache.query(
            "SELECT position, record_key, scope, payload FROM records WHERE "
            + selection
            + " AND (position, record_key, scope)>(?, ?, ?) ORDER BY position, record_key, scope LIMIT ?",
            (*args, *position, limit + 1),
        )
        selected, size, last = [], 1024, position
        for number, key, partition, encoded in rows[:limit]:
            item = json.loads(encoded)
            item["detail_ref"] = (
                {**item["detail_ref"], **dataset["identity"]}
                if item.get("detail_ref")
                else None
            )
            if params.get("compact") != "1" and item["detail_ref"]:
                item = self.detail(dataset, item["detail_ref"], item)
            size += len(json.dumps(item, ensure_ascii=False).encode("utf-8"))
            if size > self.settings["max_response_bytes"]:
                if not selected:
                    raise SystemAPIError(
                        "response_too_large",
                        "This record exceeds the history byte budget.",
                        413,
                    )
                break
            selected.append(item)
            last = [number, key, partition]
        more = len(rows) > len(selected)
        return {
            "items": selected,
            "total": total,
            "next_cursor": {
                "scope": scope,
                "version": dataset["version"],
                "position": last,
                "at": time.time(),
            }
            if more
            else None,
        }

    def _run_page(
        self, dataset: dict, params: dict, scope: list, position: list, limit: int
    ) -> dict:
        where, args = (
            ("run_id=?", [params["run_id"]]) if params.get("run_id") else ("1=1", [])
        )
        cache = dataset["cache"]
        rows = cache.query(
            "SELECT first_cursor, run_id, started_at, revision FROM runs WHERE "
            + where
            + " AND (first_cursor,run_id)>(?,?) ORDER BY first_cursor,run_id LIMIT ?",
            (*args, position[0], position[1], limit + 1),
        )
        items = [
            {
                "run_id": run,
                "started_at": started,
                "template_revision_id": revision,
                "status": "recorded",
            }
            for _, run, started, revision in rows[:limit]
        ]
        total = cache.query("SELECT COUNT(*) FROM runs WHERE " + where, tuple(args))[0][
            0
        ]
        return {
            "items": items,
            "total": total,
            "next_cursor": {
                "scope": scope,
                "version": dataset["version"],
                "at": time.time(),
                "position": [rows[len(items) - 1][0], rows[len(items) - 1][1], ""],
            }
            if len(rows) > len(items)
            else None,
        }

    def _event_page(
        self, dataset: dict, params: dict, scope: list, position: list, limit: int
    ) -> dict:
        cache = dataset["cache"]
        selection, args = "1=1", []
        if params.get("run_id"):
            selection += " AND run_id=?"
            args.append(params["run_id"])
        if params.get("view", "effective") == "effective":
            selection += " AND effective=1"
        total = cache.query(
            "SELECT COUNT(*) FROM facts WHERE " + selection, tuple(args)
        )[0][0]
        records = cache.query(
            "SELECT cursor, event_id, compact FROM facts WHERE "
            + selection
            + " AND cursor>? ORDER BY cursor LIMIT ?",
            (*args, position[0], limit + 1),
        )
        items, size = [], 1024
        if params.get("compact") == "1":
            events = iter(json.loads(row[2]) for row in records[:limit])
        else:
            events = cache.iter_events([row[1] for row in records[:limit]])
        try:
            for event in events:
                event["detail_ref"] = {
                    "event_ids": [event["event_id"]],
                    "kind": "events",
                    **dataset["identity"],
                }
                size += len(json.dumps(event, ensure_ascii=False).encode("utf-8"))
                if size > self.settings["max_response_bytes"]:
                    if not items:
                        raise SystemAPIError(
                            "response_too_large",
                            "This event exceeds the response byte budget.",
                            413,
                        )
                    break
                items.append(event)
        finally:
            events.close()
        return {
            "items": items,
            "total": total,
            "next_cursor": {
                "scope": scope,
                "version": dataset["version"],
                "position": [records[len(items) - 1][0], "", ""],
                "at": time.time(),
            }
            if len(records) > len(items)
            else None,
        }

    def detail(self, dataset: dict, reference: dict, row: dict | None = None) -> dict:
        if any(
            reference.get(key) != dataset["identity"][key]
            for key in ("journal_id", "generation")
        ):
            raise SystemAPIError(
                "history_changed", "This detail belongs to replaced history.", 409
            )
        identifiers = reference.get("event_ids")
        if (
            not isinstance(identifiers, list)
            or not 1 <= len(identifiers) <= 1000
            or any(not isinstance(key, str) for key in identifiers)
        ):
            raise ValueError("Detail requires a bounded list of event IDs.")
        events = dataset["cache"].events(identifiers)
        kind = reference.get("kind")
        if kind == "events":
            return events[0]
        result = dict(row or {})
        result["source_events"] = events
        if kind == "commands":
            result["observations"] = events
        for event in events:
            if event["event_type"] == "attempt.parameters":
                result.update(event["data"])
            if event["event_type"] == "error.recorded":
                result.update(event["data"])
        return result

    def safe_path(self, directory: Path, relative: str) -> Path:
        path = (directory / relative).resolve()
        if not path.is_relative_to(directory.resolve()):
            raise ValueError("Requested file escapes the experiment.")
        return path

    def snapshots(self, identifier: str) -> list[dict]:
        directory = self.registry().get(identifier)
        if directory is None:
            raise SystemAPIError("not_found", "Unknown experiment.", 404)
        root = (self.project / "snapshots" / directory.name).resolve()
        if not root.is_relative_to(self.project):
            raise ValueError("Snapshot directory escapes the project.")
        result = []
        for path in sorted(root.glob("*/manifest.json")):
            if not path.resolve().is_relative_to(root):
                continue
            try:
                manifest = read_object(path)
                state = manifest.get("state", {})
                result.append(
                    {
                        "snapshot_id": path.parent.name,
                        "created_at": manifest.get("created_at"),
                        "kind": manifest.get("kind"),
                        "label": manifest.get("label"),
                        "template_revision_id": state.get("template_revision_id"),
                        "cycle_number": state.get("cycle_number"),
                        "journal": manifest.get("journal"),
                        "services": manifest.get("services"),
                        "file_count": len(manifest.get("files", {})),
                        "validation": {
                            "runner_state": bool(state),
                            "template": bool(state.get("template")),
                            "journal_boundary": bool(manifest.get("journal")),
                        },
                        "status": "published",
                        "integrity": "checked_on_restore",
                        "available": True,
                    }
                )
            except (ValueError, OSError):
                result.append(
                    {
                        "snapshot_id": path.parent.name,
                        "status": "unavailable",
                        "available": False,
                    }
                )
        return result

    def artifact(self, identifier: str, artifact_id: str) -> Path:
        dataset = self.load(identifier, force=True)
        records = dataset["cache"].query(
            "SELECT payload FROM records WHERE kind='artifacts' AND record_key=? LIMIT 1",
            (artifact_id,),
        )
        event = (
            dataset["cache"].events(
                json.loads(records[0][0])["detail_ref"]["event_ids"]
            )[0]
            if records
            else None
        )
        if event is None:
            raise SystemAPIError(
                "not_found", "Artifact is not present in this history.", 404
            )
        directory = dataset["directory"]
        if event["event_type"] == "artifact.recorded":
            context = event["context"]
            rows = dataset["cache"].query(
                "SELECT compact FROM facts WHERE attempt_id=? AND kind='attempt.parameters' ORDER BY cursor DESC LIMIT 1",
                (context.get("attempt_id"),),
            )
            parameters = json.loads(rows[0][0])["context"] if rows else {}
            context = {**parameters, **context}
            if any(
                context.get(key) is None
                for key in (
                    "attempt_id",
                    "cycle_number",
                    "module_name",
                    "stage_id",
                    "attempt_number",
                )
            ):
                raise SystemAPIError(
                    "not_found",
                    "The artifact's attempt directory is not recorded.",
                    404,
                )
            relative_directory = f"shared_artifacts/epoch_{context['cycle_number']}/{context['module_name']}/{context['stage_id']}/attempt_{context['attempt_number']}"
            directory = self.safe_path(directory, relative_directory)
            relative_file = event["data"]["path"]
        if not isinstance(relative_file, str) or not relative_file:
            raise SystemAPIError(
                "not_found", "The artifact has no recorded local path.", 404
            )
        try:
            path = self.safe_path(directory, relative_file)
        except ValueError as error:
            raise SystemAPIError(
                "invalid_artifact", "The recorded artifact escapes its directory.", 400
            ) from error
        if not path.is_file():
            raise SystemAPIError(
                "not_found", "Recorded artifact file is unavailable.", 404
            )
        return path
