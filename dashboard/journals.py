"""Local experiment discovery and incremental reads through the logger's public API."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
import weakref
from collections import OrderedDict
from contextlib import ExitStack, closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from core.historycache import (
    HistoryCacheBusy,
    HistoryCacheChanged,
    HistoryCacheLimit,
    JournalHistoryCache,
    acquire_cache_writer,
)
from core.logger import OperationLogger
from core.logger_utils.events import LoggingError
from core.runner_utils.runtimeio import read_json, write_json
from dashboard.api_client import SystemAPIError
from dashboard.projections import (
    compact_event,
    instant,
    module_statistics,
    project_scope,
)


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
        publication = {}
        try:
            publication["modules_published"] = journals.publish_modules()
        except Exception as error:  # noqa: BLE001 - Module publication failure must not invalidate journal projections.
            publication["modules_error"] = str(error)
        return {
            "experiment_id": identifier,
            "pid": os.getpid(),
            "complete": complete,
            **publication,
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


def publish_module_statistics(settings: dict) -> bool:
    """Process-pool entry point for a final project-wide publication."""
    journals = LocalJournals(settings)
    try:
        return journals.publish_modules()
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
        self._module_signature: tuple | None = None
        self._module_publication: dict | None = None
        self._read_snapshots: dict[str, tuple[tuple, dict]] = {}
        self._windows: dict[str, tuple[dict, tuple, OrderedDict, dict]] = {}
        self._timeline_overviews: OrderedDict[tuple, dict] = OrderedDict()
        self._timeline_lock = threading.Lock()

    def cached(self, identifier: str) -> dict:
        """Load a published dataset using only the derived database's metadata."""
        key = hashlib.sha256(identifier.encode()).hexdigest()
        path = self.state_directory / f"{key}.cache.sqlite"
        if not path.exists():
            return self._pending_dataset(identifier)
        try:
            return self._read_cached(identifier, path, key)
        except sqlite3.OperationalError as error:
            if "no such table" in str(error):
                return self._pending_dataset(identifier)
            raise SystemAPIError("cache_unavailable", str(error)) from error
        except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
            raise SystemAPIError(
                "cache_unavailable", f"Cannot read cached history: {error}"
            ) from error

    def _read_cached(self, identifier: str, path: Path, key: str) -> dict:
        with (
            self._lock,
            closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db,
        ):
            db.execute("BEGIN")
            metadata = dict(
                db.execute(
                    "SELECT key,value FROM metadata WHERE key IN ('source','version','reader_revision','ready','cached_through','publication_boundary')"
                )
            )
            if not metadata.get("reader_revision") or "version" not in metadata:
                return self._pending_dataset(identifier)
            source = json.loads(metadata["source"])
            status = path.stat()
            signature = (
                status.st_dev,
                status.st_ino,
                metadata["source"],
                metadata["version"],
                metadata["reader_revision"],
                metadata.get("ready"),
                metadata.get("cached_through"),
                metadata.get("publication_boundary"),
            )
            previous = self._read_snapshots.get(identifier)
            if previous and previous[0] == signature:
                dataset = previous[1]
            else:
                values = dict(
                    db.execute(
                        "SELECT key,value FROM metadata WHERE key IN ('reader_context','ready','cached_through','boundary','publication_boundary')"
                    )
                )
                context = json.loads(values["reader_context"])
                if (
                    context["project_root"] != str(self.project)
                    or source["experiment_id"] != identifier
                ):
                    return self._pending_dataset(identifier)
                identity, file_key = source["identity"], tuple(source["file_key"])
                reader = JournalHistoryCache(
                    path,
                    self.state_directory / f"{key}.json",
                    identity,
                    file_key,
                    identifier,
                    self.window_events,
                    self.max_bytes,
                    self.max_events,
                )
                latest = db.execute(
                    "SELECT occurred_at FROM facts ORDER BY cursor DESC LIMIT 1"
                ).fetchone()
                boundary = json.loads(values.get("boundary", "null"))
                target_key = (
                    "publication_boundary" if values.get("ready") == "1" else "boundary"
                )
                dataset = {
                    "experiment_id": identifier,
                    "directory": Path(context["directory"]),
                    "identity": identity,
                    "file_key": file_key,
                    "state": context["state"],
                    "entries": [],
                    "cache": reader,
                    "version": int(metadata["version"]),
                    "complete": values.get("ready") == "1",
                    "cached_through": json.loads(values["cached_through"]),
                    "boundary": boundary,
                    "target_boundary": json.loads(
                        values.get(target_key, values.get("boundary", "null"))
                    ),
                    "observed_at": latest[0] if latest else None,
                    "gap": None,
                    "error": None,
                    "refreshed": time.monotonic(),
                }
                self._read_snapshots[identifier] = (signature, dataset)
            window = self._windows.get(identifier)
            if (
                window
                and window[0] == dataset["identity"]
                and window[1] == dataset["file_key"]
            ):
                dataset["cache"].window = window[2]
                first = next(iter(window[2].values()), None)
                dataset["window_start_cursor"] = (
                    first["entry"]["cursor"] if first else None
                )
                dataset["window_count"] = len(window[2])
                observed = window[3].get("boundary") or {}
                requested = dataset.get("target_boundary") or {}
                if observed.get("change_cursor", 0) > requested.get("change_cursor", 0):
                    dataset["target_boundary"] = observed
                cached = dataset["cached_through"]
                if cached["cursor"] < observed.get("cursor", 0) or cached[
                    "change_cursor"
                ] < observed.get("change_cursor", 0):
                    dataset["complete"] = False
                predecessor = window[3].get("window_predecessor_cursor")
                cached_end = dataset["cached_through"]["cursor"]
                if predecessor is not None and cached_end < predecessor:
                    dataset["gap"] = {
                        "after": cached_end,
                        "before": dataset["window_start_cursor"],
                    }
                    dataset["complete"] = False
            return dataset

    def _pending_dataset(self, identifier: str) -> dict:
        return {
            "experiment_id": identifier,
            "identity": None,
            "state": {},
            "entries": [],
            "complete": False,
            "error": "History is being prepared in the background.",
            "refreshed": None,
        }

    def modules(self) -> dict:
        """Serve a ready publication without touching any original journal."""
        if self.project is None:
            raise SystemAPIError(
                "not_configured", "Configure project_root to read module statistics."
            )
        path = self.state_directory / "modules.json"
        with self._lock:
            try:
                status = path.stat()
            except FileNotFoundError:
                return {
                    "items": [],
                    "complete": False,
                    "error": "Open an experiment or run precache to prepare module statistics.",
                }
            except OSError as error:
                raise SystemAPIError(
                    "cache_unavailable", f"Cannot inspect module statistics: {error}"
                ) from error
            signature = (
                status.st_dev,
                status.st_ino,
                status.st_mtime_ns,
                status.st_size,
            )
            if signature == self._module_signature:
                return self._module_publication
            try:
                document = read_object(path, self.settings["max_response_bytes"])
            except (OSError, TypeError, ValueError) as error:
                raise SystemAPIError(
                    "cache_unavailable", f"Cannot read module statistics: {error}"
                ) from error
            if document.get("schema_version") != 1 or document.get(
                "project_root"
            ) != str(self.project):
                return {
                    "items": [],
                    "complete": False,
                    "error": "Module statistics have not been prepared for this project.",
                }
            if (
                not isinstance(document.get("items"), list)
                or not isinstance(document.get("sources"), dict)
                or type(document.get("complete")) is not bool
            ):
                raise SystemAPIError(
                    "cache_unavailable", "Invalid module statistics publication."
                )
            self._module_publication = document
            self._module_signature = signature
            return document

    def publish_modules(self) -> bool:
        """Materialize exact project statistics from consistent cache snapshots."""
        if self.project is None:
            return True
        path = self.state_directory / "modules.json"
        try:
            writer = acquire_cache_writer(path)
        except HistoryCacheBusy:
            return False
        with writer, ExitStack() as resources:
            registry = self.registry()
            models, sources = self._module_sources(registry, resources)
            signature = {
                "schema_version": 1,
                "project_root": str(self.project),
                "sources": sources,
            }
            previous = (
                read_object(path, self.settings["max_response_bytes"])
                if path.exists()
                else {}
            )
            if all(previous.get(key) == value for key, value in signature.items()):
                return True
            complete = all(source["complete"] for source in sources.values())
            items = module_statistics(models)
            for item in items:
                item["complete"] = item["complete"] and complete
            document = {
                **signature,
                "items": items,
                "complete": complete,
                "error": None
                if complete
                else "Some experiment histories are incomplete; statistics cover the published records only.",
                "published_at": datetime.now(UTC).isoformat(),
            }
            if (
                len(json.dumps(document, ensure_ascii=False).encode("utf-8"))
                > self.settings["max_response_bytes"]
            ):
                raise HistoryCacheLimit("Module statistics exceed max_response_bytes.")
            write_json(path, document)
            return True

    def _module_sources(
        self, registry: dict[str, Path], resources: ExitStack
    ) -> tuple[list[tuple[dict, sqlite3.Connection]], dict[str, dict]]:
        models, sources = [], {}
        for identifier, directory in registry.items():
            key = hashlib.sha256(identifier.encode()).hexdigest()
            database = self.state_directory / f"{key}.cache.sqlite"
            source = {"directory": str(directory), "complete": False}
            sources[identifier] = source
            if not database.exists():
                source["error"] = "Cache is not initialized."
                continue
            try:
                connection = resources.enter_context(
                    closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True))
                )
                connection.execute("PRAGMA query_only=ON")
                connection.execute("BEGIN")
                metadata = dict(connection.execute("SELECT key, value FROM metadata"))
                identity = json.loads(metadata["source"])
                configuration = read_object(
                    self.state_directory / f"{key}.json", 1048576
                )
                if (
                    identity["experiment_id"] != identifier
                    or configuration["logging"]["expected_journal"]
                    != identity["identity"]
                    or Path(configuration["logging"]["db_path"]).resolve()
                    != (directory / "journals/events.sqlite").resolve()
                ):
                    source["error"] = "Cache belongs to a different journal."
                    continue
                source.update(
                    journal=identity["identity"],
                    file_key=identity["file_key"],
                    cache_schema_version=identity["version"],
                    version=int(metadata.get("version", 0)),
                    cached_through=json.loads(metadata.get("cached_through", "null")),
                    complete=metadata.get("ready") == "1",
                )
                template = connection.execute(
                    "SELECT json_extract(compact,'$.data.template.name') FROM facts WHERE kind='template.applied' AND effective=1 ORDER BY cursor DESC LIMIT 1"
                ).fetchone()
                name = template[0] if template and template[0] else identifier
                models.append(
                    (
                        {
                            "experiment_id": identifier,
                            "name": name,
                            "complete": source["complete"],
                        },
                        connection,
                    )
                )
            except (sqlite3.Error, ValueError, KeyError, OSError) as error:
                source.update(complete=False, error=str(error))
        return models, sources

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

    def scheduling_states(self, registry: dict[str, Path]) -> dict[str, dict]:
        """Read runner metadata for scheduling without opening stopped journals."""
        states = {}
        for identifier, directory in registry.items():
            try:
                path = self.safe_path(directory, "runner/state.json")
                state = read_object(path) if path.exists() else {}
                if state.get("experiment_id") not in (None, identifier):
                    raise ValueError("Experiment state belongs to another experiment.")
                template = state.get("template", {})
                if not isinstance(template, dict):
                    raise TypeError("Experiment state template must be a JSON object.")
                states[identifier] = {
                    "phase": state.get("phase", "unknown"),
                    "mode": state.get("mode"),
                    "name": template.get("name") or identifier,
                }
            except (OSError, ValueError, TypeError) as error:
                states[identifier] = {"phase": "unknown", "error": str(error)}
        return states

    def preview(self, identifier: str) -> dict | None:
        """Render a small complete RAM window while its disk cache is constructed."""
        directory = self.registry().get(identifier)
        if directory is None:
            return None
        state = read_object(self.safe_path(directory, "runner/state.json"))
        if state.get("experiment_id") != identifier:
            return None
        identity = read_object(self.safe_path(directory, "runner/journal.json"))
        database = self.safe_path(directory, "journals/events.sqlite")
        status = database.stat()
        file_key = (status.st_dev, status.st_ino)
        config = self._reader_configuration(identifier, state, database, identity)
        limit = min(500, self.window_events)
        with OperationLogger(config, read_only=True) as source:
            boundary = source.read_event_batch([])["boundary"]
            if boundary["event_count"] > limit:
                return None
            page = source.read_events(limit=limit, view="raw")
            if page["has_more"] or page["boundary"] != boundary:
                return None
            entries, window, results, size = [], OrderedDict(), {}, 0
            for item in page["events"]:
                event = item["event"]
                if event["context"].get("experiment_id") not in (None, identifier):
                    return None
                encoded_size = len(json.dumps(item, ensure_ascii=False).encode("utf-8"))
                size += encoded_size
                if size > min(self.max_bytes, self.settings["max_response_bytes"]):
                    return None
                metadata = {
                    "effective": True,
                    "effective_author": None,
                    "provisional": False,
                    "ignored": event["data"].get("ignored"),
                    "confirmation": "recorded",
                }
                if event["event_type"] == "command.result":
                    request_id = event["data"]["request_id"]
                    if request_id not in results:
                        results[request_id] = source.read_command_result(request_id)
                    result = results[request_id]
                    observation = next(
                        row
                        for row in result["observations"]
                        if row["event_id"] == event["event_id"]
                    )
                    metadata.update(
                        effective=event["event_id"] == result["event_id"],
                        effective_author=result["author"],
                        provisional=result["provisional"],
                        ignored=observation["ignored"],
                        confirmation="provisional"
                        if result["provisional"]
                        else "confirmed",
                    )
                entries.append({**event, "cursor": item["cursor"], **metadata})
                window[event["event_id"]] = {"entry": item, "size": encoded_size}
            if source.read_event_batch([])["boundary"] != boundary:
                return None
        self._windows[identifier] = (identity, file_key, window, {"boundary": boundary})
        return {
            "experiment_id": identifier,
            "directory": directory,
            "identity": identity,
            "file_key": file_key,
            "state": state,
            "entries": entries,
            "complete": True,
            "error": None,
            "boundary": boundary,
            "target_boundary": boundary,
            "cached_through": {**identity, "cursor": 0, "change_cursor": 0},
            "window_start_cursor": page["events"][0]["cursor"]
            if page["events"]
            else None,
            "window_count": len(window),
            "gap": None,
            "refreshed": time.monotonic(),
            "source": "ram_window",
        }

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
                state,
                compact_event,
                project_scope,
                target=target,
                window=window,
                reader_context={
                    "directory": str(directory),
                    "project_root": str(self.project),
                },
            )
        else:
            publication = reader.observe(target)
        if read_object(identity_path) != identity:
            raise ValueError("Journal generation changed while reading history.")
        old = self._snapshots.get(identifier)
        previous["checked"] = time.monotonic()
        self._cache[identifier] = previous
        if window:
            self._windows[identifier] = (
                identity,
                file_key,
                OrderedDict(reader.window),
                publication,
            )
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
            "file_key": file_key,
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
            self._read_snapshots.clear()
            self._windows.clear()
        with self._timeline_lock:
            self._timeline_overviews.clear()

    def read_view(self, dataset: dict, reader, *args) -> dict:
        """Keep a multi-query response within its advertised cache publication."""
        try:
            return dataset["cache"].read_view(
                dataset["version"], reader, dataset, *args
            )
        except HistoryCacheChanged as error:
            raise SystemAPIError("history_changed", str(error), 409) from error
        except HistoryCacheLimit as error:
            raise SystemAPIError("history_limit", str(error), 413) from error
        except (LoggingError, OSError, sqlite3.Error) as error:
            raise SystemAPIError("journal_unavailable", str(error)) from error

    def timeline(self, dataset: dict, params: dict, observed_at: str | None) -> dict:
        """Read a bounded time slice and its ancestors from the existing cache."""
        cache = dataset["cache"]
        scope = [
            dataset["identity"],
            params.get("run_id"),
            params.get("since"),
            params.get("until"),
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
                    raise ValueError("Timeline changed.")
                position = cursor["position"]
                if (
                    not isinstance(position, list)
                    or len(position) != 3
                    or type(position[0]) is not int
                    or not all(isinstance(value, str) for value in position[1:])
                ):
                    raise ValueError("Invalid timeline cursor.")
            except (KeyError, TypeError, ValueError) as error:
                raise SystemAPIError(
                    "history_changed", "Refresh this timeline range.", 409
                ) from error
        limit = int(params.get("limit", 200))
        if not 1 <= limit <= 1000:
            raise SystemAPIError(
                "invalid_limit", "limit must be between 1 and 1000.", 400
            )
        start_sql = "julianday(json_extract(payload,'$.started_at'))"
        # Open operations extend only to the last available observation.
        end_sql = f"MAX({start_sql},COALESCE(julianday(json_extract(payload,'$.finished_at')),julianday(?),{start_sql}))"
        where, args = "kind='operations'", []
        if params.get("run_id"):
            where += " AND run_id=?"
            args.append(params["run_id"])
        where += f" AND {start_sql} IS NOT NULL"
        # Reader identity changes on replacement/rebuild, even if version resets.
        # Weak references avoid retaining readers and their raw payload windows.
        overview_key = (weakref.ref(cache), dataset["version"], params.get("run_id"))
        with self._timeline_lock:
            overview = self._timeline_overviews.get(overview_key)
            if overview is not None:
                self._timeline_overviews.move_to_end(overview_key)
        if overview is None:
            finish_sql = "julianday(json_extract(payload,'$.finished_at'))"
            recorded_end = f"MAX({start_sql},COALESCE({finish_sql},{start_sql}))"
            first, last, count, has_open = cache.query(
                f"SELECT MIN({start_sql}),MAX({recorded_end}),COUNT(*),MAX({finish_sql} IS NULL) FROM records WHERE {where}",
                tuple(args),
            )[0]
            first_ms = (
                round((first - 2440587.5) * 86400000) if first is not None else None
            )
            last_ms = round((last - 2440587.5) * 86400000) if last is not None else None
            histogram = [0] * 64
            if count:
                bins = cache.query(
                    f"SELECT MIN(63,CAST((ROUND(({start_sql}-2440587.5)*86400000)-?)*64.0/? AS INTEGER)),COUNT(*) FROM records WHERE {where} GROUP BY 1",
                    (first_ms, max(last_ms - first_ms, 1), *args),
                )
                for bucket, number in bins:
                    histogram[max(0, bucket)] = number
            overview = {
                "has_open": bool(has_open),
                "timeline": {
                    "start": first_ms,
                    "end": last_ms,
                    "histogram_end": last_ms,
                    "histogram": histogram,
                    "operation_count": count,
                },
            }
            with self._timeline_lock:
                self._timeline_overviews[overview_key] = overview
                self._timeline_overviews.move_to_end(overview_key)
                while len(self._timeline_overviews) > 32:
                    self._timeline_overviews.popitem(last=False)
        timeline = {
            **overview["timeline"],
            "histogram": list(overview["timeline"]["histogram"]),
        }
        # Elapsed live time changes the axis, not the recorded histogram buckets.
        # Keeping their own domain avoids rescanning or approximate rebinning.
        observed = instant(observed_at)
        if overview["has_open"] and observed is not None:
            timeline["end"] = max(timeline["end"], round(observed * 1000))
        if params.get("since") is not None:
            where += f" AND {start_sql}<=julianday(?) AND {end_sql}>=julianday(?)"
            args.extend((params["until"], observed_at, params["since"]))
        total = timeline["operation_count"]
        if params.get("since") is not None:
            total = cache.query(
                f"SELECT COUNT(*) FROM records WHERE {where}", tuple(args)
            )[0][0]
        rows = cache.query(
            f"SELECT position,record_key,scope,payload FROM records WHERE {where} "
            "AND (position,record_key,scope)>(?,?,?) ORDER BY position,record_key,scope LIMIT ?",
            (*args, *position, limit + 1),
        )
        selected = rows[:limit]
        seeds = json.dumps(
            [{"key": key, "scope": partition} for _, key, partition, _ in selected]
        )
        ancestors = cache.query(
            """WITH RECURSIVE tree(record_key,scope,run_id,payload) AS (
                SELECT r.record_key,r.scope,r.run_id,r.payload
                FROM json_each(?) seed CROSS JOIN records r
                WHERE r.kind='operations' AND r.record_key=json_extract(seed.value,'$.key')
                  AND r.scope=json_extract(seed.value,'$.scope')
                UNION
                SELECT p.record_key,p.scope,p.run_id,p.payload
                FROM tree child CROSS JOIN records p
                WHERE p.kind='operations' AND p.run_id IS child.run_id
                  AND p.record_key=json_extract(child.payload,'$.parent_operation_id')
            ) SELECT payload FROM tree LIMIT 1001""",
            (seeds,),
        )
        if len(ancestors) > 1000:
            raise SystemAPIError(
                "history_limit",
                "Too many timeline ancestors; request a smaller page.",
                413,
            )
        items = []
        for (encoded,) in ancestors:
            row = json.loads(encoded)
            if row.get("detail_ref"):
                row["detail_ref"] = {**row["detail_ref"], **dataset["identity"]}
            items.append(row)
        return {
            "items": items,
            "total": total,
            "timeline": timeline,
            "next_cursor": {
                "scope": scope,
                "version": dataset["version"],
                "at": time.time(),
                "position": list(selected[-1][:3]),
            }
            if len(rows) > limit
            else None,
        }

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
        if not isinstance(reference, dict):
            raise SystemAPIError(
                "invalid_reference", "Detail reference must be an object.", 400
            )
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
            or any(not isinstance(key, str) or not key for key in identifiers)
        ):
            raise SystemAPIError(
                "invalid_reference", "Detail requires a bounded list of event IDs.", 400
            )
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
