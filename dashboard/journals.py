"""Local experiment discovery and incremental reads through the logger's public API."""

from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
from pathlib import Path

from core.logger import OperationLogger
from core.logger_utils.events import LoggingError
from core.runner_utils.runtimeio import read_json
from dashboard.api_client import SystemAPIError


def read_object(path: Path, maximum: int = 33554432) -> dict:
    return read_json(path, max_bytes=maximum)


class LocalJournals:
    def __init__(self, settings: dict) -> None:
        self.project = settings["project_root"]
        self.state_directory = settings["state_directory"] / "readers"
        self.max_events = settings["history_max_events"]
        self.max_bytes = settings["history_max_bytes"]
        self.interval = min(settings["refresh_seconds"], 5)
        self._cache: dict[str, dict] = {}
        self._snapshots: dict[str, dict] = {}
        self._lock = threading.RLock()

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

    def load(self, identifier: str, *, force: bool = False) -> dict:
        with self._lock:
            directory = self.registry().get(identifier)
            if directory is None:
                raise SystemAPIError("not_found", "Unknown experiment.", 404)
            previous = self._cache.get(identifier)
            try:
                if (
                    previous
                    and not force
                    and time.monotonic() - previous["refreshed"] < self.interval
                ):
                    identity = read_object(
                        self.safe_path(directory, "runner/journal.json")
                    )
                    status = self.safe_path(directory, "journals/events.sqlite").stat()
                    if (
                        identity == previous["identity"]
                        and (status.st_dev, status.st_ino) == previous["file_key"]
                    ):
                        return self._snapshots[identifier]
                state_path = self.safe_path(directory, "runner/state.json")
                state = read_object(state_path) if state_path.exists() else {}
                if state and state.get("experiment_id") != identifier:
                    raise ValueError(
                        "Experiment metadata belongs to a different experiment."
                    )
                identity_path = self.safe_path(directory, "runner/journal.json")
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
                file_status = database.stat()
                file_key = (file_status.st_dev, file_status.st_ino)
                if (
                    not previous
                    or previous["identity"] != identity
                    or previous["file_key"] != file_key
                ):
                    previous = {
                        "experiment_id": identifier,
                        "directory": directory,
                        "identity": identity,
                        "file_key": file_key,
                        "raw": {},
                        "metadata": {},
                        "event_checkpoint": None,
                        "change_checkpoint": None,
                        "bytes": 0,
                        "entries": [],
                        "complete": False,
                    }
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
                    "operation_context": {
                        "source": "dashboard",
                        "experiment_id": identifier,
                    },
                }
                self.state_directory.mkdir(parents=True, exist_ok=True)
                config_path = self.state_directory / (
                    hashlib.sha256(identifier.encode()).hexdigest() + ".json"
                )
                encoded = json.dumps(configuration, ensure_ascii=False)
                if (
                    not config_path.exists()
                    or config_path.read_text(encoding="utf-8") != encoded
                ):
                    temporary = config_path.with_suffix(".tmp")
                    temporary.write_text(encoded, encoding="utf-8")
                    temporary.replace(config_path)
                deadline = time.monotonic() + 5
                complete = True
                with OperationLogger(config_path, read_only=True) as reader:
                    while True:
                        page = reader.read_events(
                            previous["event_checkpoint"], limit=500
                        )
                        for entry in page["events"]:
                            event = entry["event"]
                            if event["context"].get("experiment_id") not in (
                                None,
                                identifier,
                            ):
                                raise ValueError(
                                    "The journal contains another experiment's events."
                                )
                            size = len(
                                json.dumps(event, ensure_ascii=False).encode("utf-8")
                            )
                            if event["event_id"] not in previous["raw"]:
                                if (
                                    len(previous["raw"]) >= self.max_events
                                    or previous["bytes"] + size > self.max_bytes
                                ):
                                    raise SystemAPIError(
                                        "history_limit",
                                        "Journal exceeds dashboard history limits; increase history_max_events/history_max_bytes.",
                                        413,
                                    )
                                previous["bytes"] += size
                            previous["raw"][event["event_id"]] = entry
                        previous["event_checkpoint"] = page["checkpoint"]
                        previous["boundary"] = page["boundary"]
                        if not page["has_more"]:
                            break
                        if time.monotonic() > deadline:
                            complete = False
                            break
                    while time.monotonic() <= deadline:
                        page = reader.read_changes(
                            previous["change_checkpoint"], limit=500
                        )
                        for change in page["changes"]:
                            entry = change["entry"]
                            event = entry["event"]
                            for record in (change["observed_event"], event):
                                existing = previous["raw"].get(record["event_id"])
                                if existing:
                                    existing["event"] = record
                            for related in change["related_event_ids"]:
                                previous["metadata"][related] = {
                                    "effective": related
                                    == change["effective_event_id"],
                                    "effective_author": change["effective_author"],
                                    "provisional": change["provisional"],
                                }
                            for superseded in event.get("data", {}).get(
                                "supersedes", []
                            ):
                                old = previous["raw"].get(superseded["event_id"])
                                if old:
                                    old["event"]["data"]["ignored"] = superseded[
                                        "ignored"
                                    ]
                        previous["change_checkpoint"] = page["checkpoint"]
                        if not page["has_more"]:
                            break
                    else:
                        complete = False
                    current_identity = reader.get_journal_info()
                    if any(
                        current_identity[key] != identity[key]
                        for key in ("journal_id", "generation")
                    ):
                        raise ValueError(
                            "Journal identity changed while it was being read."
                        )
                # The runner may replace its journal metadata immediately after the last read.
                if read_object(identity_path) != identity:
                    raise ValueError(
                        "Journal generation changed; retry with the new history."
                    )
                entries = []
                for event_id, entry in sorted(
                    previous["raw"].items(), key=lambda pair: pair[1]["cursor"]
                ):
                    event = entry["event"]
                    metadata = previous["metadata"].get(event_id, {})
                    entries.append(
                        {
                            **event,
                            "cursor": entry["cursor"],
                            **metadata,
                            "ignored": event["data"].get("ignored"),
                            "confirmation": "provisional"
                            if metadata.get("provisional")
                            else "confirmed"
                            if metadata.get("effective_author")
                            else "recorded",
                        }
                    )
                if previous.get("change_checkpoint") is None or (
                    previous["event_checkpoint"]["cursor"] < page["boundary"]["cursor"]
                ):
                    complete = False
                previous.update(
                    state=state,
                    entries=entries,
                    complete=complete,
                    error=None,
                    refreshed=time.monotonic(),
                )
                self._cache[identifier] = previous
                # Publish one detached snapshot per refresh. Dashboard consumers
                # treat it as read-only; mutable ingestion buffers never escape.
                self._snapshots[identifier] = copy.deepcopy(
                    {
                        key: previous[key]
                        for key in (
                            "experiment_id",
                            "directory",
                            "identity",
                            "state",
                            "entries",
                            "complete",
                            "error",
                            "refreshed",
                        )
                    }
                )
                return self._snapshots[identifier]
            except SystemAPIError:
                self._cache.pop(identifier, None)
                self._snapshots.pop(identifier, None)
                raise
            except (OSError, TypeError, ValueError, LoggingError) as error:
                self._cache.pop(identifier, None)
                self._snapshots.pop(identifier, None)
                raise SystemAPIError(
                    "journal_unavailable",
                    f"Cannot read experiment {identifier}: {error}",
                ) from error

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
        event = next(
            (
                entry
                for entry in dataset["entries"]
                if entry["event_type"] == "artifact.recorded"
                and entry["data"].get("artifact_id") == artifact_id
            ),
            None,
        )
        if event is None:
            raise SystemAPIError(
                "not_found", "Artifact is not present in this history.", 404
            )
        directory = dataset["directory"]
        if event["event_type"] == "artifact.recorded":
            context = event["context"]
            parameters = next(
                (
                    entry["context"]
                    for entry in dataset["entries"]
                    if entry["event_type"] == "attempt.parameters"
                    and entry["context"].get("attempt_id") == context.get("attempt_id")
                ),
                {},
            )
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
