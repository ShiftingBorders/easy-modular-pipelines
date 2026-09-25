"""Read published experiment metadata without starting or restoring a runner."""

from collections.abc import Iterator
from contextlib import closing
from pathlib import Path, PurePosixPath, PureWindowsPath
from uuid import UUID

from core.logger_utils.events import copy_json_object, require_text
from core.logger_utils.storage import SQLiteEventStore
from core.runner_utils.runtimeio import read_json
from core.runner_utils.state import JsonObject


class ExperimentReader:
    def __init__(self, project_root: Path) -> None:
        if not project_root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._root = project_root.resolve()

    def _path(self, base: Path, relative: str) -> Path:
        path = (base / relative).resolve()
        if not path.is_relative_to(base) or not path.is_relative_to(self._root):
            raise ValueError("Metadata path escapes its project directory.")
        return path

    def _registry(self) -> JsonObject:
        path = self._path(self._root, "experiments.json")
        return read_json(path) if path.exists() else {}

    def _directory(self, experiment_id: str, registry: JsonObject) -> Path:
        require_text(experiment_id, "experiment_id")
        if experiment_id not in registry:
            raise FileNotFoundError(f"Unknown experiment: {experiment_id}")
        folder = registry[experiment_id]
        if (
            not isinstance(folder, str)
            or not folder
            or Path(folder).name != folder
            or folder in (".", "..")
        ):
            raise ValueError(f"Invalid registry entry: {experiment_id}")
        base = self._path(self._root, "experiments")
        return self._path(base, folder)

    def inspect_experiment(self, experiment_id: str) -> JsonObject:
        directory = self._directory(experiment_id, self._registry())
        state = read_json(self._path(directory, "runner/state.json"))
        if state.get("experiment_id") != experiment_id:
            raise ValueError("Saved state belongs to another experiment.")
        if state.get("schema_version") != 3:
            raise ValueError("Unsupported saved experiment state schema.")
        return {
            "experiment_id": experiment_id,
            "directory": str(directory),
            "source": "saved_state",
            "state": state,
        }

    def list_experiments(self) -> JsonObject:
        registry = self._registry()
        items = []
        for identifier in sorted(registry):
            item = {"experiment_id": identifier, "source": "saved_state"}
            try:
                directory = self._directory(identifier, registry)
                state = read_json(self._path(directory, "runner/state.json"))
                if state.get("experiment_id") != identifier:
                    raise ValueError("Saved state belongs to another experiment.")
                if state.get("schema_version") != 3:
                    raise ValueError("Unsupported saved experiment state schema.")
                template = copy_json_object(state.get("template"), "saved template")
                item.update(
                    directory=str(directory),
                    name=template.get("name"),
                    phase=state.get("phase"),
                    mode=state.get("mode"),
                    available=True,
                    error=None,
                )
            except (OSError, ValueError, TypeError) as error:
                item.update(available=False, error=str(error))
            items.append(item)
        return {"items": items}

    def inspect_snapshot(self, experiment_id: str, snapshot_id: str) -> JsonObject:
        snapshot_id = str(UUID(require_text(snapshot_id, "snapshot_id")))
        directory = self._directory(experiment_id, self._registry())
        base = self._path(self._root, "snapshots")
        base = self._path(base, directory.name)
        folder = self._path(base, snapshot_id)
        manifest = read_json(self._path(folder, "manifest.json"))
        if manifest.get("schema_version") != 2:
            raise ValueError("Unsupported snapshot manifest schema.")
        if (
            manifest.get("experiment_id") != experiment_id
            or manifest.get("snapshot_id") != snapshot_id
        ):
            raise ValueError("Snapshot manifest identity does not match its location.")
        state = copy_json_object(manifest.get("state"), "snapshot state")
        if state.get("experiment_id") != experiment_id:
            raise ValueError("Snapshot state belongs to another experiment.")
        return {
            "experiment_id": experiment_id,
            "snapshot_id": snapshot_id,
            "created_at": manifest.get("created_at"),
            "label": manifest.get("label"),
            "kind": manifest.get("kind"),
            "cycle_number": state.get("cycle_number"),
            "stage_position": state.get("stage_position"),
            "template_revision_id": state.get("template_revision_id"),
            "integrity": "not_checked",
            "manifest": manifest,
        }

    def list_snapshots(self, experiment_id: str) -> JsonObject:
        directory = self._directory(experiment_id, self._registry())
        base = self._path(self._root, "snapshots")
        base = self._path(base, directory.name)
        items = []
        for path in sorted(base.glob("*/manifest.json")):
            item = {"snapshot_id": path.parent.name, "integrity": "not_checked"}
            try:
                details = self.inspect_snapshot(experiment_id, path.parent.name)
                details.pop("manifest")
                item.update(details, available=True, error=None)
            except (OSError, ValueError, TypeError) as error:
                item.update(available=False, error=str(error))
            items.append(item)
        return {"experiment_id": experiment_id, "items": items}

    def _artifact_records(
        self, experiment_id: str
    ) -> Iterator[tuple[JsonObject, JsonObject]]:
        directory = self._directory(experiment_id, self._registry())
        identity = read_json(self._path(directory, "runner/journal.json"))
        store = SQLiteEventStore(
            self._path(directory, "journals/events.sqlite"),
            busy_timeout_seconds=5,
            max_event_bytes=None,
            open_mode="existing",
            min_free_bytes=0,
            expected_journal=identity,
            read_only=True,
        )
        try:
            store.open()
            checkpoint = None
            boundary = None
            attempts = {}
            while True:
                page = store.read_events(checkpoint, limit=1000)
                if boundary is None:
                    boundary = page["boundary"]["cursor"]
                for entry in page["events"]:
                    if entry["cursor"] > boundary:
                        return
                    event = entry["event"]
                    context = event["context"]
                    # Continuations inherit events with their original experiment
                    # IDs. The store checks journal identity; artifact paths are
                    # resolved within the requested experiment's directory.
                    attempt_id = context.get("attempt_id")
                    if event["event_type"] == "attempt.parameters" and attempt_id:
                        attempts[attempt_id] = context
                    elif event["event_type"] == "artifact.recorded":
                        yield event, {**attempts.get(attempt_id, {}), **context}
                checkpoint = page["checkpoint"]
                if checkpoint["cursor"] >= boundary or not page["has_more"]:
                    return
        finally:
            store.close()

    def _artifact_path(
        self, directory: Path, event: JsonObject, context: JsonObject
    ) -> Path:
        for name in ("cycle_number", "attempt_number"):
            if type(context.get(name)) is not int or context[name] < 1:
                raise ValueError(f"Artifact context lacks a valid {name}.")
        for name in ("attempt_id", "module_name", "stage_id"):
            component = require_text(context.get(name), name)
            if component in (".", "..") or any(c in component for c in '/\\:*?"<>|'):
                raise ValueError(f"Invalid artifact context: {name}.")
        attempt = self._path(
            directory,
            f"shared_artifacts/epoch_{context['cycle_number']}/{context['module_name']}/"
            f"{context['stage_id']}/attempt_{context['attempt_number']}",
        )
        relative = require_text(event["data"].get("path"), "artifact path")
        windows = PureWindowsPath(relative)
        posix = PurePosixPath(relative.replace("\\", "/"))
        if (
            windows.drive
            or windows.root
            or posix.is_absolute()
            or ".." in posix.parts
            or ":" in relative
        ):
            raise ValueError("Artifact path must remain inside its attempt directory.")
        path = self._path(attempt, posix.as_posix())
        if not path.is_file():
            raise FileNotFoundError("Recorded artifact file is unavailable.")
        return path

    def list_artifacts(self, experiment_id: str) -> JsonObject:
        directory = self._directory(experiment_id, self._registry())
        items = []
        with closing(self._artifact_records(experiment_id)) as records:
            for event, context in records:
                item = {
                    **event["data"],
                    "event_id": event["event_id"],
                    "context": context,
                }
                try:
                    path = self._artifact_path(directory, event, context)
                    item.update(
                        available=True,
                        experiment_path=path.relative_to(directory).as_posix(),
                        error=None,
                    )
                except (OSError, ValueError, TypeError) as error:
                    item.update(available=False, error=str(error))
                items.append(item)
        return {"experiment_id": experiment_id, "items": items}

    def get_artifact(self, experiment_id: str, artifact_id: str) -> JsonObject:
        require_text(artifact_id, "artifact_id")
        directory = self._directory(experiment_id, self._registry())
        with closing(self._artifact_records(experiment_id)) as records:
            for event, context in records:
                if event["data"].get("artifact_id") == artifact_id:
                    path = self._artifact_path(directory, event, context)
                    return {
                        "experiment_id": experiment_id,
                        "artifact_id": artifact_id,
                        "path": str(path),
                    }
        raise FileNotFoundError("Artifact is not present in the experiment journal.")

    def read(
        self, command: str, args: JsonObject, selected_id: str | None
    ) -> JsonObject:
        handlers = {
            "stats.artifacts": (self.list_artifacts, {"experiment_id"}),
            "stats.artifact": (self.get_artifact, {"experiment_id", "artifact_id"}),
            "stats.experiments": (self.list_experiments, set()),
            "stats.experiment": (self.inspect_experiment, {"experiment_id"}),
            "stats.snapshots": (self.list_snapshots, {"experiment_id"}),
            "stats.snapshot": (self.inspect_snapshot, {"experiment_id", "snapshot_id"}),
        }
        handler, allowed = handlers[command]
        if args.keys() - allowed:
            raise ValueError("Unknown metadata read arguments.")
        args = dict(args)
        if command in ("stats.snapshots", "stats.snapshot"):
            args.setdefault("experiment_id", selected_id)
        for field in allowed:
            require_text(args.get(field), field)
        result = handler(**args)
        if command == "stats.experiments":
            for item in result["items"]:
                item["selected"] = item["experiment_id"] == selected_id
        return result
