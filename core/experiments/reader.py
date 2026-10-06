"""Read published experiment metadata without starting or restoring a runner."""

from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from uuid import UUID

from core.journal.storage import SQLiteEventStore
from core.models.artifacts import ArtifactLocation
from core.models.experiment_registry import RegistryEntry
from core.models.runner_state import SavedStateMetadata
from core.models.server_arguments import (
    ArtifactReference,
    ExperimentReference,
    NoArguments,
    SnapshotMetadataReference,
)
from core.models.snapshot_documents import SnapshotMetadata
from core.primitives.json_files import read_json
from core.primitives.json_values import JsonObject, copy_json_object, require_text


class ExperimentReader:
    """Read saved project metadata and resolve journaled artifacts within project roots."""
    def __init__(self, project_root: Path) -> None:
        """Bind a resolved absolute project root without reading project metadata."""
        if not project_root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._root = project_root.resolve()

    def _path(self, base: Path, relative: str) -> Path:
        """Resolve a path and require it to stay within both its base and the project.

        Args:
            base: Resolved directory within which the relative path must stay.
            relative: Path string resolved against base and checked against both
                base and project root.

        Returns:
            Resolved absolute path confined to both the supplied base and project
            root.
        """
        path = (base / relative).resolve()
        if not path.is_relative_to(base) or not path.is_relative_to(self._root):
            raise ValueError("Metadata path escapes its project directory.")
        return path

    def _registry(self) -> JsonObject:
        """Read the project experiment registry, returning an empty object when absent."""
        path = self._path(self._root, "experiments.json")
        return read_json(path) if path.exists() else {}

    def _directory(self, experiment_id: str, registry: JsonObject) -> Path:
        """Resolve a registered experiment folder within the project's experiments root.

        Args:
            experiment_id: Registered experiment identifier used to select saved
                state or history.
            registry: Experiment IDs mapped to their project-local folder names.

        Returns:
            Resolved experiment directory from its validated registry folder.
        """
        require_text(experiment_id, "experiment_id")
        if experiment_id not in registry:
            raise FileNotFoundError(f"Unknown experiment: {experiment_id}")
        try:
            folder = RegistryEntry.model_validate(
                {"folder": registry[experiment_id]}
            ).folder
        except (ValueError, TypeError) as error:
            raise ValueError(f"Invalid registry entry: {experiment_id}") from error
        base = self._path(self._root, "experiments")
        return self._path(base, folder)

    def inspect_experiment(self, experiment_id: str) -> JsonObject:
        """Return saved experiment state metadata without claiming current process liveness.

        Args:
            experiment_id: Registered experiment identifier used to select saved
                state or history.

        Returns:
            Saved experiment state metadata without claiming current process
            liveness.
        """
        directory = self._directory(experiment_id, self._registry())
        state = self._read_saved_state(directory, experiment_id)
        return {
            "experiment_id": experiment_id,
            "directory": str(directory),
            "source": "saved_state",
            "state": state.model_dump(exclude_unset=True),
        }

    def list_experiments(self) -> JsonObject:
        """List registered experiments with saved metadata or per-entry read errors.

        Returns:
            Items sorted by experiment ID, each with saved-state metadata or an
            availability error. Saved phase is not a live process observation.
        """
        registry = self._registry()
        items = []
        for identifier in sorted(registry):
            item = {"experiment_id": identifier, "source": "saved_state"}
            try:
                directory = self._directory(identifier, registry)
                state = self._read_saved_state(directory, identifier)
                template = copy_json_object(state.template, "saved template")
                item.update(
                    directory=str(directory),
                    name=template.get("name"),
                    phase=state.phase,
                    mode=state.mode,
                    available=True,
                    error=None,
                )
            except (OSError, ValueError, TypeError) as error:
                item.update(available=False, error=str(error))
            items.append(item)
        return {"items": items}

    def _read_saved_state(
        self, directory: Path, experiment_id: str
    ) -> SavedStateMetadata:
        """Read the inspection subset of state.json and verify its experiment identity.

        Args:
            directory: Resolved experiment root containing runner state and
                shared_artifacts.
            experiment_id: Registered experiment identifier used to select saved
                state or history.

        Returns:
            Validated inspection metadata preserving compatible historical extra
            fields.
        """
        state = read_json(self._path(directory, "runner/state.json"))
        if state.get("experiment_id") != experiment_id:
            raise ValueError("Saved state belongs to another experiment.")
        return SavedStateMetadata.model_validate(state)

    def inspect_snapshot(self, experiment_id: str, snapshot_id: str) -> JsonObject:
        """Read snapshot metadata and check identity without checking payload integrity.

        Args:
            experiment_id: Registered experiment identifier.
            snapshot_id: UUID of a snapshot belonging to that experiment.

        Returns:
            Snapshot metadata and manifest with integrity marked not_checked.

        Raises:
            ValueError: Manifest/state identity disagrees with its location.
            FileNotFoundError: The experiment or requested snapshot is unavailable.
        """
        snapshot_id = str(UUID(require_text(snapshot_id, "snapshot_id")))
        directory = self._directory(experiment_id, self._registry())
        base = self._path(self._root, "snapshots")
        base = self._path(base, directory.name)
        folder = self._path(base, snapshot_id)
        manifest = read_json(self._path(folder, "manifest.json"))
        metadata = SnapshotMetadata.model_validate(manifest)
        if (
            metadata.experiment_id != experiment_id
            or metadata.snapshot_id != snapshot_id
        ):
            raise ValueError("Snapshot manifest identity does not match its location.")
        state = metadata.state
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
        """List snapshot headers with per-item availability and unchecked integrity.

        Args:
            experiment_id: Registered experiment identifier used to select saved
                state or history.

        Returns:
            Experiment identity and snapshot metadata entries with independent
            availability errors; payload integrity is not checked.
        """
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
        """Read recorded artifacts up to the first observed journal boundary.

        Args:
            experiment_id: Experiment whose journal is opened read-only.

        Yields:
            Artifact event and merged attempt context. Closing the iterator releases
            the owned journal connection.
        """
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
                    artifact = self._artifact_entry(event, attempts)
                    if artifact is not None:
                        yield artifact
                checkpoint = page["checkpoint"]
                if checkpoint["cursor"] >= boundary or not page["has_more"]:
                    return
        finally:
            store.close()

    def _artifact_entry(
        self, event: JsonObject, attempts: dict[str, JsonObject]
    ) -> tuple[JsonObject, JsonObject] | None:
        """Track attempt context and return artifact events with their inherited coordinates.

        Args:
            event: Complete journal event envelope.
            attempts: Mutable attempt-ID/context mapping populated from earlier
                parameter events.

        Returns:
            Artifact event and merged attempt context, or None for other events.
            Parameter events update the supplied attempts mapping.
        """
        context = event["context"]
        # Continuations inherit events with their original experiment
        # IDs. The store checks journal identity; artifact paths are
        # resolved within the requested experiment's directory.
        attempt_id = context.get("attempt_id")
        if event["event_type"] == "attempt.parameters" and attempt_id:
            attempts[attempt_id] = context
        elif event["event_type"] == "artifact.recorded":
            return event, {**attempts.get(attempt_id, {}), **context}
        return None

    def _artifact_path(
        self, directory: Path, event: JsonObject, context: JsonObject
    ) -> Path:
        """Validate recorded artifact coordinates and resolve an existing confined file.

        Args:
            directory: Resolved experiment root containing runner state and
                shared_artifacts.
            event: Complete journal event envelope.
            context: Journal/participant coordinates associated with this operation.

        Returns:
            Existing absolute file path confined to the artifact's attempt
            directory.
        """
        location = ArtifactLocation.model_validate(
            {"context": context, "path": event["data"].get("path")}
        )
        return self._resolve_artifact_path(directory, location)

    def _resolve_artifact_path(
        self, directory: Path, location: ArtifactLocation
    ) -> Path:
        """Resolve an attempt-relative artifact path and require an existing regular file.

        Args:
            directory: Resolved experiment root containing runner state and
                shared_artifacts.
            location: Validated attempt coordinates and attempt-relative artifact
                path.

        Returns:
            Existing file path resolved under the attempt directory and project
            root.
        """
        context = location.context
        attempt = self._path(
            directory,
            f"shared_artifacts/epoch_{context.cycle_number}/{context.module_name}/"
            f"{context.stage_id}/attempt_{context.attempt_number}",
        )
        path = self._path(attempt, location.path)
        if not path.is_file():
            raise FileNotFoundError("Recorded artifact file is unavailable.")
        return path

    def list_artifacts(self, experiment_id: str) -> JsonObject:
        """List journaled artifacts with current file availability and experiment-relative paths.

        Args:
            experiment_id: Registered experiment identifier used to select saved
                state or history.

        Returns:
            Experiment identity and journaled artifact entries, each with file
            availability and an experiment-relative path when resolvable.
        """
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
        """Resolve a registered artifact to its current absolute local file path.

        Args:
            experiment_id: Experiment whose journal and artifact tree are inspected.
            artifact_id: Artifact identifier recorded in the journal.

        Returns:
            Experiment/artifact identity and absolute path to the available file.

        Raises:
            FileNotFoundError: No matching registration or file exists.
            ValueError: Artifact metadata or filesystem confinement is invalid.
        """
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
        """Validate and dispatch a saved-metadata read command.

        Args:
            command: Supported experiment, snapshot, or artifact stats command.
            args: JSON arguments validated for that command.
            selected_id: Current experiment used as the default for snapshot reads.

        Returns:
            Requested metadata; experiment-list entries include selection flags.
        """
        handlers = {
            "stats.artifacts": (self.list_artifacts, ExperimentReference),
            "stats.artifact": (self.get_artifact, ArtifactReference),
            "stats.experiments": (self.list_experiments, NoArguments),
            "stats.experiment": (self.inspect_experiment, ExperimentReference),
            "stats.snapshots": (self.list_snapshots, ExperimentReference),
            "stats.snapshot": (self.inspect_snapshot, SnapshotMetadataReference),
        }
        handler, model = handlers[command]
        args = dict(args)
        if command in ("stats.snapshots", "stats.snapshot"):
            args.setdefault("experiment_id", selected_id)
        arguments = model.model_validate(args)
        parameters = {
            name: getattr(arguments, name)
            for name in type(arguments).model_fields
            if name in arguments.model_fields_set
        }
        result = handler(**parameters)
        if command == "stats.experiments":
            for item in result["items"]:
                item["selected"] = item["experiment_id"] == selected_id
        return result
