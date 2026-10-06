"""Runner-local journal ownership and per-process client configurations."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import UUID, uuid4, uuid5

from core.experiments.state import RunnerState, state_from_document
from core.journal.logger import OperationLogger
from core.journal.storage import SQLiteEventStore
from core.models.experiment_template import ExperimentTemplate
from core.models.journal_diagnostics import JournalSnapshotManifest
from core.primitives.json_files import read_json, write_json
from core.primitives.json_values import JsonObject, copy_json_object, require_text


class RunnerJournal:
    """Own the runner's logger and publish identity-bound client configurations."""
    client: OperationLogger
    reader_config_path: Path | None = None

    def open(self, state: RunnerState, *, create: bool) -> None:
        """Open a new/existing experiment journal and publish its identity and reader config.

        Args:
            state: Experiment state supplying paths, context, and logging policy.
            create: Whether to create a new journal instead of checking its saved identity.

        Raises:
            RuntimeError: The runner journal is already open.
        """
        if getattr(self, "_opened", False):
            raise RuntimeError("Runner journal is already open.")
        if create:
            self._identity = None
        else:
            self._identity = read_json(
                state.experiment_directory / "runner" / "journal.json"
            )
        context = {
            "source": "runner",
            "experiment_id": state.experiment_id,
            "run_id": state.run_id,
        }
        config_path = self.write_client_config(state, context, create=create)
        self.client = OperationLogger(config_path)
        self.client.open()
        self._opened = True
        info = self.client.get_journal_info()
        self._identity = {key: info[key] for key in ("journal_id", "generation")}
        write_json(
            state.experiment_directory / "runner" / "journal.json", self._identity
        )
        # Existing-mode config also provides a stable entry point for later readers.
        self.reader_config_path = self.write_client_config(state, context)

    def write_client_config(
        self, state: RunnerState, context: JsonObject, *, create: bool = False
    ) -> Path:
        """Write an identity-bound logging configuration and return its absolute path.

        Args:
            state: Experiment state supplying the journal location and policy.
            context: Journal context for the future client.
            create: Whether the client should create a new journal with no expected identity.

        Returns:
            Path to a new JSON configuration beneath runner/logging.
        """
        settings = state.template.logging.model_dump(exclude_unset=True)
        settings.update(
            {
                "db_path": str(
                    state.experiment_directory / "journals" / "events.sqlite"
                ),
                "open_mode": "create" if create else "existing",
                "expected_journal": None if create else self._identity,
            }
        )
        path = state.experiment_directory / "runner" / "logging" / f"{uuid4()}.json"
        write_json(path, {"logging": settings, "operation_context": context})
        return path

    def record_template(
        self,
        state: RunnerState,
        template_yaml: str,
        template: ExperimentTemplate,
        reason: str,
    ) -> None:
        """Journal an applied template and update the state's revision and run identity.

        Args:
            state: Mutable state receiving the applied template and revision.
            template_yaml: Original template text retained in the journal.
            template: Validated normalized template.
            reason: Application reason; initial preserves the existing revision ID.
        """
        previous = state.template_revision_id
        changed = state.template.model_dump(exclude_unset=True) != template.model_dump(
            exclude_unset=True
        )
        revision = str(uuid4()) if reason != "initial" else previous
        context = {"experiment_id": state.experiment_id, "run_id": state.run_id}
        if changed:
            context["previous_run_id"] = state.run_id
            context["run_id"] = f"{uuid4()}:{state.run_id}"
        self.client.record_template_applied(
            template.model_dump(exclude_unset=True),
            template_yaml=template_yaml,
            template_revision_id=revision,
            previous_template_revision_id=previous if revision != previous else None,
            reason=reason,
            context=context,
        )
        state.run_id = context["run_id"]
        state.template_revision_id = revision
        state.template_yaml = template_yaml
        state.template = template

    def complete_restore(
        self,
        state: RunnerState,
        journal_manifest: JournalSnapshotManifest | JsonObject,
        restoration_id: str,
        diagnostics_directory: Path | None = None,
    ) -> None:
        """Finalize journal restoration and reopen the runner with its new generation.

        Args:
            state: Restored experiment state.
            journal_manifest: Snapshot journal metadata.
            restoration_id: Transaction UUID used to derive a repeatable new generation.
            diagnostics_directory: Optional diagnostic export to retain during restoration.

        Raises:
            RuntimeError: The runner journal is still open.
            ValueError: Restored paths escape the experiment or metadata is invalid.
        """
        if getattr(self, "_opened", False):
            raise RuntimeError("Close the runner journal before restoring it.")
        if not isinstance(journal_manifest, JournalSnapshotManifest):
            journal_manifest = copy_json_object(journal_manifest, "journal manifest")
        restoration = UUID(require_text(restoration_id, "restoration_id"))
        root = state.experiment_directory.resolve()
        database = root / "journals" / "events.sqlite"
        if database.is_symlink() or not database.resolve().is_relative_to(root):
            raise ValueError("Restored journal path escapes the experiment.")
        identity_path = root / "runner" / "journal.json"
        if identity_path.is_symlink() or not identity_path.resolve().is_relative_to(
            root
        ):
            raise ValueError("Restored journal identity path escapes the experiment.")
        settings = state.template.logging
        identity = (
            {
                "journal_id": journal_manifest.journal_id,
                "generation": journal_manifest.generation,
            }
            if isinstance(journal_manifest, JournalSnapshotManifest)
            else {key: journal_manifest[key] for key in ("journal_id", "generation")}
        )
        # The same transaction must choose the same generation after a crash
        # between the database commit and publication of runner/journal.json.
        generation = uuid5(restoration, "experiment-journal-generation").hex
        store = SQLiteEventStore(
            database,
            busy_timeout_seconds=settings.busy_timeout_seconds,
            max_event_bytes=settings.max_event_bytes,
            min_free_bytes=settings.min_free_bytes,
            open_mode="existing",
            expected_journal=identity,
            diagnostic_context={
                "source": "runner",
                "experiment_id": state.experiment_id,
                "run_id": state.run_id,
            },
        )
        try:
            result = store.complete_restore(
                journal_manifest.model_dump()
                if isinstance(journal_manifest, JournalSnapshotManifest)
                else journal_manifest,
                restoration_id=restoration.hex,
                new_generation=generation,
                diagnostics=diagnostics_directory,
            )
        finally:
            store.close()
        self._identity = {key: result[key] for key in ("journal_id", "generation")}
        write_json(identity_path, self._identity)
        self.open(state, create=False)

    def close(self) -> None:
        """Close the owned client and clear the published reader-config reference."""
        if not getattr(self, "_opened", False):
            return
        self.client.close()
        self._opened = False
        self.reader_config_path = None


async def _read_recovery_checkpoint(
    root: Path, experiment_id: str, state_error: Exception
) -> RunnerState:
    # state.json is optional. A committed checkpoint can bootstrap
    # recovery without accepting edits to the on-disk template.
    identity = read_json(root / "runner/journal.json")
    state = None
    for config_path in (root / "runner/logging").glob("*.json"):
        try:
            config = read_json(config_path)["logging"]
        except (OSError, ValueError, TypeError, KeyError):
            continue
        if (
            config.get("open_mode") != "existing"
            or config.get("expected_journal") != identity
            or Path(config.get("db_path", "")).resolve()
            != root / "journals/events.sqlite"
        ):
            continue
        reader = OperationLogger(config_path)
        try:
            reader.open()
            checkpoint = boundary = None
            while True:
                page = await asyncio.to_thread(
                    reader.read_events, checkpoint, limit=1000
                )
                if boundary is None:
                    boundary = page["boundary"]["cursor"]
                for entry in page["events"]:
                    if entry["cursor"] > boundary:
                        break
                    event = entry["event"]
                    if event["context"].get("experiment_id") != experiment_id:
                        continue
                    if event["event_type"] in (
                        "runner.checkpoint",
                        "rebuild.checkpoint",
                    ):
                        state = state_from_document(root, event["data"])
                    elif event["event_type"] == "experiment.restored":
                        state = None
                checkpoint = page["checkpoint"]
                if checkpoint["cursor"] >= boundary or not page["has_more"]:
                    break
        finally:
            reader.close()
        break
    if state is None:
        raise RuntimeError(
            "Recovery requires a valid saved state or committed journal checkpoint."
        ) from state_error
    return state


async def _read_reload_progress(
    logger: OperationLogger,
    experiment_id: str,
    template_revision_id: str,
    cycle_number: int,
    completed_stage_ids: set[str],
) -> set[str]:
    # Results cover successes. Checkpoints also retain policy-accepted skips;
    # numerical positions alone cannot prove progress after move/rerun.
    completed = set(completed_stage_ids)
    lineage = set()
    ancestor = experiment_id
    while ancestor:
        lineage.add(ancestor)
        ancestor = ancestor.partition(":")[2]
    checkpoint = boundary = None
    while True:
        page = await asyncio.to_thread(logger.read_events, checkpoint, limit=1000)
        if boundary is None:
            boundary = page["boundary"]["cursor"]
        for entry in page["events"]:
            if entry["cursor"] > boundary:
                break
            event = entry["event"]
            document = event["data"]
            if (
                event["event_type"] == "runner.checkpoint"
                and document.get("experiment_id") in lineage
                and document.get("template_revision_id") == template_revision_id
                and document.get("cycle_number") == cycle_number
                and document.get("pending_advance")
            ):
                completed.add(
                    document["template"]["stages"][document["stage_position"] - 1][
                        "stage_id"
                    ]
                )
            elif (
                event["event_type"] == "reload.progress"
                and event["context"].get("experiment_id") in lineage
                and document.get("template_revision_id") == template_revision_id
                and document.get("cycle_number") == cycle_number
            ):
                completed.update(document.get("preserved_completed_stage_ids", []))
        checkpoint = page["checkpoint"]
        if checkpoint["cursor"] >= boundary or not page["has_more"]:
            break
    return completed


async def _read_recovery_evidence(
    logger: OperationLogger, experiment_id: str
) -> tuple[JsonObject | None, list[JsonObject]]:
    checkpoint = None
    latest = None
    launches = []
    boundary = None
    while True:
        page = await asyncio.to_thread(
            logger.read_events, checkpoint, limit=1000
        )
        if boundary is None:
            boundary = page["boundary"]["cursor"]
        for entry in page["events"]:
            if entry["cursor"] > boundary:
                break
            event = entry["event"]
            if event["context"].get("experiment_id") != experiment_id:
                continue
            if event["event_type"] == "experiment.restored":
                latest, launches = None, []
            elif event["event_type"] in (
                "runner.checkpoint",
                "rebuild.checkpoint",
            ):
                latest, launches = event["data"], []
            elif (
                event["event_type"] == "control.intent"
                and event["data"].get("action") == "start_stage"
            ):
                launches.append(
                    {
                        **event["context"],
                        "queued_monotonic": event["data"][
                            "queued_monotonic"
                        ],
                        "queued_at": event["data"]["queued_at"],
                    }
                )
        checkpoint = page["checkpoint"]
        if checkpoint["cursor"] >= boundary or not page["has_more"]:
            break
    return latest, launches
