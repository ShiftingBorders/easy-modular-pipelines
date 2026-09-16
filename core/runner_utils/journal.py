"""Runner-local journal ownership and per-process client configurations."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4, uuid5

from core.logger import OperationLogger
from core.logger_utils.events import copy_json_object, require_text
from core.logger_utils.storage import SQLiteEventStore
from core.runner_utils.runtimeio import read_json, write_json
from core.runner_utils.state import JsonObject, RunnerState


class RunnerJournal:
    client: OperationLogger
    reader_config_path: Path | None = None

    def open(self, state: RunnerState, *, create: bool) -> None:
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
        settings = dict(state.template["logging"])
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
        self, state: RunnerState, template_yaml: str, template: JsonObject, reason: str
    ) -> None:
        previous = state.template_revision_id
        changed = state.template != template
        revision = str(uuid4()) if reason != "initial" else previous
        context = {"experiment_id": state.experiment_id, "run_id": state.run_id}
        if changed:
            context["previous_run_id"] = state.run_id
            context["run_id"] = f"{uuid4()}:{state.run_id}"
        self.client.record_template_applied(
            template,
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
        journal_manifest: JsonObject,
        restoration_id: str,
        diagnostics_directory: Path | None = None,
    ) -> None:
        if getattr(self, "_opened", False):
            raise RuntimeError("Close the runner journal before restoring it.")
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
        settings = state.template["logging"]
        identity = {key: journal_manifest[key] for key in ("journal_id", "generation")}
        # The same transaction must choose the same generation after a crash
        # between the database commit and publication of runner/journal.json.
        generation = uuid5(restoration, "experiment-journal-generation").hex
        store = SQLiteEventStore(
            database,
            busy_timeout_seconds=settings["busy_timeout_seconds"],
            max_event_bytes=settings["max_event_bytes"],
            min_free_bytes=settings["min_free_bytes"],
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
                journal_manifest,
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
        if not getattr(self, "_opened", False):
            return
        self.client.close()
        self._opened = False
        self.reader_config_path = None
