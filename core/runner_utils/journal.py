"""Runner-local journal ownership and per-process client configurations."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from core.logger import OperationLogger
from core.runner_utils.runtimeio import read_json, write_json
from core.runner_utils.state import JsonObject, RunnerState


class RunnerJournal:
    client: OperationLogger

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
        self._reader_config = self.write_client_config(state, context)

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
        raise NotImplementedError(
            "Journal restoration belongs to the snapshot implementation."
        )

    def close(self) -> None:
        if not getattr(self, "_opened", False):
            return
        self.client.close()
        self._opened = False
