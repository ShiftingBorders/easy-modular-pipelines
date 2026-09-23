"""Literal timing histories and real journals for approved dashboard blocks A-H."""

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from core.logger import OperationLogger
from tests.helpers.logging_process import existing_settings, write_settings


def timestamp(seconds=0):
    return (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)).isoformat()


def history(durations=((2, 3600), (4, 7200)), *, total=4):
    """Sequential stages separated by three seconds; timestamps are independent facts."""
    template = {
        "name": "timing",
        "cycles": total,
        "stages": [
            {"stage_id": "A", "module": {"name": "short", "version": "1"}},
            {"stage_id": "B", "module": {"name": "long", "version": "1"}},
        ],
    }
    base = {
        "experiment_id": "exp-test",
        "run_id": "run-test",
        "template_revision_id": "rev-1",
        "source": "runner",
    }
    entries = []

    def add(kind, seconds, data, context=None, operation_id=None):
        entries.append(
            {
                "schema_version": 2,
                "event_id": uuid4().hex,
                "producer_instance_id": "fixture",
                "sequence_number": len(entries) + 1,
                "occurred_at": timestamp(seconds),
                "event_type": kind,
                "context": {**base, **(context or {})},
                "operation_id": operation_id,
                "data": data,
            }
        )

    add(
        "template.applied",
        0,
        {
            "template": template,
            "template_yaml": "name: timing\n",
            "template_revision_id": "rev-1",
        },
    )
    clock = 1
    for cycle, times in enumerate(durations, 1):
        for index, duration in enumerate(times):
            definition = template["stages"][index]
            context = {
                "attempt_id": f"{cycle}-{index}",
                "stage_id": definition["stage_id"],
                "cycle_number": cycle,
                "attempt_number": 1,
                "module_name": definition["module"]["name"],
                "module_version": "1",
            }
            add(
                "attempt.parameters",
                clock,
                {
                    "effective_settings": {},
                    "template": template,
                    "template_yaml": "name: timing\n",
                },
                context,
            )
            add(
                "stage.process_started",
                clock,
                {"started_at": timestamp(clock), "process": {}},
                context,
            )
            clock += duration
            add(
                "stage.finished", clock, {"outcome": "succeeded", "result": {}}, context
            )
            clock += 3
    return {
        "experiment_id": "exp-test",
        "identity": {"journal_id": "journal", "generation": "generation"},
        "state": {
            **base,
            "template": template,
            "phase": "waiting",
            "mode": "paused",
            "cycle_number": len(durations) + 1,
        },
        "entries": entries,
        "complete": True,
        "error": None,
    }


class JournalWorkspace:
    def __init__(
        self, root: Path, dataset=None, *, identifier="exp-test", folder="recorded"
    ):
        self.root = root
        self.directory = root / "experiments" / folder
        (self.directory / "runner").mkdir(parents=True)
        self.config = write_settings(
            self.directory / "journals",
            db_path="events.sqlite",
            context={"experiment_id": identifier, "run_id": "run-test"},
        )
        self.logger = OperationLogger(self.config)
        self.logger.open()
        self.identity = {
            key: self.logger.get_journal_info()[key]
            for key in ("journal_id", "generation")
        }
        existing_settings(self.config)
        dataset = copy.deepcopy(dataset or history())
        dataset["experiment_id"] = identifier
        dataset["state"]["experiment_id"] = identifier
        for event in dataset["entries"]:
            event["context"]["experiment_id"] = identifier
            if event["event_type"] == "command.result":
                self.logger._store.append_command_result(event)
            else:
                self.logger._store.append(event)
        settings = json.loads(self.config.read_text(encoding="utf-8"))["logging"]
        dataset["state"]["template"]["logging"] = settings
        (self.directory / "runner/state.json").write_text(
            json.dumps(dataset["state"]), encoding="utf-8"
        )
        (self.directory / "runner/journal.json").write_text(
            json.dumps(self.identity), encoding="utf-8"
        )
        registry_path = root / "experiments.json"
        registry = (
            json.loads(registry_path.read_text(encoding="utf-8"))
            if registry_path.exists()
            else {}
        )
        registry[identifier] = folder
        registry_path.write_text(json.dumps(registry), encoding="utf-8")

    def close(self):
        self.logger.close()


def resource_status(value=25):
    measured = {
        "value": value,
        "unit": "percent",
        "kind": "gauge",
        "scope": "host",
        "attributes": {},
    }
    sample = {
        "series_id": "host:test",
        "observed_at": datetime.now(UTC).isoformat(),
        "resources": {
            name: copy.deepcopy(measured)
            for name in ("host_cpu_percent", "host_memory_percent", "host_disk_percent")
        },
        "freshness": {
            name: {"fresh": True}
            for name in ("host_cpu_percent", "host_memory_percent", "host_disk_percent")
        },
    }
    return {
        "state": "running",
        "history_id": "history",
        "latest": [sample],
        "journal_error": None,
    }
