"""Literal runner observations for approved dashboard conditional tests D01-D08."""

import copy
from uuid import uuid4

from tests.dashboard_tests.integration_helpers import timestamp


class ConditionalHistory:
    def __init__(self):
        self.template = {
            "name": "conditional history",
            "cycles": 2,
            "stages": [
                {"stage_id": stage, "module": {"name": stage, "version": "1"}}
                for stage in ("P", "A", "B", "C")
            ],
        }
        self.template["stages"][-1]["returns_data"] = False
        self.state = {
            "experiment_id": "exp-test",
            "run_id": "run-test",
            "template_revision_id": "rev-1",
            "template": self.template,
            "template_yaml": "name: conditional history\n",
            "cycle_number": 1,
            "stage_position": 4,
            "phase": "waiting",
            "mode": "paused",
            "stage_result_ids": {},
            "pending_advance": True,
            "active_attempt": None,
            "pending_input": None,
            "last_dag_decision": None,
        }
        self.entries = []
        self.template_applied()
        for stage in ("P", "A", "B", "C"):
            self.attempt(stage, 1)
            self.state["stage_result_ids"][stage] = f"{stage}-1"
        self.checkpoint()

    def append(self, kind, data, **context):
        index = len(self.entries) + 1
        self.entries.append(
            {
                "schema_version": 2,
                "event_id": uuid4().hex,
                "producer_instance_id": "conditional-fixture",
                "sequence_number": index,
                "occurred_at": timestamp(index),
                "event_type": kind,
                "operation_id": None,
                "context": {
                    "experiment_id": "exp-test",
                    "run_id": self.state["run_id"],
                    "template_revision_id": self.state["template_revision_id"],
                    "source": "runner",
                    **context,
                },
                "data": copy.deepcopy(data),
            }
        )

    def template_applied(self):
        self.append(
            "template.applied",
            {
                "template": self.template,
                "template_yaml": self.state["template_yaml"],
                "template_revision_id": self.state["template_revision_id"],
            },
        )

    def attempt(self, stage, number, *, finish=True):
        context = {
            "stage_id": stage,
            "attempt_id": f"{stage}-{number}",
            "attempt_number": number,
            "cycle_number": self.state["cycle_number"],
            "module_name": stage,
            "module_version": "1",
        }
        self.append(
            "attempt.parameters",
            {
                "effective_settings": {},
                "template": self.template,
                "template_yaml": self.state["template_yaml"],
            },
            **context,
        )
        self.append("stage.process_started", {"process": {}}, **context)
        if finish:
            self.append(
                "stage.finished",
                {
                    "outcome": "succeeded",
                    "result_request_id": f"{stage}-{number}",
                    "result": {"result": "success", "data": {"value": stage}},
                },
                **context,
            )

    def checkpoint(self):
        self.state["checkpoint_id"] = uuid4().hex
        self.append("runner.checkpoint", self.state)

    def move(self, source="C", target="A", number=2):
        self.attempt(source, number)
        ids = [item["stage_id"] for item in self.template["stages"]]
        position = ids.index(target) + 1
        accepted = {**self.state["stage_result_ids"], source: f"{source}-{number}"}
        self.state.update(
            stage_position=position,
            pending_advance=False,
            active_attempt=None,
            phase="waiting",
            mode="paused",
            stage_result_ids={
                key: value
                for key, value in accepted.items()
                if key in ids[: position - 1]
            },
            pending_input={
                "source_stage_id": source,
                "stage_id": target,
                "request_id": f"{source}-{number}",
                "experiment_id": "exp-test",
            },
            last_dag_decision={
                "source_stage_id": source,
                "request_id": f"{source}-{number}",
                "experiment_id": "exp-test",
                "decision": {"command": "move", "stage_id": target},
            },
        )
        self.checkpoint()

    def start_target(self):
        self.attempt("A", 2, finish=False)
        self.state.update(
            phase="stage_running",
            mode="running",
            active_attempt={"attempt_id": "A-2"},
            last_dag_decision=None,
        )
        self.checkpoint()

    def finish_target(self):
        self.append(
            "stage.finished",
            {
                "outcome": "succeeded",
                "result_request_id": "A-2",
                "result": {"result": "success", "data": None},
            },
            stage_id="A",
            attempt_id="A-2",
            attempt_number=2,
            cycle_number=1,
            module_name="A",
            module_version="1",
        )
        self.state.update(
            phase="waiting", mode="paused", active_attempt=None, pending_advance=True
        )
        self.state["stage_result_ids"]["A"] = "A-2"
        self.checkpoint()

    def dataset(self, *, cursors=True):
        entries = copy.deepcopy(self.entries)
        if cursors:
            for index, entry in enumerate(entries, 1):
                entry.update(cursor=index, effective=True, ignored=False)
        return {
            "experiment_id": "exp-test",
            "state": copy.deepcopy(self.state),
            "entries": entries,
            "complete": True,
            "error": None,
            "identity": {"journal_id": "fixture", "generation": "fixture"},
        }

    def live(self):
        result = {
            key: copy.deepcopy(value)
            for key, value in self.state.items()
            if key
            not in {"template", "template_yaml", "active_attempt", "last_dag_decision"}
        }
        result.update(
            fresh=True,
            observed_at=timestamp(len(self.entries) + 1),
            active_attempt_id=(self.state["active_attempt"] or {}).get("attempt_id"),
            dag_decision=copy.deepcopy(self.state["last_dag_decision"]),
        )
        return result
