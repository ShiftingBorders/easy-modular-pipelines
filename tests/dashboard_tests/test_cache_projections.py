"""Approved exactness regressions for partitioned journal projections."""

import copy
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from dashboard.api_client import SystemAPIClient
from dashboard.config import load_settings
from dashboard.journals import LocalJournals
from dashboard.projections import (
    cached_experiment_views,
    cached_metrics,
    experiment_views,
    window_experiment_views,
)
from dashboard.views import DashboardViews
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import (
    JournalWorkspace,
    history,
    timestamp,
)


class CacheProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_ram_measurement_cards_do_not_use_only_the_requested_page(self):
        """T043: a complete RAM view preserves metrics beyond the selected page."""
        data = self.measurements()
        for event in data["entries"][-2:]:
            metric = event["data"]["resources"]["custom"]
            event["data"]["resources"] = {
                f"metric-{index}": dict(metric) for index in range(20)
            }
        _, reader, _ = self.cache(data)
        reader.window_events = 500
        preview = reader.preview("exp-test")
        model = window_experiment_views(preview, {})
        views = DashboardViews(reader.settings, SystemAPIClient(reader.settings))
        self.addCleanup(views.journals.close)
        with patch.object(views, "_model", AsyncMock(return_value=(preview, model))):
            result = await views.experiment("exp-test", "measurements", {"limit": 1})
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(result["total"], 20)
        self.assertEqual(len(result["measurements"]), 20)
        self.assertEqual({row["value"] for row in result["measurements"]}, {9})

    async def test_complete_ram_window_matches_disk_and_keeps_raw_context(self):
        """T021/T026/T034/T035/T087: exact first render before disk publication."""
        data = self.measurements()
        for event in data["entries"][-2:]:
            event["context"].pop("run_id")
        workspace, reader, _ = self.cache(data)
        reader.window_events = 500
        context = {"participant_id": "service", "participant_instance_id": "instance"}
        old = workspace.logger.record_command_result(
            "request",
            {"ok": 1},
            author="participant",
            outcome="succeeded",
            context=context,
        )
        workspace.logger.record_command_result(
            "request", {"ok": 0}, author="runner", outcome="failed", context=context
        )
        preview = reader.preview("exp-test")
        self.assertIsNotNone(preview)
        model = window_experiment_views(preview, {}, "run-test")
        self.assertEqual(model["measurements"][0]["value"], 9)
        self.assertFalse(
            next(event for event in model["events"] if event["event_id"] == old)[
                "effective"
            ]
        )
        self.assertTrue(
            next(event for event in model["events"] if event["event_id"] == old)[
                "ignored"
            ]
        )
        self.assertNotIn(
            "run_id",
            next(
                event
                for event in model["events"]
                if event["event_type"] == "resources.recorded"
            )["context"],
        )
        disk = reader.load("exp-test", force=True)
        self.assertEqual(
            model["summary"], cached_experiment_views(disk, {}, "run-test")["summary"]
        )
        self.assertEqual(
            model["measurements"][0]["value"],
            reader.page(disk, "measurements", {"compact": "1"})["items"][0]["value"],
        )

    async def test_preview_never_exceeds_the_active_window_or_payload_budget(self):
        """T016/T022/T089: small-history optimization has the same RAM boundaries."""
        _, reader, _ = self.cache(history())
        self.assertIsNone(reader.preview("exp-test"))
        reader.window_events = 500
        reader.max_bytes = 1
        self.assertIsNone(reader.preview("exp-test"))

    async def test_historical_revision_is_not_replaced_by_latest_template(self):
        """T036/T059: each cycle retains the template revision recorded for it."""
        data = self.measurements()
        revision = copy.deepcopy(data["entries"][0])
        revision.update(
            event_id=uuid4().hex, sequence_number=500, occurred_at=timestamp(100)
        )
        revision["context"]["template_revision_id"] = "rev-2"
        revision["data"]["template_revision_id"] = "rev-2"
        revision["data"]["template"]["stages"] = []
        data["entries"].append(revision)
        _, reader, dataset = self.cache(data)
        old = reader.page(
            dataset, "measurements", {"compact": "1", "revision": "rev-1"}
        )
        self.assertEqual(old["total"], 1)
        self.assertEqual(old["items"][0]["value"], 9)
        self.assertEqual(
            reader.page(dataset, "measurements", {"compact": "1", "revision": "rev-2"})[
                "total"
            ],
            0,
        )
        views = DashboardViews(reader.settings, SystemAPIClient(reader.settings))
        self.addCleanup(views.journals.close)
        model = cached_experiment_views(dataset, {})
        template = views._cached_template(dataset, model, {"revision": "rev-1"})
        self.assertEqual(len(template["template"]["stages"]), 2)

    async def test_command_observations_from_different_cycles_are_grouped_once(self):
        """T037: request identity groups runner/service observations across scopes."""
        data = history(((2, 5),))
        for number, cycle in enumerate((1, 2)):
            event = copy.deepcopy(data["entries"][1])
            event.update(
                event_id=uuid4().hex,
                sequence_number=700 + number,
                event_type="control.intent" if number == 0 else "control.result",
                data={"action": "pause", "outcome": "succeeded"}
                if number
                else {"action": "pause"},
            )
            event["context"].update(request_id="same-request", cycle_number=cycle)
            data["entries"].append(event)
        _, reader, dataset = self.cache(data)
        page = reader.page(dataset, "commands", {"compact": "1"})
        self.assertEqual(page["total"], 1)
        self.assertEqual(page["items"][0]["status"], "succeeded")
        detail = reader.detail(dataset, page["items"][0]["detail_ref"])
        self.assertEqual(len(detail["observations"]), 2)

    async def asyncSetUp(self):
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        self.root = Path(temporary.name)

    def cache(self, data, name="case"):
        root = self.root / name
        workspace = JournalWorkspace(root / "project", data)
        self.addCleanup(workspace.close)
        settings = load_settings(
            write_settings(
                root, project_root=str(workspace.root), history_window_events=2
            )
        )
        reader = LocalJournals(settings)
        self.addCleanup(reader.close)
        dataset = reader.load("exp-test", force=True)
        self.assertTrue(dataset["complete"])
        return workspace, reader, dataset

    def measurements(self, kind="delta"):
        data = history(((2, 5),))
        for number, value in enumerate((3, 6)):
            event = copy.deepcopy(data["entries"][1])
            event.update(
                event_id=uuid4().hex,
                sequence_number=100 + number,
                event_type="resources.recorded",
                data={
                    "resources": {
                        "custom": {
                            "value": value,
                            "unit": "token",
                            "kind": kind,
                            "scope": "operation",
                            "estimated": False,
                            "attributes": {},
                        }
                    }
                },
            )
            data["entries"].append(event)
        return data

    async def test_missing_coordinates_preserve_records_and_original_context(self):
        """T034/T035: inheritance agrees with filtering, counters and raw details."""
        for missing in ("run_id", "template_revision_id", "cycle_number"):
            with self.subTest(missing=missing):
                data = self.measurements()
                for event in data["entries"][-2:]:
                    event["context"].pop(missing)
                error = copy.deepcopy(data["entries"][-1])
                error.update(
                    event_id=uuid4().hex,
                    sequence_number=200,
                    event_type="error.recorded",
                    data={
                        "error_id": "error",
                        "error_type": "Example",
                        "message": "failure",
                    },
                )
                data["entries"].append(error)
                _, reader, dataset = self.cache(data, missing)
                rows = reader.page(dataset, "measurements", {"compact": "1"})["items"]
                self.assertEqual(rows[0]["value"], 9)
                errors = reader.page(
                    dataset, "errors", {"compact": "1", "run_id": "run-test"}
                )
                self.assertEqual(errors["total"], 1)
                model = cached_experiment_views(dataset, {})
                self.assertEqual(model["summary"]["error_count"], 1)
                original = dataset["cache"].events([error["event_id"]])[0]
                self.assertNotIn(missing, original["context"])
                self.assertEqual(original["context"], error["context"])

    async def test_external_ancestry_and_late_parent_do_not_double_count(self):
        """T040/T041/T042: indexed transitive parents, late updates and cycles."""
        data = self.measurements("total")
        data["entries"][-2]["data"]["resources"]["custom"]["value"] = 10
        data["entries"][-1]["data"]["resources"]["custom"]["value"] = 4
        for event, operation in zip(data["entries"][-2:], ("outer", "inner")):
            event["operation_id"] = operation
        for number, (operation, parent) in enumerate(
            (("outer", None), ("inner", "middle"))
        ):
            event = copy.deepcopy(data["entries"][1])
            event.update(
                event_id=uuid4().hex,
                sequence_number=300 + number,
                event_type="operation.started",
                operation_id=operation,
                data={
                    "operation_type": "work",
                    "operation_name": operation,
                    "parent_operation_id": parent,
                },
            )
            data["entries"].append(event)
        workspace, reader, dataset = self.cache(data)
        self.assertEqual(
            reader.page(dataset, "measurements", {"compact": "1"})["items"][0]["value"],
            14,
        )
        parent = copy.deepcopy(data["entries"][-1])
        parent.update(
            event_id=uuid4().hex,
            sequence_number=400,
            operation_id="middle",
            context={"experiment_id": "exp-test", "run_id": "run-test"},
            data={
                "operation_type": "work",
                "operation_name": "middle",
                "parent_operation_id": "outer",
            },
        )
        workspace.logger._store.append(parent)
        updated = reader.load("exp-test", force=True)
        row = reader.page(updated, "measurements", {"compact": "1"})["items"][0]
        self.assertIsNone(row["value"])
        self.assertFalse(row["complete"])
        self.assertEqual(row["reason"], "overlapping_operation_measurements")
        identifiers = [
            row[0]
            for row in updated["cache"].query(
                "SELECT record_key FROM records WHERE kind='operations'"
            )
        ]
        self.assertEqual(identifiers.count("middle"), 1)
        parent.update(event_id=uuid4().hex, sequence_number=401, operation_id="outer")
        parent["data"]["parent_operation_id"] = "inner"
        workspace.logger._store.append(parent)
        self.assertTrue(reader.load("exp-test", force=True)["complete"])

    async def test_resource_kinds_and_weighted_gauge_match_full_projection(self):
        """T038/T039: exact numeric semantics survive compact cache inputs."""
        for kind, expected in (
            ("delta", 9),
            ("total", 6),
            ("peak", 6),
            ("gauge", 5.25),
        ):
            with self.subTest(kind=kind):
                data = self.measurements(kind)
                if kind == "gauge":
                    data["entries"][-2]["data"]["resources"]["custom"]["attributes"] = {
                        "interval_seconds": 1
                    }
                    data["entries"][-1]["data"]["resources"]["custom"]["attributes"] = {
                        "interval_seconds": 3
                    }
                _, reader, dataset = self.cache(data, kind)
                row = reader.page(dataset, "measurements", {"compact": "1"})["items"][0]
                self.assertEqual(row["value"], expected)
                self.assertEqual(
                    {key: value for key, value in row.items() if key != "detail_ref"},
                    experiment_views(data)["measurements"][0],
                )

    async def test_recent_chart_limit_does_not_change_full_cohort_totals(self):
        """T043: display samples are bounded while full-history totals stay exact."""
        data = history(tuple((1, 2) for _ in range(25)), total=30)
        for number, attempt in enumerate(
            [
                event
                for event in data["entries"]
                if event["event_type"] == "attempt.parameters"
                and event["context"]["stage_id"] == "A"
            ]
        ):
            event = copy.deepcopy(attempt)
            event.update(
                event_id=uuid4().hex,
                sequence_number=1000 + number,
                event_type="resources.recorded",
                data={
                    "resources": {
                        "units": {
                            "value": 2,
                            "unit": "token",
                            "scope": "process",
                            "kind": "delta",
                            "estimated": False,
                            "attributes": {},
                        }
                    }
                },
            )
            data["entries"].append(event)
        _, reader, dataset = self.cache(data)
        model = cached_experiment_views(dataset, {})
        metrics = cached_metrics(dataset, model)
        self.assertEqual(len(metrics["measurements"]), 20)
        self.assertEqual(metrics["metric_summaries"][0]["total"], 50)
        self.assertEqual(
            reader.page(dataset, "measurements", {"compact": "1"})["total"], 25
        )

    async def test_live_attempt_overlay_matches_full_history_without_writes(self):
        """T057/T058: fresh/stale, wrong experiment and historical run semantics."""
        data = history(((2, 5),))
        data["entries"] = [
            event
            for event in data["entries"]
            if not (
                event["event_type"] == "stage.finished"
                and event["context"].get("attempt_id") == "1-1"
            )
        ]
        _, reader, dataset = self.cache(data)
        views = DashboardViews(reader.settings, SystemAPIClient(reader.settings))
        self.addCleanup(views.journals.close)
        before = dataset["cache"].query(
            "SELECT kind,payload FROM records ORDER BY kind,record_key"
        )
        for fresh, identifier, run in (
            (True, "exp-test", None),
            (False, "exp-test", None),
            (True, "other", None),
            (True, "exp-test", "old-run"),
        ):
            with self.subTest(fresh=fresh, identifier=identifier, run=run):
                live = {
                    "experiment_id": identifier,
                    "fresh": fresh,
                    "phase": "stage_running",
                }
                model = cached_experiment_views(dataset, live, run)
                expected = experiment_views(data, live, run)
                page = views._cached_view(
                    dataset,
                    model,
                    {},
                    "parameters",
                    {"compact": "1", **({"run_id": run} if run else {})},
                )
                self.assertEqual(
                    [(row["attempt_id"], row["status"]) for row in page["items"]],
                    [
                        (row["attempt_id"], row["status"])
                        for row in expected["parameters"]
                    ],
                )
        self.assertEqual(
            dataset["cache"].query(
                "SELECT kind,payload FROM records ORDER BY kind,record_key"
            ),
            before,
        )
