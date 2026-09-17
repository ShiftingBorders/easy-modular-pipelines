"""Approved D/E: independent timing arithmetic, lineage and resource semantics."""

import copy
import unittest
from datetime import datetime, timedelta
from uuid import uuid4

from dashboard.projections import elapsed, experiment_views
from tests.dashboard_tests.integration_helpers import history, timestamp


class ProjectionTests(unittest.TestCase):
    def test_service_node_resolves_module_from_services_definition(self):
        """I22: a DAG reference does not need a duplicated module declaration."""
        dataset = history()
        template = dataset["state"]["template"]
        node = template["stages"][1]
        module = node.pop("module")
        node["service_id"] = "weather"
        template["services"] = [{"service_id": "weather", "module": module}]
        result = experiment_views(dataset)
        self.assertEqual(
            result["forecast"]["component_durations"][1]["module_name"], "long"
        )

    def test_forecast_does_not_bridge_separate_logical_runs_with_same_revision(self):
        old = history(((1, 2),), total=2)
        current = history(((4, 8),), total=2)
        current["state"]["run_id"] = "current-run"
        for row in current["entries"]:
            row["context"]["run_id"] = "current-run"
            if row["context"].get("attempt_id"):
                row["context"]["attempt_id"] = "current-" + row["context"]["attempt_id"]
            row["occurred_at"] = (
                datetime.fromisoformat(row["occurred_at"]) + timedelta(hours=24)
            ).isoformat()
            if row["event_type"] == "stage.process_started":
                row["data"]["started_at"] = row["occurred_at"]
        current["entries"] = old["entries"] + current["entries"]
        result = experiment_views(current)
        self.assertEqual(len(result["runs"]), 2)
        self.assertEqual(result["forecast"]["sample_mean_seconds"], 15)
        self.assertEqual(result["forecast"]["eta_seconds"], 15)

    def test_ignored_errors_remain_in_raw_history_without_counting_as_effective(self):
        data = history()
        row = copy.deepcopy(data["entries"][0])
        row.update(
            event_id="ignored",
            event_type="error.recorded",
            ignored={"reason": "rollback"},
            data={"error_type": "OldError", "message": "old"},
        )
        data["entries"].append(row)
        model = experiment_views(data)
        self.assertIn("ignored", [row["event_id"] for row in model["events"]])
        self.assertNotIn("ignored", [row["event_id"] for row in model["effective"]])
        self.assertEqual(model["summary"]["error_count"], 0)

    def test_seconds_and_hours_use_whole_cycles_including_transition_time(self):
        model = experiment_views(history())
        forecast = model["forecast"]
        # Cycles: 2 + 3 + 3600 = 3605; 4 + 3 + 7200 = 7207.
        self.assertEqual(forecast["sample_mean_seconds"], 5406)
        self.assertEqual(forecast["eta_seconds"], 10812)
        self.assertEqual(forecast["eta_low_seconds"], 7210)
        self.assertEqual(forecast["eta_high_seconds"], 14414)
        self.assertEqual(forecast["sample_cycles"], 2)
        self.assertEqual(
            [row["mean_seconds"] for row in forecast["component_durations"]], [3, 5400]
        )
        self.assertEqual(
            [row["duration_seconds"] for row in model["parameters"]], [2, 3600, 4, 7200]
        )
        self.assertEqual(forecast["completed_cycles"], 2)

    def test_fractional_seconds_are_not_rounded_to_zero(self):
        result = experiment_views(history(((0.01, 0.09),)))
        self.assertAlmostEqual(result["forecast"]["sample_mean_seconds"], 3.1, places=5)
        self.assertAlmostEqual(
            result["parameters"][0]["duration_seconds"], 0.01, places=5
        )

    def test_active_cycle_elapsed_time_is_subtracted_but_eta_never_negative(self):
        data = history(((2, 5),), total=3)  # ten-second reference cycle
        event = copy.deepcopy(data["entries"][1])
        event.update(event_id=uuid4().hex, occurred_at=timestamp(14))
        event["context"].update(cycle_number=2, attempt_id="active")
        data["entries"].extend(
            [
                event,
                {
                    **copy.deepcopy(event),
                    "event_type": "stage.process_started",
                    "data": {"started_at": timestamp(14)},
                },
            ]
        )
        live = {
            "experiment_id": "exp-test",
            "fresh": True,
            "phase": "stage_running",
            "cycle_number": 2,
            "observed_at": timestamp(19),
        }
        self.assertEqual(experiment_views(data, live)["forecast"]["eta_seconds"], 15)
        live["observed_at"] = timestamp(1000)
        self.assertEqual(experiment_views(data, live)["forecast"]["eta_seconds"], 0)

    def test_pause_excludes_a_cycle_from_timing_sample(self):
        data = history()
        event = copy.deepcopy(data["entries"][0])
        event.update(
            event_id=uuid4().hex,
            event_type="runner.checkpoint",
            occurred_at=timestamp(5),
            data={"mode": "paused", "phase": "waiting"},
        )
        data["entries"].append(event)
        forecast = experiment_views(data)["forecast"]
        self.assertEqual(forecast["sample_cycles"], 1)
        self.assertEqual(forecast["sample_mean_seconds"], 7207)

    def test_retry_delay_and_failed_attempt_count_in_cycle_wall_time(self):
        data = history(((2, 5),), total=2)
        first = data["entries"][3]
        first["data"]["outcome"] = "failed"
        retry = copy.deepcopy(data["entries"][1:4])
        for row, at in zip(retry, (5, 5, 7), strict=True):
            row["event_id"] = uuid4().hex
            row["occurred_at"] = timestamp(at)
            row["context"].update(attempt_id="retry", attempt_number=2)
        retry[1]["data"]["started_at"] = timestamp(5)
        retry[2]["data"]["outcome"] = "succeeded"
        for row, at in zip(data["entries"][4:], (10, 10, 15), strict=True):
            row["occurred_at"] = timestamp(at)
            if row["event_type"] == "stage.process_started":
                row["data"]["started_at"] = timestamp(at)
        data["entries"][4:4] = retry
        result = experiment_views(data)
        self.assertEqual(result["forecast"]["sample_mean_seconds"], 14)
        self.assertEqual(
            result["forecast"]["component_durations"][0]["mean_seconds"], 6
        )

    def test_incomplete_failed_or_unknown_workload_has_no_false_eta(self):
        for scenario in ("incomplete", "failed", "unknown", "empty"):
            with self.subTest(scenario=scenario):
                data = history(((2, 5),))
                if scenario == "incomplete":
                    data["complete"] = False
                if scenario == "failed":
                    data["entries"][-1]["data"]["outcome"] = "failed"
                if scenario == "unknown":
                    data["state"]["template"]["cycles"] = None
                if scenario == "empty":
                    data["entries"] = []
                self.assertIsNone(experiment_views(data)["forecast"]["eta_seconds"])

    def test_completed_run_has_zero_remaining_time(self):
        data = history(((2, 5),), total=1)
        data["state"]["phase"] = "completed"
        self.assertEqual(experiment_views(data)["forecast"]["eta_seconds"], 0)

    def test_new_revision_does_not_reuse_old_duration_sample(self):
        data = history()
        new = copy.deepcopy(data["entries"][0])
        new["data"]["template_revision_id"] = "rev-2"
        new["context"]["template_revision_id"] = "rev-2"
        data["entries"].append(new)
        self.assertIsNone(experiment_views(data)["forecast"]["sample_mean_seconds"])

    def test_historical_run_does_not_inherit_current_live_state(self):
        data = history()
        data["state"]["run_id"] = "new-run"
        live = {
            "experiment_id": "exp-test",
            "fresh": True,
            "phase": "stage_running",
            "cycle_number": 9,
        }
        result = experiment_views(data, live, "run-test")
        self.assertFalse(result["summary"]["fresh"])
        self.assertNotEqual(result["summary"]["status"], "running")
        self.assertEqual(experiment_views(data, live, "missing")["parameters"], [])

    def test_timeline_parentage_errors_and_dag_loop_are_independent(self):
        data = history(((2, 5),))
        context = data["entries"][1]["context"]
        for identity, parent in (("outer", None), ("inner", "outer")):
            data["entries"].append(
                {
                    "event_id": identity,
                    "event_type": "operation.started",
                    "context": context,
                    "occurred_at": timestamp(1),
                    "operation_id": identity,
                    "data": {
                        "operation_type": "work",
                        "operation_name": identity,
                        "parent_operation_id": parent,
                    },
                }
            )
        data["entries"].append(
            {
                "event_id": "error",
                "event_type": "error.recorded",
                "occurred_at": timestamp(2),
                "context": context,
                "data": {"error_type": "ValueError", "message": "bad"},
            }
        )
        model = experiment_views(data)
        ops = {row["operation_id"]: row for row in model["operations"]}
        self.assertEqual(ops["inner"]["parent_operation_id"], "outer")
        self.assertEqual(ops["outer"]["parent_operation_id"], "attempt:1-0")
        self.assertIsNone(ops["inner"]["finished_at"])
        self.assertEqual(len(model["errors"]), 1)
        self.assertEqual(model["errors"][0]["stage_id"], "A")
        self.assertEqual(model["template"]["edges"][0], {"from": "A", "to": "B"})
        self.assertEqual(model["template"]["edges"][-1]["to"], "A")

    def test_elapsed_handles_timezone_and_invalid_times(self):
        self.assertEqual(
            elapsed("2026-01-01T01:00:00+01:00", "2026-01-01T00:00:01+00:00"), 1
        )
        for first, last in (
            (None, timestamp()),
            ("bad", timestamp()),
            (timestamp(2), timestamp(1)),
        ):
            self.assertIsNone(elapsed(first, last))


class MetricProjectionTests(unittest.TestCase):
    def test_units_and_scopes_are_not_silently_combined(self):
        data = self.measurements([3, 6])
        data["entries"][-1]["data"]["resources"]["custom"]["unit"] = "USD"
        rows = experiment_views(data)["measurements"]
        self.assertEqual({row["unit"] for row in rows}, {"token", "USD"})
        self.assertEqual({row["value"] for row in rows}, {3, 6})
        data["entries"][-1]["data"]["resources"]["custom"]["scope"] = "host"
        self.assertEqual(len(experiment_views(data)["measurements"]), 1)

    def measurements(self, values, kind="delta", *, scope="process", attributes=None):
        data = history(((2, 5),))
        context = data["entries"][1]["context"]
        for value in values:
            data["entries"].append(
                {
                    "event_id": uuid4().hex,
                    "occurred_at": timestamp(2),
                    "event_type": "resources.recorded",
                    "context": context,
                    "operation_id": None,
                    "producer_instance_id": "writer",
                    "data": {
                        "resources": {
                            "custom": {
                                "value": value,
                                "unit": "token",
                                "kind": kind,
                                "scope": scope,
                                "estimated": False,
                                "attributes": attributes or {},
                            }
                        }
                    },
                }
            )
        return data

    def test_resource_semantics_do_not_confuse_totals_deltas_peaks_and_gauges(self):
        for kind, expected in (("delta", 9), ("total", 6), ("peak", 6), ("gauge", 4.5)):
            with self.subTest(kind=kind):
                row = experiment_views(self.measurements([3, 6], kind))["measurements"][
                    0
                ]
                self.assertEqual(row["value"], expected)
                self.assertTrue(row["complete"])

    def test_unknown_negative_estimated_and_mixed_kinds(self):
        self.assertEqual(
            experiment_views(self.measurements([-3, 1]))["measurements"][0]["value"], -2
        )
        row = experiment_views(self.measurements([None]))["measurements"][0]
        self.assertIsNone(row["value"])
        self.assertFalse(row["complete"])
        data = self.measurements([3, 6])
        data["entries"][-1]["data"]["resources"]["custom"].update(
            kind="gauge", estimated=True
        )
        row = experiment_views(data)["measurements"][0]
        self.assertIsNone(row["value"])
        self.assertFalse(row["complete"])
        self.assertTrue(row["estimated"])

    def test_gauges_use_interval_weights(self):
        data = self.measurements([10, 30], "gauge")
        data["entries"][-2]["data"]["resources"]["custom"]["attributes"] = {
            "interval_seconds": 1
        }
        data["entries"][-1]["data"]["resources"]["custom"]["attributes"] = {
            "interval_seconds": 3
        }
        self.assertEqual(experiment_views(data)["measurements"][0]["value"], 25)

    def test_nested_totals_with_unmeasured_intermediate_parent_are_not_added(self):
        data = self.measurements([10, 4], "total", scope="operation")
        context = data["entries"][1]["context"]
        data["entries"][-2]["operation_id"] = "outer"
        data["entries"][-1]["operation_id"] = "inner"
        for identity, parent in (
            ("outer", None),
            ("middle", "outer"),
            ("inner", "middle"),
        ):
            data["entries"].append(
                {
                    "event_type": "operation.started",
                    "operation_id": identity,
                    "occurred_at": timestamp(1),
                    "context": context,
                    "data": {
                        "operation_type": "work",
                        "operation_name": identity,
                        "parent_operation_id": parent,
                    },
                }
            )
        row = experiment_views(data)["measurements"][0]
        self.assertIsNone(row["value"])
        self.assertEqual(row["reason"], "overlapping_operation_measurements")
