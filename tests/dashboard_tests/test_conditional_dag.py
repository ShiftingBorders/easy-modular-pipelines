"""Approved D01-D07: authoritative DAG progress across raw/RAM/cache reads."""

import asyncio
import copy
import json
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path

from core.journal.history_cache import JournalHistoryCache
from dashboard.config import load_settings
from dashboard.journals import LocalJournals
from dashboard.projections import (
    cached_experiment_views,
    compact_event,
    experiment_views,
    window_experiment_views,
)
from tests.dashboard_tests.conditional_helpers import ConditionalHistory
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import JournalWorkspace
from tests.helpers.services import wait_for
from tests.helpers.snapshots import SnapshotWorkspace


class ConditionalDagTests(unittest.TestCase):
    def cache(self, history):
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        root = Path(temporary.name)
        workspace = JournalWorkspace(root / "project", history.dataset(cursors=False))
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

    def statuses(self, model):
        return {node["stage_id"]: node["status"] for node in model["template"]["nodes"]}

    def moves(self, model):
        return [
            edge
            for edge in model["template"]["edges"]
            if edge.get("kind") == "conditional_move"
        ]

    def test_backwards_move_resets_current_badges_but_keeps_attempt_history(self):
        """D01/D02: old success stays historical; new accepted results restore badges."""
        history = ConditionalHistory()
        model = experiment_views(history.dataset(), history.live())
        self.assertEqual(
            self.statuses(model), dict.fromkeys(("P", "A", "B", "C"), "succeeded")
        )
        history.move()
        model = experiment_views(history.dataset(), history.live())
        self.assertEqual(
            self.statuses(model),
            {"P": "succeeded", "A": "pending", "B": "pending", "C": "pending"},
        )
        old = {item["attempt_id"]: item["status"] for item in model["parameters"]}
        self.assertEqual({old[key] for key in ("A-1", "B-1", "C-1")}, {"succeeded"})
        history.start_target()
        self.assertEqual(
            self.statuses(experiment_views(history.dataset(), history.live()))["A"],
            "running",
        )
        history.finish_target()
        model = experiment_views(history.dataset(), history.live())
        self.assertEqual(
            self.statuses(model),
            {"P": "succeeded", "A": "succeeded", "B": "pending", "C": "pending"},
        )
        self.assertFalse(self.moves(model)[0]["active"])

    def test_ram_compact_and_restarted_disk_cache_agree(self):
        """D03: current markers/edges do not depend on how much history fits in RAM."""
        for phase in ("moved", "running", "accepted"):
            with self.subTest(phase=phase):
                history = ConditionalHistory()
                history.move()
                history.checkpoint()
                if phase != "moved":
                    history.start_target()
                if phase == "accepted":
                    history.finish_target()
                for number in range(6):
                    history.append("fixture.tail", {"number": number})
                live = history.live()
                full = experiment_views(history.dataset(), live)
                compact = history.dataset()
                compact["entries"] = [
                    compact_event(event) for event in compact["entries"]
                ]
                models = [
                    window_experiment_views(history.dataset(), live),
                    experiment_views(compact, live),
                ]
                _, reader, dataset = self.cache(history)
                self.assertLessEqual(len(dataset["entries"]), 2)
                models.append(cached_experiment_views(dataset, live))
                reader.close()
                restarted = LocalJournals(reader.settings)
                self.addCleanup(restarted.close)
                models.append(
                    cached_experiment_views(restarted.cached("exp-test"), live)
                )
                for model in models:
                    self.assertEqual(self.statuses(model), self.statuses(full))
                    self.assertEqual(self.moves(model), self.moves(full))
                    self.assertIs(model["template"]["nodes"][-1]["returns_data"], False)

    def test_live_move_invalidates_success_before_cache_ingests_it(self):
        """D03: an old completed cache cannot overwrite a newer live reset."""
        history = ConditionalHistory()
        _, _, dataset = self.cache(history)
        history.move()
        model = cached_experiment_views(dataset, history.live())
        self.assertEqual(
            self.statuses(model),
            {"P": "succeeded", "A": "pending", "B": "pending", "C": "pending"},
        )
        self.assertEqual(
            [(edge["from"], edge["to"], edge["active"]) for edge in self.moves(model)],
            [("C", "A", True)],
        )

    def test_forward_backward_self_moves_are_distinct_from_template_flow(self):
        """D04: recorded pairs are deduplicated, and false still denotes conditional."""
        history = ConditionalHistory()
        history.template["stages"][1]["returns_data"] = True
        history.template_applied()
        history.move("A", "C")
        history.move("C", "C")
        history.move("C", "A", number=3)
        history.checkpoint()
        full = experiment_views(history.dataset(), history.live())
        _, _, dataset = self.cache(history)
        for model in (full, cached_experiment_views(dataset, history.live())):
            self.assertEqual(
                {(edge["from"], edge["to"]) for edge in self.moves(model)},
                {("A", "C"), ("C", "C"), ("C", "A")},
            )
            self.assertEqual(len(self.moves(model)), 3)
            self.assertEqual(
                sum(edge.get("active", False) for edge in self.moves(model)), 1
            )
            self.assertIn({"from": "B", "to": "C"}, model["template"]["edges"])
            self.assertIs(model["template"]["nodes"][-1]["returns_data"], False)

    def test_scope_selection_and_offline_state_do_not_reuse_other_moves(self):
        """D05: another cycle/revision/run has its own progress, without fake liveness."""
        for field, value in (
            ("cycle_number", 2),
            ("template_revision_id", "rev-2"),
            ("run_id", "run-2"),
        ):
            with self.subTest(field=field):
                history = ConditionalHistory()
                history.move()
                history.state.update(
                    stage_result_ids={},
                    pending_input=None,
                    last_dag_decision=None,
                    pending_advance=False,
                    stage_position=1,
                )
                history.state[field] = value
                history.template_applied()
                history.checkpoint()
                _, _, dataset = self.cache(history)
                for model in (
                    experiment_views(history.dataset(), history.live()),
                    cached_experiment_views(dataset, history.live()),
                ):
                    self.assertEqual(self.moves(model), [])
                    self.assertEqual(
                        self.statuses(model),
                        dict.fromkeys(("P", "A", "B", "C"), "pending"),
                    )
                if field == "run_id":
                    old = cached_experiment_views(dataset, history.live(), "run-test")
                    self.assertEqual(
                        [(edge["from"], edge["to"]) for edge in self.moves(old)],
                        [("C", "A")],
                    )
                    self.assertFalse(
                        any(node["current"] for node in old["template"]["nodes"])
                    )
        history = ConditionalHistory()
        history.move()
        history.start_target()
        _, _, dataset = self.cache(history)
        for model in (
            experiment_views(history.dataset()),
            cached_experiment_views(dataset, {}),
        ):
            self.assertEqual(self.statuses(model)["A"], "unconfirmed")
            self.assertFalse(
                any(node["current"] for node in model["template"]["nodes"])
            )

    def test_incompatible_cache_rebuilds_to_one_without_changing_source(self):
        """D06: discarded development formats rebuild; journal facts remain intact."""
        history = ConditionalHistory()
        history.move()
        workspace, reader, dataset = self.cache(history)
        before = workspace.logger.read_events(limit=1000)
        cache = dataset["cache"]
        for version in (5, 6):
            with self.subTest(version=version):
                with closing(sqlite3.connect(cache.path)) as db, db:
                    source = json.loads(
                        db.execute(
                            "SELECT value FROM metadata WHERE key='source'"
                        ).fetchone()[0]
                    )
                    source["version"] = version
                    db.execute(
                        "UPDATE metadata SET value=? WHERE key='source'",
                        (json.dumps(source),),
                    )
                reader.close()
                reader = LocalJournals(reader.settings)
                self.addCleanup(reader.close)
                self.assertNotIn("cache", reader.cached("exp-test"))
                rebuilt = reader.load("exp-test", force=True)
                cache = rebuilt["cache"]
                source = json.loads(
                    cache.query("SELECT value FROM metadata WHERE key='source'")[0][0]
                )
                self.assertEqual(source["version"], 1)
                self.assertEqual(JournalHistoryCache.SCHEMA_VERSION, 1)
                self.assertEqual(
                    self.statuses(cached_experiment_views(rebuilt, history.live())),
                    {"P": "succeeded", "A": "pending", "B": "pending", "C": "pending"},
                )
                self.assertEqual(workspace.logger.read_events(limit=1000), before)

    def test_compaction_retains_only_the_execution_metadata_needed_by_the_dag(self):
        """D03/D07: preserve reset/identity fields without copying application payloads."""
        history = ConditionalHistory()
        history.move()
        event = copy.deepcopy(history.dataset()["entries"][-1])
        event["data"].update(
            last_result={"large": "payload"}, template_yaml="large YAML"
        )
        compact = compact_event(event)
        self.assertEqual(compact["data"]["stage_result_ids"], {"P": "P-1"})
        self.assertEqual(
            compact["data"]["last_dag_decision"], event["data"]["last_dag_decision"]
        )
        self.assertEqual(
            compact["data"]["pending_input"], event["data"]["pending_input"]
        )
        self.assertNotIn("last_result", compact["data"])
        self.assertNotIn("template_yaml", compact["data"])


class ConditionalRuntimeStateTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_publishes_detached_current_progress_and_active_identity(
        self,
    ):
        """D07: actual stage transitions expose precisely the state used by DAG views."""
        async with asyncio.timeout(120):
            work = SnapshotWorkspace(services=False, cycles=1)
            self.addAsyncCleanup(work.close)
            seed, skipped, target = work.stages
            gate = work.files.gate()
            target["settings"]["gate"] = str(gate)
            condition = work.files.stage(
                source=Path(__file__).resolve().parents[1]
                / "helpers/conditional_stage.py",
                stage_kind="conditional",
                settings={
                    "decision": {
                        "command": "move",
                        "stage_id": target["stage_id"],
                        "data": False,
                    }
                },
            )
            condition["returns_data"] = True
            work.template["stages"] = [seed, condition, skipped, target]
            runner = await work.launch()
            initial = runner.get_state()
            self.assertEqual(initial["stage_result_ids"], {})
            self.assertFalse(initial["pending_advance"])
            self.assertIsNone(initial["active_attempt_id"])
            await runner.step()
            published = runner.get_state()
            self.assertTrue(published["pending_advance"])
            self.assertEqual(set(published["stage_result_ids"]), {seed["stage_id"]})
            published["stage_result_ids"].clear()
            self.assertIn(seed["stage_id"], runner.get_state()["stage_result_ids"])
            await runner.step()
            moved = runner.get_state()
            self.assertEqual(moved["stage_position"], 4)
            self.assertFalse(moved["pending_advance"])
            self.assertIsNone(moved["active_attempt_id"])
            self.assertNotIn(target["stage_id"], moved["stage_result_ids"])
            step = asyncio.create_task(runner.step())
            try:
                await wait_for(
                    lambda: (
                        runner._state.active_attempt is not None
                        and (
                            runner._state.active_attempt.artifacts_directory
                            / "ready.json"
                        ).exists()
                    ),
                    30,
                )
                active = runner.get_state()
                self.assertEqual(active["phase"], "stage_running")
                self.assertEqual(
                    active["active_attempt_id"], runner._state.active_attempt.attempt_id
                )
                self.assertNotEqual(
                    active["active_attempt_id"], moved["pending_input"]["request_id"]
                )
            finally:
                gate.touch()
                result = await asyncio.wait_for(step, 60)
            self.assertIs(result["result"]["data"]["input"], False)
            final = runner.get_state()
            self.assertIsNone(final["active_attempt_id"])
            self.assertIn(target["stage_id"], final["stage_result_ids"])
