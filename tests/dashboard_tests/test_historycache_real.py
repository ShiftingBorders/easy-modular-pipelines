"""Approved T092: portable regression from the recorded Heilbronn experiment."""

import gzip
import json
import multiprocessing
import os
import time
import unittest
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import psutil

from dashboard.config import load_settings
from dashboard.journals import LocalJournals, cache_experiment
from dashboard.projections import cached_experiment_views, experiment_views
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import JournalWorkspace


class RealHistoryCacheTests(unittest.TestCase):
    def test_real_history_worker_memory_respects_single_and_multiple_budgets(self):
        """T087/T089/T090: include native worker RSS in both memory budgets."""
        fixture = Path(__file__).with_name("fixtures") / "heilbronn"
        with gzip.open(fixture / "history.json.gz", "rt", encoding="utf-8") as stream:
            history = json.load(stream)
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        root = Path(temporary.name)
        for index in range(2):
            workspace = JournalWorkspace(
                root / "project",
                history,
                identifier=f"exp-{index}",
                folder=f"experiment-{index}",
            )
            self.addCleanup(workspace.close)
        measurements = []
        for workers, budget in ((1, 2_000_000_000), (2, 4_000_000_000)):
            settings = load_settings(
                write_settings(
                    root,
                    project_root=str(root / "project"),
                    state_directory=str(root / f"state-{workers}"),
                    cache_workers=workers,
                )
            )
            peak = psutil.Process().memory_info().rss
            with ProcessPoolExecutor(
                max_workers=workers, mp_context=multiprocessing.get_context("spawn")
            ) as pool:
                pending = {f"exp-{index}": None for index in range(workers)}
                for _ in range(len(history["entries"])):
                    futures = {
                        identifier: pool.submit(
                            cache_experiment, settings, identifier, target
                        )
                        for identifier, target in pending.items()
                    }
                    while any(not future.done() for future in futures.values()):
                        memory = psutil.Process().memory_info().rss
                        for process in psutil.Process().children(recursive=True):
                            try:
                                memory += process.memory_info().rss
                            except psutil.NoSuchProcess:
                                pass
                        peak = max(peak, memory)
                        self.assertLessEqual(memory, budget)
                        time.sleep(0.02)
                    for identifier, future in futures.items():
                        result = future.result()
                        self.assertNotIn("error", result)
                        if result["complete"]:
                            del pending[identifier]
                        else:
                            pending[identifier] = result["target_boundary"]
                    if not pending:
                        break
                self.assertFalse(pending)
            measurements.append(
                {"workers": workers, "peak_rss_bytes": peak, "budget_bytes": budget}
            )
        destination = (
            Path(__file__).resolve().parents[2]
            / ".artifacts/logs"
            / f"dashboard-cache-memory-{os.name}.json"
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(measurements, indent=2), encoding="utf-8")

    def test_heilbronn_history_matches_frozen_source_results(self):
        """T092: real cycles, screen records and module statistics survive caching."""
        fixture = Path(__file__).with_name("fixtures") / "heilbronn"
        with gzip.open(fixture / "history.json.gz", "rt", encoding="utf-8") as stream:
            history = json.load(stream)
        expected = json.loads((fixture / "expected.json").read_text(encoding="utf-8"))
        full = experiment_views(history)
        self.assertEqual(full["summary"], expected["summary"])
        self.assertEqual(full["measurements"], expected["measurements"])
        self.assertEqual(
            {key: full["forecast"][key] for key in expected["forecast"]},
            expected["forecast"],
        )

        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        root = Path(temporary.name)
        workspace = JournalWorkspace(root / "project", history)
        self.addCleanup(workspace.close)
        settings = load_settings(
            write_settings(
                root, project_root=str(workspace.root), history_window_events=10
            )
        )
        reader = LocalJournals(settings)
        self.addCleanup(reader.close)
        dataset = reader.load("exp-test", force=True)
        target = dataset["target_boundary"]
        # A cold cache can require several bounded worker-sized batches.
        for _ in range(len(history["entries"])):
            if dataset["complete"]:
                break
            dataset = reader.load("exp-test", force=True, target=target)
        self.assertTrue(dataset["complete"])
        self.assertEqual(dataset["window_count"], 10)
        model = reader.read_view(dataset, cached_experiment_views, {})
        self.assertEqual(model["summary"], expected["summary"])
        self.assertEqual(
            {key: model["forecast"][key] for key in expected["forecast"]},
            expected["forecast"],
        )
        measurements = reader.page(dataset, "measurements", {"compact": "1"})
        self.assertEqual(measurements["total"], len(expected["measurements"]))
        self.assertCountEqual(
            [
                {key: value for key, value in row.items() if key != "detail_ref"}
                for row in measurements["items"]
            ],
            expected["measurements"],
        )
        for kind, fields in expected["record_fields"].items():
            with self.subTest(view=kind):
                page = reader.page(dataset, kind, {"compact": "1"})
                self.assertEqual(page["total"], len(expected["records"][kind]))
                self.assertCountEqual(
                    [{key: row.get(key) for key in fields} for row in page["items"]],
                    expected["records"][kind],
                )
        self.assertTrue(reader.publish_modules())
        publication = reader.modules()
        self.assertTrue(publication["complete"])
        module_fields = tuple(expected["modules"][0])
        self.assertCountEqual(
            [{key: row[key] for key in module_fields} for row in publication["items"]],
            expected["modules"],
        )
