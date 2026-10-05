"""Approved I: exact full-history module publications and their read path."""

import hashlib
import json
import sqlite3
import unittest
from contextlib import ExitStack, closing
from pathlib import Path
from unittest.mock import patch

from core.journal.logger import OperationLogger
from core.models.journal_cache import CacheIdentity, CacheSource
from dashboard.api_client import SystemAPIError
from dashboard.config import load_settings
from dashboard.journals import LocalJournals, cache_experiment
from dashboard.module_publication import ModuleDataset, ModuleSource
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import JournalWorkspace, history


class ModulePublicationTests(unittest.TestCase):
    def test_module_source_models_reach_native_inventory_and_callback_boundaries(self):
        publication = self.publish()
        with ExitStack() as resources:
            datasets, sources = self.reader._module_sources(
                self.reader.registry(), resources
            )
            for dataset in datasets:
                self.assertIsInstance(dataset, ModuleDataset)
                self.assertIsInstance(dataset.identity, CacheIdentity)
                source = sources[dataset.experiment_id]
                self.assertIsInstance(source, ModuleSource)
                self.assertIsInstance(source.cache, CacheSource)
                self.assertIs(dataset.identity, source.cache.identity)
                self.assertEqual(
                    source.document(), publication["sources"][dataset.experiment_id]
                )
                self.assertEqual(
                    dataset.document()["identity"], source.cache.identity.model_dump()
                )
                self.assertEqual(
                    dataset.connection.execute("SELECT 1").fetchone(), (1,)
                )

    def test_corrupt_metadata_keeps_its_existing_publication_read_error_boundary(self):
        self.publish()
        key = hashlib.sha256(b"exp-0").hexdigest()
        with (
            closing(
                sqlite3.connect(self.reader.state_directory / f"{key}.cache.sqlite")
            ) as database,
            database,
        ):
            database.execute("UPDATE metadata SET value='-1' WHERE key='version'")
        self.assertTrue(self.reader.publish_modules())
        document = json.loads(
            (self.reader.state_directory / "modules.json").read_text(encoding="utf-8")
        )
        self.assertEqual(document["sources"]["exp-0"]["version"], -1)
        with self.assertRaises(SystemAPIError) as error:
            self.reader.modules()
        self.assertEqual(error.exception.code, "cache_unavailable")
        self.assertEqual(str(error.exception), "Invalid module statistics publication.")

    def setUp(self):
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        self.root = Path(temporary.name)
        self.workspaces = []
        for index, durations in enumerate((((1, 10), (2, 20)), ((100, 1000),))):
            workspace = JournalWorkspace(
                self.root / "project",
                history(durations),
                identifier=f"exp-{index}",
                folder=f"experiment-{index}",
            )
            self.workspaces.append(workspace)
            self.addCleanup(workspace.close)
        self.settings = load_settings(
            write_settings(
                self.root,
                project_root=str(self.root / "project"),
                history_window_events=1,
            )
        )
        self.reader = LocalJournals(self.settings)
        self.addCleanup(self.reader.close)

    def publish(self):
        for identifier in self.reader.registry():
            for _ in range(30):
                result = cache_experiment(self.settings, identifier)
                self.assertNotIn("error", result)
                if result["complete"]:
                    break
            self.assertTrue(result["complete"])
        self.assertTrue(self.reader.publish_modules())
        return self.reader.modules()

    def test_full_cohort_percentiles_are_not_averaged_per_experiment(self):
        """T060/T061/T062: exact counts and nearest-rank quantiles across histories."""
        publication = self.publish()
        rows = {row["name"]: row for row in publication["items"]}
        self.assertEqual(rows["short"]["runs"], 3)
        self.assertEqual(rows["short"]["p50_seconds"], 2)
        self.assertEqual(rows["short"]["p95_seconds"], 100)
        self.assertEqual(rows["long"]["p50_seconds"], 20)
        self.assertEqual(rows["long"]["p95_seconds"], 1000)
        self.assertEqual(rows["short"]["restarts"], 0)
        self.assertEqual(rows["short"]["error_count"], 0)
        self.assertTrue(publication["complete"])

    def test_get_reuses_publication_without_source_reads_or_aggregation(self):
        """T063/T064/T024: warm reads and unchanged publications are reused."""
        self.publish()
        path = self.reader.state_directory / "modules.json"
        before = path.stat().st_mtime_ns
        with patch.object(
            OperationLogger,
            "open",
            side_effect=AssertionError("Modules opened journal"),
        ):
            first = self.reader.modules()
            with patch(
                "dashboard.journals.read_object",
                side_effect=AssertionError("Reparsed unchanged publication"),
            ):
                retained = self.reader._module_publication
                self.assertEqual(first, self.reader.modules())
                self.assertIs(retained, self.reader._module_publication)
            self.assertTrue(self.reader.publish_modules())
        self.assertEqual(path.stat().st_mtime_ns, before)
        other = LocalJournals(self.settings)
        self.addCleanup(other.close)
        self.assertEqual(other.modules(), first)

    def test_registry_changes_and_invalid_publications_are_explicit(self):
        """T065: missing, foreign, corrupt and changed project publications."""
        self.assertFalse(self.reader.modules()["complete"])
        self.publish()
        registry = self.root / "project/experiments.json"
        registry.write_text(json.dumps({"exp-0": "experiment-0"}), encoding="utf-8")
        self.assertTrue(self.reader.publish_modules())
        self.assertEqual(set(self.reader.modules()["sources"]), {"exp-0"})
        path = self.reader.state_directory / "modules.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        path.write_text(
            json.dumps({**original, "project_root": "another-project"}),
            encoding="utf-8",
        )
        self.assertFalse(self.reader.modules()["complete"])
        path.write_text("{invalid", encoding="utf-8")
        with self.assertRaises(SystemAPIError):
            self.reader.modules()

    def test_module_failure_preserves_completed_experiment_cache(self):
        """T066: module publication failure is separate from history completion."""
        with patch(
            "dashboard.journals.LocalJournals.publish_modules",
            side_effect=OSError("publication failed"),
        ):
            result = cache_experiment(self.settings, "exp-0")
        self.assertTrue(result["complete"])
        self.assertIn("publication failed", result["modules_error"])
        dataset = self.reader.cached("exp-0")
        self.assertTrue(dataset["complete"])
        self.assertEqual(
            self.reader.page(dataset, "parameters", {"compact": "1"})["total"], 4
        )

    def test_recent_attempt_limit_does_not_truncate_counts_or_quantiles(self):
        """T062/T089: 100 displayed attempts do not bound the historical cohort."""
        workspace = JournalWorkspace(
            self.root / "project",
            history(tuple((1, 2) for _ in range(101)), total=101),
            identifier="many",
            folder="many",
        )
        self.addCleanup(workspace.close)
        rows = self.publish()["items"]
        short = next(row for row in rows if row["name"] == "short")
        self.assertEqual(short["runs"], 104)
        self.assertEqual(len(short["recent_attempts"]), 100)
        self.assertEqual(short["p50_seconds"], 1)
        self.assertEqual(short["p95_seconds"], 1)
