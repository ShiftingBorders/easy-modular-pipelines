"""Approved G: real process precache and cache publication lifecycles."""

import json
import os
import subprocess
import sys
import unittest
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path

from core.historycache import acquire_cache_writer
from dashboard.__main__ import precache
from dashboard.config import load_settings
from dashboard.journals import LocalJournals, cache_experiment
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)
from tests.dashboard_tests.integration_helpers import JournalWorkspace, history


class PrecacheTests(unittest.TestCase):
    def setUp(self):
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        self.root = Path(temporary.name)
        self.workspaces = []
        for index in range(3):
            data = history(((1, 2),))
            data["state"]["phase"] = "stopped"
            workspace = JournalWorkspace(
                self.root / "project",
                data,
                identifier=f"exp-{index}",
                folder=f"experiment-{index}",
            )
            self.addCleanup(workspace.close)
            self.workspaces.append(workspace)
        self.config = write_settings(
            self.root, project_root=str(self.root / "project"), cache_workers=2
        )
        self.settings = load_settings(self.config)
        self.reader = LocalJournals(self.settings)
        self.addCleanup(self.reader.close)

    def test_cli_precaches_stopped_experiments_in_independent_processes(self):
        """T015/T045/T046/T047/T051/T066: finite targets, process PIDs, final Modules."""
        snapshots = [
            list(workspace.logger._store._connection.iterdump())
            for workspace in self.workspaces
        ]
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "dashboard",
                "--config",
                str(self.config),
                "--mode",
                "precache",
            ],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        records = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(records[-1], {"mode": "precache", "completed": 3, "failed": 0})
        progress = [row for row in records if "pid" in row]
        self.assertEqual(
            {row["experiment_id"] for row in progress}, {"exp-0", "exp-1", "exp-2"}
        )
        self.assertGreater(len({row["pid"] for row in progress}), 1)
        self.assertNotIn(os.getpid(), {row["pid"] for row in progress})
        for row in progress:
            if row["complete"]:
                self.assertGreaterEqual(
                    row["cached_through"]["change_cursor"],
                    row["target_boundary"]["change_cursor"],
                )
        self.assertTrue(self.reader.modules()["complete"])
        for index, workspace in enumerate(self.workspaces):
            self.assertEqual(
                list(workspace.logger._store._connection.iterdump()), snapshots[index]
            )
        self.assertFalse((self.settings["state_directory"] / "monitor.lock").exists())
        self.assertFalse((self.settings["state_directory"] / "commands.json").exists())

    def test_fixed_target_finishes_despite_later_appends(self):
        """T045/T027: appends after a captured target cannot prolong that task."""
        first = cache_experiment(self.settings, "exp-0")
        self.assertTrue(first["complete"])
        target = first["target_boundary"]
        self.workspaces[0].logger.record_event("after-target")
        repeated = cache_experiment(self.settings, "exp-0", target)
        self.assertTrue(repeated["complete"])
        self.assertEqual(repeated["target_boundary"], target)
        later = cache_experiment(self.settings, "exp-0")
        self.assertGreater(
            later["cached_through"]["cursor"], first["cached_through"]["cursor"]
        )

    def test_writer_lock_is_visible_in_another_native_process(self):
        """T048/T050: Windows and POSIX workers observe the same writer exclusion."""
        cache_experiment(self.settings, "exp-0")
        dataset = self.reader.cached("exp-0")
        with ProcessPoolExecutor(
            max_workers=1, mp_context=get_context("spawn")
        ) as pool:
            with acquire_cache_writer(dataset["cache"].path):
                result = pool.submit(cache_experiment, self.settings, "exp-0").result(
                    timeout=30
                )
                self.assertEqual(result["error"]["code"], "cache_busy")
            result = pool.submit(cache_experiment, self.settings, "exp-0").result(
                timeout=30
            )
            self.assertTrue(result["complete"])

    def test_one_failed_source_does_not_discard_other_experiment_progress(self):
        """T049/T051: failed experiment exit code and independent successful caches."""
        self.workspaces[0].close()
        (self.workspaces[0].directory / "journals/events.sqlite").unlink()
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dashboard",
                "--config",
                str(self.config),
                "--mode",
                "precache",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertTrue(self.reader.cached("exp-1")["complete"])
        self.assertTrue(self.reader.cached("exp-2")["complete"])
        self.assertFalse(self.reader.modules()["complete"])

    def test_no_project_is_a_configuration_error_and_empty_registry_finishes(self):
        """T051/T065: configuration failure and a valid empty project differ."""
        with self.assertRaisesRegex(ValueError, "project_root"):
            precache({**self.settings, "project_root": None})
        (self.root / "project/experiments.json").write_text("{}", encoding="utf-8")
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "dashboard",
                "--config",
                str(self.config),
                "--mode",
                "precache",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout.splitlines()[-1])["completed"], 0)
        self.assertEqual(self.reader.modules()["items"], [])
