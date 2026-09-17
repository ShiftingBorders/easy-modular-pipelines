"""Approved resource_collector.md A: settings, identity, and bounded RAM history."""

import json
import os
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

from core.resource_utils.state import CollectorSettings, ResourceHistory, ResourceTarget
from core.runner_utils.runtimeio import process_identity
from tests.helpers.resources import (
    DEFAULT_CONFIG,
    TEMP_ROOT,
    settings,
    target,
    write_settings,
)


class ResourceStateTests(unittest.TestCase):
    def setUp(self):
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=TEMP_ROOT)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

    def test_agreed_defaults_and_explicit_file_do_not_depend_on_cwd(self):
        """A: agreed default values and explicit paths are read from their files."""
        expected = {
            "sample_interval_seconds": 1,
            "history_seconds": 900,
            "max_buffer_bytes": 16777216,
            "stale_after_intervals": 3,
            "status_interval_seconds": 1,
            "startup_timeout_seconds": 10,
            "heartbeat_timeout_seconds": 10,
            "shutdown_timeout_seconds": 5,
            "restart_delays_seconds": [1, 2, 4, 8, 16, 30],
            "stable_reset_seconds": 60,
            "logging_busy_timeout_seconds": 0.05,
            "logging_retry_seconds": 5,
            "disk_path": str(DEFAULT_CONFIG.parent.parent.resolve()),
            "network_interface": None,
            "network_reference_address": "1.1.1.1",
            "gpu_interval_seconds": 5,
        }
        path = write_settings(self.root, history_seconds=42)
        before = Path.cwd()
        try:
            os.chdir(self.root)
            self.assertEqual(asdict(CollectorSettings.load(DEFAULT_CONFIG)), expected)
            self.assertEqual(CollectorSettings.load(path).history_seconds, 42)
            with self.assertRaises(ValueError):
                CollectorSettings.load(Path("settings.json"))
        finally:
            os.chdir(before)

    def test_settings_reject_missing_unknown_malformed_and_invalid_values(self):
        """A: schema/type/range errors fail before processes or journals are created."""
        path = write_settings(self.root)
        valid = json.loads(path.read_text())
        documents = [[], {}, {**valid, "extra": 1}]
        documents.extend(
            {key: value for key, value in valid.items() if key != removed}
            for removed in valid
        )
        for key in valid.keys() - {
            "disk_path",
            "network_interface",
            "network_reference_address",
        }:
            for value in (True, None, "1", -1, 0, float("inf"), float("nan")):
                documents.append({**valid, key: value})
        for key in ("disk_path", "network_interface", "network_reference_address"):
            for value in (True, 1, [], {}, ""):
                documents.append({**valid, key: value})
        documents.append({**valid, "network_reference_address": "not-an-ip"})
        for key, value in (
            ("sample_interval_seconds", 0.01),
            ("max_buffer_bytes", 4095),
            ("stale_after_intervals", 1.5),
            ("max_buffer_bytes", 4096.5),
            ("logging_busy_timeout_seconds", 61),
            ("heartbeat_timeout_seconds", valid["status_interval_seconds"]),
            ("restart_delays_seconds", []),
            ("restart_delays_seconds", [2, 1]),
            ("restart_delays_seconds", [1, False]),
        ):
            documents.append({**valid, key: value})
        for document in documents:
            with self.subTest(document=document):
                path.write_text(json.dumps(document), encoding="utf-8")
                with self.assertRaises((TypeError, ValueError)):
                    CollectorSettings.load(path)
        path.write_text("{", encoding="utf-8")
        with self.assertRaises(ValueError):
            CollectorSettings.load(path)
        with self.assertRaises(FileNotFoundError):
            CollectorSettings.load(self.root / "missing.json")

    def test_target_requires_complete_identity_and_detaches_input(self):
        """A/B: targets retain exact OS identity and cannot alias caller state."""
        document = target(process_identity(os.getpid()))
        parsed = ResourceTarget.from_document(document)
        document["identity"]["pid"] += 1
        self.assertNotEqual(parsed.identity["pid"], document["identity"]["pid"])
        for field in ("pid", "created_at_os", "host_id", "boot_id"):
            invalid = target(dict(parsed.identity))
            del invalid["identity"][field]
            with self.subTest(field=field), self.assertRaises(ValueError):
                ResourceTarget.from_document(invalid)

    def test_history_age_byte_budget_and_eviction_are_observable(self):
        """A: old entries disappear by age or size with visible cursor gaps."""
        history = ResourceHistory(
            settings(self.root, max_buffer_bytes=4096, history_seconds=2)
        )
        with patch("core.resource_utils.state.time.monotonic", return_value=10):
            for index in range(20):
                history.append({"value": "x" * 300, "index": index})
            page = history.read()
        self.assertLessEqual(page["buffer_bytes"], 4096)
        self.assertGreater(page["evicted_samples"], 0)
        self.assertTrue(page["gap"])
        self.assertEqual(page["samples"][-1]["index"], 19)
        with patch("core.resource_utils.state.time.monotonic", return_value=13):
            expired = history.read(after=page["cursor"])
        self.assertEqual(expired["samples"], [])
        self.assertEqual(expired["buffer_bytes"], 0)
        self.assertEqual(expired["evicted_samples"], 20)

    def test_history_pages_cursors_limits_and_detached_values(self):
        """A: paging neither repeats entries nor exposes mutable stored objects."""
        history = ResourceHistory(settings(self.root))
        payload = {"nested": {"value": 1}}
        history.append(payload)
        payload["nested"]["value"] = 9
        for index in range(5):
            history.append({"index": index})
        first = history.read(limit=2)
        self.assertEqual(first["samples"][0]["nested"]["value"], 1)
        first["samples"][0]["nested"]["value"] = 99
        self.assertEqual(history.read(limit=1)["samples"][0]["nested"]["value"], 1)
        second = history.read(after=first["cursor"], limit=4)
        self.assertEqual([item["cursor"] for item in second["samples"]], [3, 4, 5, 6])
        self.assertEqual(history.read(after=6)["samples"], [])
        self.assertTrue(history.read(after=999)["gap"])
        self.assertEqual(history.read(after=999)["cursor"], 6)
        self.assertNotEqual(
            history.history_id, ResourceHistory(settings(self.root)).history_id
        )
        for args in (
            {"limit": 0},
            {"limit": 1001},
            {"limit": True},
            {"after": -1},
            {"after": False},
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                history.read(**args)

    def test_history_pages_bound_encoded_response_size(self):
        """A: large retained samples must not bypass the page byte limit."""
        history = ResourceHistory(settings(self.root))
        for _ in range(4):
            history.append({"value": "x" * 400000})
        page = history.read(limit=1000)
        self.assertLessEqual(
            sum(len(json.dumps(item).encode()) for item in page["samples"]), 1048576
        )
        self.assertEqual(len(page["samples"]), 2)

    def test_oversized_single_sample_cannot_bypass_page_limit_or_hide_a_gap(self):
        """A: an individual oversized sample is discarded with a visible cursor gap."""
        history = ResourceHistory(settings(self.root))
        history.append({"value": 1})
        history.append({"value": "x" * 1100000})
        history.append({"value": 3})
        page = history.read()
        self.assertLessEqual(
            sum(len(json.dumps(item).encode()) for item in page["samples"]), 1048576
        )
        self.assertEqual([item["value"] for item in page["samples"]], [1, 3])
        self.assertTrue(page["gap"])
        self.assertEqual(page["evicted_samples"], 1)
        self.assertEqual(page["cursor"], 3)
