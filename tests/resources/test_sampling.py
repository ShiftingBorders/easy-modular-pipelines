"""Approved resource_collector.md B: real measurements and deterministic CPU math."""

import asyncio
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import psutil

from core.models.journal_records import JournalContext
from core.models.resource_messages import (
    CollectorSnapshot,
    CollectorUpdate,
    ResourceMeasurement,
    ResourceSample,
)
from core.models.resource_target import ResourceTargetDocument
from core.primitives.processes import process_identity
from core.resources.observations import MeasuredResource, ResourceObservation
from core.resources.sampling import ResourceSampler, _apply_collector_update
from core.resources.state import ResourceTarget
from tests.helpers.dag import wait_until
from tests.helpers.resources import (
    memory_and_cpu_process,
    spawned,
    stop_process,
    target,
)


class SamplerTests(unittest.TestCase):
    def test_observation_models_retain_measurements_and_public_json_values(self):
        measurement = self.sampler._measurement(25, "percent", "host", "time")
        self.assertIsInstance(measurement, MeasuredResource)
        measurement = ResourceMeasurement.model_validate(measurement.document())
        context = JournalContext.model_validate({"experiment_id": "experiment"})
        sample = ResourceSample.model_validate(
            {
                "series_id": "host:test",
                "context": context,
                "observed_at": "time",
                "observed_monotonic": 10,
                "resources": {"cpu": measurement},
                "extra": {"value": 1.0},
            }
        )
        self.assertIs(sample.context, context)
        self.assertIs(sample.resources["cpu"], measurement)
        expected = sample.model_dump(exclude_unset=True)
        emitted = sample.model_dump(exclude_unset=True)
        emitted["resources"]["cpu"]["attributes"]["external"] = True
        self.assertEqual(sample.model_dump(exclude_unset=True), expected)
        target_model = ResourceTargetDocument.model_validate(
            {
                "series_id": self.target.series_id,
                "identity": self.identity,
                "context": self.target.context,
            }
        )
        observed = self.sampler._process_observation(target_model)
        self.assertIsInstance(observed, ResourceObservation)
        self.assertTrue(
            all(
                isinstance(item, MeasuredResource)
                for item in observed.resources.values()
            )
        )
        self.assertEqual(
            observed.resources["process_memory_rss_bytes"].attributes[
                "observed_process"
            ],
            self.identity,
        )

    def test_native_observations_keep_the_original_public_validation_boundary(self):
        import math

        context = {"custom": "value"}
        sample = self.sampler._host_sample(context)
        self.assertIs(sample["context"], context)
        measurement = self.sampler._measurement(float("nan"), "percent", "host", "time")
        self.assertTrue(math.isnan(measurement.document()["value"]))
        with self.assertRaises(ValueError):
            ResourceMeasurement.model_validate(measurement.document())

    def test_child_update_retains_models_and_context_with_original_wire_shape(self):
        path = (Path.cwd() / ".artifacts/tmp/Resource.json").resolve()
        document = {
            "command": "update",
            "revision": 1,
            "snapshot": {
                "context": {"experiment_id": "experiment"},
                "logging_config_path": str(path),
                "targets": [target(process_identity(os.getpid()))],
            },
            "restart_notice": {
                "reason": "collector_restarted",
                "error": "closed",
                "previous_collector_id": None,
                "context": {"experiment_id": "experiment"},
            },
        }
        message = CollectorUpdate.model_validate(document)
        initial = CollectorSnapshot.model_validate(
            {"context": {}, "logging_config_path": None, "targets": []}
        )
        writer, sampler = Mock(), Mock()
        result = _apply_collector_update(
            message, writer, sampler, initial, [], 0, False, False
        )
        snapshot, targets, revision, received_notice, changed = result
        self.assertIs(snapshot, message.snapshot)
        self.assertIs(targets[0], message.snapshot.targets[0])
        self.assertIsInstance(targets[0], ResourceTargetDocument)
        self.assertIsInstance(snapshot.context, JournalContext)
        self.assertIs(writer.restart_notice, message.restart_notice)
        self.assertEqual((revision, received_notice, changed), (1, True, True))
        writer.select.assert_called_once_with(path)
        sampler.reset.assert_called_once_with()
        self.assertEqual(
            snapshot.model_dump(mode="json"), message.snapshot.model_dump(mode="json")
        )
        exported = ResourceTarget.from_document(document["snapshot"]["targets"][0])
        self.assertEqual(
            exported.identity, document["snapshot"]["targets"][0]["identity"]
        )
        self.assertEqual(
            exported.context, document["snapshot"]["targets"][0]["context"]
        )

    def setUp(self):
        self.sampler = ResourceSampler("test-collector")
        self.identity = process_identity(os.getpid())
        self.target = ResourceTarget.from_document(target(self.identity))

    def test_model_and_public_targets_share_the_same_process_cpu_baseline(self):
        retained = ResourceTargetDocument.model_validate(
            {
                "series_id": self.target.series_id,
                "identity": self.identity,
                "context": self.target.context,
            }
        )
        with (
            patch("core.resources.sampling.psutil.Process") as process,
            patch(
                "core.resources.sampling.process_identity", return_value=self.identity
            ),
            patch("core.resources.sampling.time.monotonic", side_effect=[10, 12, 13]),
        ):
            process.return_value.cpu_times.side_effect = [
                SimpleNamespace(user=value, system=0) for value in (0, 1, 4)
            ]
            process.return_value.memory_info.return_value = SimpleNamespace(rss=4096)
            samples = [
                self.sampler._process_sample(item)
                for item in (retained, self.target, retained)
            ]
        self.assertEqual(
            [item["resources"]["process_cpu_percent"]["value"] for item in samples],
            [None, 50, 300],
        )

    def test_cpu_intervals_values_over_100_and_child_cpu_exclusion(self):
        """B: controlled CPU seconds give null, 50%, and 300%; children are excluded."""
        times = [
            SimpleNamespace(
                user=value, system=0, children_user=1000, children_system=1000
            )
            for value in (0, 1, 4)
        ]
        with (
            patch("core.resources.sampling.psutil.Process") as process,
            patch(
                "core.resources.sampling.process_identity",
                return_value=self.identity,
            ),
            patch(
                "core.resources.sampling.time.monotonic", side_effect=[10, 12, 13]
            ),
        ):
            process.return_value.cpu_times.side_effect = times
            process.return_value.memory_info.return_value = SimpleNamespace(rss=4096)
            samples = [self.sampler._process_sample(self.target) for _ in range(3)]
        measurements = [
            sample["resources"]["process_cpu_percent"] for sample in samples
        ]
        self.assertEqual([item["value"] for item in measurements], [None, 50, 300])
        self.assertEqual(
            [item["attributes"]["interval_seconds"] for item in measurements],
            [None, 2, 1],
        )
        self.assertEqual(measurements[0]["attributes"]["reason"], "first_interval")
        for measurement in measurements:
            self.assertEqual(
                (measurement["unit"], measurement["kind"], measurement["scope"]),
                ("percent", "gauge", "process"),
            )
            self.assertEqual(
                measurement["attributes"]["observed_process"], self.identity
            )

    def test_counter_reset_and_new_attempt_restart_the_cpu_baseline(self):
        """B: decreasing counters and new series cannot inherit earlier CPU usage."""
        other = ResourceTarget.from_document(target(self.identity))
        with patch("core.resources.sampling.psutil.Process") as process:
            process.return_value.cpu_times.side_effect = [
                SimpleNamespace(user=value, system=0) for value in (5, 2, 4, 5)
            ]
            process.return_value.memory_info.return_value = SimpleNamespace(rss=4096)
            self.sampler._process_sample(self.target)
            reset = self.sampler._process_sample(self.target)
            fresh = self.sampler._process_sample(other)
            self.sampler.reset()
            restarted = self.sampler._process_sample(other)
        self.assertEqual(
            reset["resources"]["process_cpu_percent"]["attributes"]["reason"],
            "counter_reset",
        )
        for sample in (fresh, restarted):
            self.assertIsNone(sample["resources"]["process_cpu_percent"]["value"])
            self.assertEqual(
                sample["resources"]["process_cpu_percent"]["attributes"]["reason"],
                "first_interval",
            )

    def test_pid_reuse_before_and_during_read_discards_the_measurement(self):
        """B: an identity change on either side of a read never becomes valid data."""
        changed = {**self.identity, "created_at_os": self.identity["created_at_os"] + 1}
        for identities in ([changed], [self.identity, changed]):
            with (
                self.subTest(identities=identities),
                patch(
                    "core.resources.sampling.process_identity",
                    side_effect=identities,
                ),
            ):
                sample = self.sampler._process_sample(self.target)
            for measurement in sample["resources"].values():
                self.assertIsNone(measurement["value"])
                self.assertEqual(
                    measurement["attributes"]["reason"], "identity_changed"
                )

    def test_missing_process_access_denial_and_foreign_host_are_unavailable(self):
        """B: missing/inaccessible/foreign targets produce null with explicit reasons."""
        for error, reason in (
            (FileNotFoundError(), "process_gone"),
            (PermissionError(), "access_denied"),
            (psutil.NoSuchProcess(self.identity["pid"]), "process_gone"),
            (psutil.AccessDenied(self.identity["pid"]), "access_denied"),
        ):
            with (
                self.subTest(error=error),
                patch(
                    "core.resources.sampling.process_identity", side_effect=error
                ),
            ):
                sample = self.sampler._process_sample(self.target)
            self.assertTrue(
                all(value["value"] is None for value in sample["resources"].values())
            )
            self.assertEqual(
                sample["resources"]["process_cpu_percent"]["attributes"]["reason"],
                reason,
            )
        for field in ("host_id", "boot_id"):
            foreign = ResourceTarget.from_document(
                target({**self.identity, field: "other"})
            )
            with patch("core.resources.sampling.psutil.Process") as process:
                sample = self.sampler._process_sample(foreign)
                process.assert_not_called()
            self.assertEqual(
                sample["resources"]["process_cpu_percent"]["attributes"]["reason"],
                "different_host_or_boot",
            )

    def test_host_memory_semantics_and_first_cpu_value(self):
        """B: host CPU is normalized and memory used equals total minus available."""
        with (
            patch("core.resources.sampling.psutil.cpu_percent", return_value=25),
            patch(
                "core.resources.sampling.psutil.virtual_memory",
                return_value=SimpleNamespace(total=1024, available=256, percent=75),
            ),
            patch("core.resources.sampling.time.monotonic", side_effect=[10, 12]),
        ):
            first = self.sampler._host_sample({})
            second = self.sampler._host_sample({})
        self.assertIsNone(first["resources"]["host_cpu_percent"]["value"])
        self.assertEqual(second["resources"]["host_cpu_percent"]["value"], 25)
        self.assertEqual(second["resources"]["host_memory_used_bytes"]["value"], 768)
        self.assertEqual(second["resources"]["host_memory_percent"]["value"], 75)
        self.assertTrue(
            all(item["scope"] == "host" for item in second["resources"].values())
        )

    def test_failure_of_one_host_metric_preserves_the_other_measurements(self):
        """B: an unavailable CPU reading is not a measured zero or a RAM failure."""
        with patch(
            "core.resources.sampling.psutil.cpu_percent",
            side_effect=OSError("CPU unavailable"),
        ):
            sample = self.sampler._host_sample({})
        self.assertIsNone(sample["resources"]["host_cpu_percent"]["value"])
        self.assertGreater(sample["resources"]["host_memory_total_bytes"]["value"], 0)

    @unittest.skipUnless(os.name == "nt", "Native Windows measurement")
    def test_windows_working_set_and_exact_identity_are_available(self):
        """B: Windows measurements carry working-set bytes and native creation identity."""
        sample = self.sampler._process_sample(self.target)
        memory = sample["resources"]["process_memory_rss_bytes"]
        self.assertGreater(memory["value"], 0)
        self.assertEqual(memory["unit"], "byte")
        self.assertEqual(
            memory["attributes"]["observed_process"], process_identity(os.getpid())
        )

    @unittest.skipUnless(
        os.name == "posix" and Path("/proc").is_dir(), "Native Linux measurement"
    )
    def test_linux_rss_and_proc_creation_ticks_are_available(self):
        """B: Linux RSS is paired with the unrounded /proc start tick."""
        sample = self.sampler._process_sample(self.target)
        memory = sample["resources"]["process_memory_rss_bytes"]
        ticks = int(
            Path(f"/proc/{os.getpid()}/stat").read_text().rsplit(")", 1)[1].split()[19]
        )
        self.assertGreater(memory["value"], 0)
        self.assertEqual(
            memory["attributes"]["observed_process"]["created_at_os"], ticks
        )


class RealSamplingTests(unittest.IsolatedAsyncioTestCase):
    async def test_busy_process_and_touched_64_mib_produce_observable_growth(self):
        """B: a real workload reports CPU>0 and RSS growth of at least 32 MiB."""
        async with asyncio.timeout(30):
            process, pipe = spawned(memory_and_cpu_process)
            self.addCleanup(stop_process, process, pipe)
            identity = await asyncio.to_thread(pipe.recv)
            sampler = ResourceSampler("real-test")
            observed = ResourceTarget.from_document(target(identity))
            baseline = sampler._process_sample(observed)
            self.assertIsNone(baseline["resources"]["process_cpu_percent"]["value"])
            memory_before = baseline["resources"]["process_memory_rss_bytes"]["value"]
            while memory_before is None:
                await asyncio.sleep(0.01)
                baseline = sampler._process_sample(observed)
                memory_before = baseline["resources"]["process_memory_rss_bytes"]["value"]
            pipe.send("allocate")
            self.assertEqual(await asyncio.to_thread(pipe.recv), "allocated")
            latest = {}

            def grew():
                latest.update(sampler._process_sample(observed))
                resources = latest["resources"]
                cpu = resources["process_cpu_percent"]["value"]
                memory = resources["process_memory_rss_bytes"]["value"]
                return (
                    cpu is not None
                    and memory is not None
                    and cpu > 0
                    and memory >= memory_before + 32 * 1024 * 1024
                )

            await wait_until(grew)
            self.assertEqual(
                latest["resources"]["process_cpu_percent"]["attributes"][
                    "observed_process"
                ],
                identity,
            )
            pipe.send("stop")
            await asyncio.to_thread(process.join, 5)
            self.assertEqual(process.exitcode, 0)
