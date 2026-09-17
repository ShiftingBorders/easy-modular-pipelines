"""Dashboard plan B: disk/network readings and intentionally inactive GPU support."""

import socket
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from core.resource_utils.hardware import HardwareSampler
from core.resource_utils.state import CollectorSettings
from tests.helpers.resources import DEFAULT_CONFIG


class HardwareTests(unittest.TestCase):
    def setUp(self):
        self.sampler = HardwareSampler(
            replace(
                CollectorSettings.load(DEFAULT_CONFIG),
                disk_path="X:/data",
                network_interface="internet",
            )
        )
        self.disk = patch(
            "core.resource_utils.hardware.psutil.disk_usage",
            return_value=SimpleNamespace(total=1000, used=600, free=400, percent=60),
        )
        self.disk.start()
        self.addCleanup(self.disk.stop)

    def test_first_network_interval_is_unknown_then_converts_bytes_to_mbps(self):
        counters = [
            {"internet": SimpleNamespace(bytes_recv=10, bytes_sent=20)},
            {"internet": SimpleNamespace(bytes_recv=2000010, bytes_sent=1000020)},
        ]
        with (
            patch("core.resource_utils.hardware.time.monotonic", side_effect=[10, 12]),
            patch(
                "core.resource_utils.hardware.psutil.net_io_counters",
                side_effect=counters,
            ),
            patch.object(
                self.sampler,
                "_sample_gpu",
                side_effect=AssertionError("unfinished GPU called"),
            ),
        ):
            first = self.sampler.sample()
            second = self.sampler.sample()
        self.assertIsNone(first["internet_receive_mbps"]["value"])
        self.assertEqual(second["internet_receive_mbps"]["value"], 8)
        self.assertEqual(second["internet_transmit_mbps"]["value"], 4)
        self.assertEqual(second["host_disk_free_bytes"]["value"], 400)
        self.assertEqual(second["host_disk_percent"]["value"], 60)
        self.assertEqual(
            second["internet_receive_mbps"]["attributes"]["interval_seconds"], 2
        )
        self.assertFalse(any("gpu" in name or "vram" in name for name in second))

    def test_reset_changed_interface_and_zero_interval_do_not_invent_throughput(self):
        for second_name, second_time, count in (
            ("internet", 10, 100),
            ("internet", 11, 1),
            ("other", 11, 200),
        ):
            with self.subTest(interface=second_name, time=second_time, count=count):
                sampler = HardwareSampler(self.sampler.settings)
                with (
                    patch(
                        "core.resource_utils.hardware.time.monotonic",
                        side_effect=[10, second_time],
                    ),
                    patch(
                        "core.resource_utils.hardware.psutil.net_io_counters",
                        side_effect=[
                            {
                                "internet": SimpleNamespace(
                                    bytes_recv=100, bytes_sent=100
                                )
                            },
                            {
                                second_name: SimpleNamespace(
                                    bytes_recv=count, bytes_sent=count
                                )
                            },
                        ],
                    ),
                ):
                    sampler.sample()
                    sampler.settings = replace(
                        sampler.settings, network_interface=second_name
                    )
                    self.assertIsNone(
                        sampler.sample()["internet_receive_mbps"]["value"]
                    )

    def test_disk_and_interface_failure_are_separate_unknown_measurements(self):
        with (
            patch(
                "core.resource_utils.hardware.psutil.disk_usage",
                side_effect=OSError("disk gone"),
            ),
            patch(
                "core.resource_utils.hardware.psutil.net_io_counters", return_value={}
            ),
        ):
            values = self.sampler.sample()
        self.assertIsNone(values["host_disk_free_bytes"]["value"])
        self.assertIn("disk gone", values["host_disk_free_bytes"]["reason"])
        self.assertIsNone(values["internet_receive_mbps"]["value"])

    def test_route_selection_uses_local_socket_address_without_sending_payload(self):
        self.sampler.settings = replace(self.sampler.settings, network_interface=None)
        with (
            patch("core.resource_utils.hardware.socket.socket") as factory,
            patch(
                "core.resource_utils.hardware.psutil.net_if_addrs",
                return_value={
                    "internet": [
                        SimpleNamespace(family=socket.AF_INET, address="192.0.2.1")
                    ]
                },
            ),
            patch(
                "core.resource_utils.hardware.psutil.net_io_counters",
                return_value={"internet": SimpleNamespace(bytes_recv=0, bytes_sent=0)},
            ),
        ):
            connection = factory.return_value.__enter__.return_value
            connection.getsockname.return_value = ("192.0.2.1", 12345)
            values = self.sampler.sample()
            connection.connect.assert_called_once_with(("1.1.1.1", 53))
            connection.send.assert_not_called()
            connection.sendto.assert_not_called()
        self.assertEqual(
            values["internet_receive_mbps"]["attributes"]["interface"], "internet"
        )
