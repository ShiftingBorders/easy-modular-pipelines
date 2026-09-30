"""Disk and selected network route telemetry."""

from __future__ import annotations

import socket
import time

import psutil

from core.resources.state import CollectorSettings


class HardwareSampler:
    def __init__(self, settings: CollectorSettings) -> None:
        self.settings = settings
        self._network_previous: tuple[str, float, int, int] | None = None

    def sample(self) -> dict[str, dict]:
        result = {}
        try:
            if not self.settings.disk_path:
                raise ValueError("disk_not_configured")
            disk = psutil.disk_usage(self.settings.disk_path)
            result.update(self._disk_metrics(disk, None))
        except (OSError, ValueError) as error:
            result.update(self._disk_metrics(None, error))
        now = time.monotonic()
        interface = self.settings.network_interface
        reason = None
        receive = transmit = None
        interval = None
        try:
            if interface is None:
                # UDP connect selects a route without sending a packet.
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as route:
                    route.connect((self.settings.network_reference_address, 53))
                    address = route.getsockname()[0]
                interface = next(
                    (
                        name
                        for name, addresses in psutil.net_if_addrs().items()
                        if any(
                            item.family == socket.AF_INET and item.address == address
                            for item in addresses
                        )
                    ),
                    None,
                )
            counters = psutil.net_io_counters(pernic=True, nowrap=False)
            if interface not in counters:
                raise ValueError("Selected internet interface is unavailable.")
            current = counters[interface]
            previous = self._network_previous
            if previous is not None and previous[0] == interface:
                interval = now - previous[1]
                incoming, outgoing = (
                    current.bytes_recv - previous[2],
                    current.bytes_sent - previous[3],
                )
                if interval > 0 and incoming >= 0 and outgoing >= 0:
                    receive, transmit = (
                        incoming * 8 / interval / 1e6,
                        outgoing * 8 / interval / 1e6,
                    )
                else:
                    reason = "counter_reset"
            else:
                reason = "first_interval"
            self._network_previous = (
                interface,
                now,
                current.bytes_recv,
                current.bytes_sent,
            )
        except (OSError, ValueError, psutil.Error) as error:
            self._network_previous = None
            reason = str(error)
        for direction, value in (("receive", receive), ("transmit", transmit)):
            result[f"internet_{direction}_mbps"] = {
                "value": value,
                "unit": "Mbps",
                "reason": reason,
                "attributes": {
                    "interface": interface,
                    "interval_seconds": interval,
                    "route_reference": self.settings.network_reference_address,
                    "traffic_scope": "selected_interface",
                },
            }
        return result

    def _disk_metrics(
        self, disk: object, error: BaseException | None
    ) -> dict[str, dict]:
        disk_names = {
            "total": "byte",
            "used": "byte",
            "free": "byte",
            "percent": "percent",
        }
        result = {}
        for field, unit in disk_names.items():
            result[f"host_disk_{field}" + ("_bytes" if unit == "byte" else "")] = {
                "value": getattr(disk, field) if error is None else None,
                "unit": unit,
                "attributes": {"path": self.settings.disk_path},
                "reason": None if error is None else str(error),
            }
        return result

    def _sample_gpu(self) -> dict[str, dict]:
        """Return no observations for legacy callers; perform no device access."""
        return {}
