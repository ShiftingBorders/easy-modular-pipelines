"""Disk and selected network route telemetry.

GPU/VRAM sampling is unfinished. Its implementation is retained for later work,
but is deliberately disconnected from sample() and must not be used yet.
"""

from __future__ import annotations

import ctypes
import os
import socket
import time
from pathlib import Path

import psutil

from core.resource_utils.state import CollectorSettings


class _GPUMemory(ctypes.Structure):
    _fields_ = [
        ("total", ctypes.c_ulonglong),
        ("free", ctypes.c_ulonglong),
        ("used", ctypes.c_ulonglong),
    ]


class _GPUUsage(ctypes.Structure):
    _fields_ = [("gpu", ctypes.c_uint), ("memory", ctypes.c_uint)]


class HardwareSampler:
    def __init__(self, settings: CollectorSettings) -> None:
        self.settings = settings
        self._network_previous: tuple[str, float, int, int] | None = None
        self._gpu_at = 0.0
        self._gpu: dict = {}

    def sample(self) -> dict[str, dict]:
        result = {}
        disk_names = {
            "total": "byte",
            "used": "byte",
            "free": "byte",
            "percent": "percent",
        }
        try:
            if not self.settings.disk_path:
                raise ValueError("disk_not_configured")
            disk = psutil.disk_usage(self.settings.disk_path)
            for field, unit in disk_names.items():
                result[f"host_disk_{field}" + ("_bytes" if unit == "byte" else "")] = {
                    "value": getattr(disk, field),
                    "unit": unit,
                    "attributes": {"path": self.settings.disk_path},
                    "reason": None,
                }
        except (OSError, ValueError) as error:
            for field, unit in disk_names.items():
                result[f"host_disk_{field}" + ("_bytes" if unit == "byte" else "")] = {
                    "value": None,
                    "unit": unit,
                    "attributes": {"path": self.settings.disk_path},
                    "reason": str(error),
                }
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
        # GPU/VRAM support is unfinished; do not call _sample_gpu here.
        return result

    def _sample_gpu(self) -> dict[str, dict]:
        """Unfinished GPU/VRAM prototype; not part of active resource collection."""
        devices = []
        reason = "GPU telemetry is unavailable on this host."
        provider = "NVML"
        library = None
        initialized = False
        try:
            if os.name == "nt":
                candidates = [
                    Path(os.environ["SystemRoot"]) / "System32/nvml.dll",
                    Path(os.environ.get("ProgramFiles", "C:/Program Files"))
                    / "NVIDIA Corporation/NVSMI/nvml.dll",
                ]
                path = next((path for path in candidates if path.is_file()), None)
                if path is None:
                    raise OSError("NVIDIA NVML is unavailable.")
                library = ctypes.CDLL(str(path))
            else:
                library = ctypes.CDLL("libnvidia-ml.so.1")
            library.nvmlInit_v2.restype = ctypes.c_int
            library.nvmlShutdown.restype = ctypes.c_int
            if library.nvmlInit_v2() != 0:
                raise OSError("NVML initialization failed.")
            initialized = True
            count = ctypes.c_uint()
            library.nvmlDeviceGetCount_v2.argtypes = [ctypes.POINTER(ctypes.c_uint)]
            if library.nvmlDeviceGetCount_v2(ctypes.byref(count)) != 0:
                raise OSError("GPU enumeration failed.")
            library.nvmlDeviceGetHandleByIndex_v2.argtypes = [
                ctypes.c_uint,
                ctypes.POINTER(ctypes.c_void_p),
            ]
            library.nvmlDeviceGetMemoryInfo.argtypes = [
                ctypes.c_void_p,
                ctypes.POINTER(_GPUMemory),
            ]
            library.nvmlDeviceGetUtilizationRates.argtypes = [
                ctypes.c_void_p,
                ctypes.POINTER(_GPUUsage),
            ]
            library.nvmlDeviceGetName.argtypes = [
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_uint,
            ]
            library.nvmlDeviceGetUUID.argtypes = [
                ctypes.c_void_p,
                ctypes.c_void_p,
                ctypes.c_uint,
            ]
            for index in range(min(count.value, 64)):
                handle = ctypes.c_void_p()
                if library.nvmlDeviceGetHandleByIndex_v2(index, ctypes.byref(handle)):
                    continue
                memory, usage = _GPUMemory(), _GPUUsage()
                if library.nvmlDeviceGetMemoryInfo(handle, ctypes.byref(memory)):
                    continue
                name, identity = (
                    ctypes.create_string_buffer(128),
                    ctypes.create_string_buffer(128),
                )
                library.nvmlDeviceGetName(handle, name, len(name))
                library.nvmlDeviceGetUUID(handle, identity, len(identity))
                loaded = (
                    library.nvmlDeviceGetUtilizationRates(handle, ctypes.byref(usage))
                    == 0
                )
                devices.append(
                    {
                        "id": identity.value.decode(errors="replace") or str(index),
                        "name": name.value.decode(errors="replace") or "NVIDIA GPU",
                        "total": memory.total,
                        "used": memory.used,
                        "utilization": usage.gpu if loaded else None,
                    }
                )
        except (OSError, AttributeError) as error:
            reason = str(error)
        finally:
            if library is not None and initialized:
                library.nvmlShutdown()
        if not devices and os.name == "posix":
            provider = "sysfs"
            for path in sorted(Path("/sys/class/drm").glob("card[0-9]*/device")):
                try:
                    total = int((path / "mem_info_vram_total").read_text())
                    used = int((path / "mem_info_vram_used").read_text())
                    busy_path = path / "gpu_busy_percent"
                    devices.append(
                        {
                            "id": path.parent.name,
                            "name": "AMD GPU",
                            "total": total,
                            "used": used,
                            "utilization": float(busy_path.read_text())
                            if busy_path.exists()
                            else None,
                        }
                    )
                except (OSError, ValueError):
                    continue
        total = sum(device["total"] for device in devices) if devices else None
        used = sum(device["used"] for device in devices) if devices else None
        loads = [
            device["utilization"]
            for device in devices
            if device["utilization"] is not None
        ]
        values = {
            "host_vram_total_bytes": (total, "byte"),
            "host_vram_used_bytes": (used, "byte"),
            "host_vram_percent": (100 * used / total if total else None, "percent"),
            "host_gpu_percent": (max(loads) if loads else None, "percent"),
        }
        return {
            name: {
                "value": value,
                "unit": unit,
                "reason": None if value is not None else reason,
                "attributes": {"devices": devices, "provider": provider},
            }
            for name, (value, unit) in values.items()
        }
