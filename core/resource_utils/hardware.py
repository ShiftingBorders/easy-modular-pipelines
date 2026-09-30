"""Compatibility imports; use the responsibility packages for new code."""

from core.resources.hardware import HardwareSampler
from core.resources.state import CollectorSettings

__all__ = [
    "CollectorSettings",
    "HardwareSampler",
]
