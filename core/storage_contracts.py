"""Compatibility imports; use the responsibility packages for new code."""

from core.storage.contracts import HashDatabase, ModuleAddResult, ModuleDatabase

__all__ = [
    "HashDatabase",
    "ModuleAddResult",
    "ModuleDatabase",
]
