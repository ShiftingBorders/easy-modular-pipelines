"""Compatibility imports; use the responsibility packages for new code."""

from core.primitives.json_files import load_json
from core.primitives.json_values import type_match_nonempty

__all__ = [
    "load_json",
    "type_match_nonempty",
]
