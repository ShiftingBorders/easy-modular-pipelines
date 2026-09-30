"""Compatibility imports; use the responsibility packages for new code."""

from core.experiments.template import create_template
from core.primitives.json_values import require_text

__all__ = [
    "create_template",
    "require_text",
]
