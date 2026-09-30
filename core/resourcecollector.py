"""Compatibility imports; use the responsibility packages for new code."""

from core.primitives.json_values import JsonObject, copy_json_object
from core.resources.collector import ResourceCollector
from core.resources.sampling import collect_resources
from core.resources.state import CollectorSettings, ResourceHistory

__all__ = [
    "CollectorSettings",
    "JsonObject",
    "ResourceCollector",
    "ResourceHistory",
    "collect_resources",
    "copy_json_object",
]
