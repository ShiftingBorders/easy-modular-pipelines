"""Compatibility imports; use the responsibility packages for new code."""

from core.journal.logger import OperationLogger
from core.journal.settings import load_logging_settings
from core.primitives.json_files import write_json
from core.primitives.json_values import JsonObject
from core.primitives.processes import process_identity
from core.resources.sampling import ResourceSampler, ResourceWriter, collect_resources
from core.resources.state import CollectorSettings, ResourceTarget

__all__ = [
    "CollectorSettings",
    "JsonObject",
    "OperationLogger",
    "ResourceSampler",
    "ResourceTarget",
    "ResourceWriter",
    "collect_resources",
    "load_logging_settings",
    "process_identity",
    "write_json",
]
