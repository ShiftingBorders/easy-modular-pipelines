"""Template policy and static resource validation."""

from __future__ import annotations

from pathlib import Path

from core.primitives.json_values import (
    JsonObject,
    copy_json_object,
    require_number,
    require_text,
)


def _validate_template_policies(template: JsonObject) -> None:
    snapshots = copy_json_object(template["snapshots"], "snapshots")
    if snapshots.keys() != {"mode", "keep"}:
        raise ValueError("snapshots requires mode and keep.")
    if snapshots["mode"] not in ("off", "after_stage", "after_epoch"):
        raise ValueError("snapshots.mode must be off, after_stage, or after_epoch.")
    if type(snapshots["keep"]) is not int or snapshots["keep"] < 1:
        raise ValueError("snapshots.keep must be a positive integer.")
    storage = copy_json_object(template["storage"], "storage")
    if storage.keys() != {"min_snapshot_free_bytes"}:
        raise ValueError("storage requires min_snapshot_free_bytes.")
    if (
        type(storage["min_snapshot_free_bytes"]) is not int
        or storage["min_snapshot_free_bytes"] < 0
    ):
        raise ValueError("min_snapshot_free_bytes must be nonnegative.")
    logging = copy_json_object(template["logging"], "logging")
    if logging.keys() != {
        "busy_timeout_seconds",
        "max_event_bytes",
        "min_free_bytes",
        "filtered_refresh_interval_seconds",
    }:
        raise ValueError("All four configurable logging fields must be explicit.")
    for key in ("busy_timeout_seconds", "filtered_refresh_interval_seconds"):
        number = require_number(logging[key], key)
        if number <= 0:
            raise ValueError(f"logging.{key} must be positive.")
        if key == "busy_timeout_seconds" and number > 60:
            raise ValueError("logging.busy_timeout_seconds must not exceed 60.")
    for key in ("max_event_bytes", "min_free_bytes"):
        value = logging[key]
        if key == "max_event_bytes" and value is None:
            continue
        if type(value) is not int or value < (1 if key == "max_event_bytes" else 0):
            raise ValueError(f"Invalid logging.{key}.")
    unknown = copy_json_object(template["unknown_state"], "unknown_state")
    if unknown.keys() != {
        "timeout_seconds",
        "on_timeout",
        "recovery_limit",
        "on_recovery_limit",
    }:
        raise ValueError("All unknown_state fields must be explicit.")
    if require_number(unknown["timeout_seconds"], "unknown_state.timeout_seconds") <= 0:
        raise ValueError("unknown_state.timeout_seconds must be positive.")
    if unknown["on_timeout"] not in ("stop", "pause", "rerun", "skip"):
        raise ValueError("Invalid unknown_state.on_timeout.")
    if type(unknown["recovery_limit"]) is not int or unknown["recovery_limit"] < 0:
        raise ValueError("unknown_state.recovery_limit must be nonnegative.")
    if unknown["on_recovery_limit"] not in ("stop", "pause"):
        raise ValueError("Invalid unknown_state.on_recovery_limit.")


def _validate_template_resources(template: JsonObject, path: Path) -> None:
    if type(template["resources"]) is not list:
        raise TypeError("resources must be an array.")
    resource_names = set()
    for resource in template["resources"]:
        if type(resource) is not dict or resource.keys() != {
            "name",
            "path",
            "hash",
        }:
            raise ValueError("A resource requires name/path/hash.")
        name = require_text(resource["name"], "resource.name")
        if (
            name in resource_names
            or name in (".", "..")
            or any(c in name for c in '/\\:*?"<>|')
        ):
            raise ValueError("Resource names must be unique safe path components.")
        resource_names.add(name)
        configured = Path(require_text(resource["path"], "resource.path"))
        if not configured.is_absolute():
            if configured.drive or configured.root:
                raise ValueError("Ambiguous resource path.")
            configured = path.parent / configured
        # Store resolved paths in the normalized document; preserve original YAML.
        resource["path"] = str(configured)
        digest = resource["hash"]
        if digest is not None and (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(c not in "0123456789abcdefABCDEF" for c in digest)
        ):
            raise ValueError("resource.hash must be SHA-256 or null.")
