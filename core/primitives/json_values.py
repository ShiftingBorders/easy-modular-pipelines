"""JSON-compatible values and shared input validation."""

from __future__ import annotations

import json
import math
from typing import Any

type JsonValue = (
    None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
)


type JsonObject = dict[str, JsonValue]


def require_text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string.")
    if not value.strip() or "\x00" in value:
        raise ValueError(f"{name} must be nonempty and contain no null characters.")
    return value


def require_number(value: object, name: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, not a boolean.")
    if value < 0 or (isinstance(value, float) and not math.isfinite(value)):
        raise ValueError(f"{name} must be finite and nonnegative.")
    return value


def _validate_json(value: object, depth: int = 0) -> None:
    # A depth limit also rejects cycles without invoking user serialization hooks.
    if depth > 32:
        raise ValueError("JSON data must not be cyclic or nested more than 32 levels.")
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("JSON data must not contain NaN or infinity.")
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("JSON object keys must be strings.")
            _validate_json(item, depth + 1)
        return
    if type(value) is list:
        for item in value:
            _validate_json(item, depth + 1)
        return
    raise TypeError("Data must contain only JSON objects, arrays, and scalar values.")


def copy_json_object(value: object, name: str) -> JsonObject:
    """Validate and detach caller-owned JSON data before any persistent write."""
    if type(value) is not dict:
        raise TypeError(f"{name} must be a JSON object.")
    _validate_json(value)
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
    try:
        encoded.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError(f"{name} contains invalid Unicode.") from error
    return json.loads(encoded)


def type_match_nonempty(val: Any, targer_type: Any) -> bool:
    if not isinstance(val, targer_type):
        return False
    return not isinstance(val, str) or bool(val.strip())
