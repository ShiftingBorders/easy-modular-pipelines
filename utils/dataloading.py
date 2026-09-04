import json
from pathlib import Path
from typing import Any


def load_json(path: Path | str) -> Any:
    """Load and return a value from a UTF-8 JSON file."""
    if isinstance(path, str):
        path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"JSON file does not exist: {path}")
    with path.open("r", encoding="utf-8") as file:
        try:
            return json.load(file)
        except json.JSONDecodeError as error:
            raise ValueError(f"JSON file cannot be parsed: {path}") from error

def type_match_nonempty(val: Any, targer_type: Any) -> bool:
    if not isinstance(val, targer_type):
        return False
    if isinstance(val, str) and not val.strip():
        return False
    return True
