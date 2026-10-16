"""Shared scalar constraints without changing established input conversions."""

from pathlib import Path
from typing import Annotated
from uuid import UUID

from pydantic import AfterValidator, BeforeValidator, Field, ValidationInfo

from core.primitives.json_values import require_number, require_text


def _text(value: object, info: ValidationInfo) -> str:
    return require_text(value, info.field_name or "value")


def _number(value: object, info: ValidationInfo) -> int | float:
    return require_number(value, info.field_name or "value")


def _absolute_path(value: object, info: ValidationInfo) -> Path:
    if not isinstance(value, (str, Path)):
        raise TypeError(f"{info.field_name} must be a string or Path.")
    path = Path(value)
    if not path.is_absolute() or "\x00" in str(path):
        raise ValueError(f"{info.field_name} must be an absolute filesystem path.")
    return path


def _uuid_text(value: object, info: ValidationInfo) -> str:
    text = require_text(value, info.field_name or "identifier")
    UUID(text)
    return text


def _boolean(value: object, info: ValidationInfo) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{info.field_name} must be a boolean.")
    return value


Text = Annotated[str, BeforeValidator(_text), Field(json_schema_extra={"minLength": 1})]
Number = Annotated[int | float, BeforeValidator(_number)]
PositiveNumber = Annotated[Number, Field(gt=0)]
PositiveInteger = Annotated[int, Field(strict=True, gt=0)]
NonnegativeInteger = Annotated[int, Field(strict=True, ge=0)]
SchemaVersionOne = Annotated[int, Field(strict=True, ge=1, le=1)]
AbsolutePath = Annotated[Path, BeforeValidator(_absolute_path)]
UUIDText = Annotated[
    str, BeforeValidator(_uuid_text), Field(json_schema_extra={"format": "uuid"})
]
NormalizedUUIDText = Annotated[UUIDText, AfterValidator(lambda value: str(UUID(value)))]
Boolean = Annotated[bool, BeforeValidator(_boolean)]
