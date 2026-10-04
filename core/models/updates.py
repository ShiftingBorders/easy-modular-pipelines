"""Checked replacements for frozen models; owners publish only successful results."""

from typing import Literal

from pydantic import BaseModel


def _update_model[T: BaseModel](
    model: T,
    /,
    *,
    _dump_mode: Literal["python", "json"] = "python",
    **changes: object,
) -> T:
    """Validate a replacement without assigning to the original model.

    Omitted defaults stay omitted. Changes replace top-level input fields, rather
    than merging nested documents. Use JSON mode for models whose input validators
    require JSON representations, such as cursor positions. This operation has no
    persistence or locking effects; those remain the caller's responsibility.
    """
    values = model.model_dump(
        mode=_dump_mode, exclude_unset=True, round_trip=True, by_alias=True
    )
    values.update(changes)
    return type(model).model_validate(values)
