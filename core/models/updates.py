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

    Omitted defaults stay omitted and existing factory values are retained,
    including generated IDs in nested models. Changes replace top-level fields,
    rather than merging nested documents. Use JSON mode for input validators that
    require JSON representations, such as cursor positions. This operation has no
    persistence or locking effects; those remain the caller's responsibility.
    """
    snapshot = model.model_copy(deep=True)
    _include_factory_values(snapshot)
    values = snapshot.model_dump(
        mode=_dump_mode, exclude_unset=True, round_trip=True, by_alias=True
    )
    values.update(changes)
    replacement = type(model).model_validate(values)
    changed_fields = {
        name
        for name, field in type(model).model_fields.items()
        if name in changes
        or field.alias in changes
        or field.serialization_alias in changes
        or (
            isinstance(field.validation_alias, str)
            and field.validation_alias in changes
        )
    }
    _restore_factory_presence(model, replacement, changed_fields)
    return replacement


def _include_factory_values(value: object) -> None:
    """Mark factory fields on a private snapshot so dumping cannot regenerate them."""
    if isinstance(value, BaseModel):
        for name, field in type(value).model_fields.items():
            if field.default_factory is not None:
                value.model_fields_set.add(name)
            _include_factory_values(getattr(value, name))
    elif isinstance(value, dict):
        for item in value.values():
            _include_factory_values(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _include_factory_values(item)


def _restore_factory_presence(
    original: object, replacement: object, changed_fields: set[str] | None = None
) -> None:
    """Restore omission metadata on unchanged fields after candidate validation.

    The candidate values have already passed their validators. Only the presence
    metadata is adjusted; neither the original data nor its field set is mutated.
    Replaced top-level fields retain the new input's presence information.
    """
    if isinstance(original, BaseModel) and isinstance(replacement, BaseModel):
        if type(original) is not type(replacement):
            return
        for name, field in type(original).model_fields.items():
            if changed_fields is not None and name in changed_fields:
                continue
            if (
                field.default_factory is not None
                and name not in original.model_fields_set
            ):
                replacement.model_fields_set.discard(name)
            _restore_factory_presence(
                getattr(original, name), getattr(replacement, name)
            )
    elif isinstance(original, dict) and isinstance(replacement, dict):
        for key in original.keys() & replacement.keys():
            _restore_factory_presence(original[key], replacement[key])
    elif isinstance(original, (list, tuple)) and isinstance(replacement, (list, tuple)):
        for previous, current in zip(original, replacement):
            _restore_factory_presence(previous, current)
