"""Character sets used by module and storage input validation."""

from string import ascii_letters, digits, hexdigits

from core.storage.errors import StorageInputError

HEX_DIGITS = frozenset(hexdigits)
MODULE_IDENTITY_CHARACTERS = ascii_letters + digits + "_-."
CONTROL_CHARACTERS = frozenset(chr(code) for code in range(32))
INVALID_MODULE_NAME_CHARACTERS = frozenset('<>:"/\\|?*') | CONTROL_CHARACTERS
INVALID_MODULE_VERSION_CHARACTERS = frozenset("/\\") | CONTROL_CHARACTERS


ALLOWED_CHARACTERS = MODULE_IDENTITY_CHARACTERS


class ClearStringErr(Exception):
    """Legacy exception type reserved for string normalization failures."""


def clear_str(*args) -> tuple[str, ...]:
    cleared_args = []
    for arg in args:
        if not isinstance(arg, str):
            raise StorageInputError(f"Provided argument {arg} is not a string")
        cleared_args.append(arg.strip())
    return tuple(cleared_args)


def check_valid_characters(valid_characters: str, string_to_check: str) -> bool:
    return set(string_to_check).issubset(valid_characters)


def check_input_metadata(module_name: str, module_version: str):
    if module_name == "" or module_version == "":
        raise StorageInputError(
            f"Module name ({module_name}) or module version ({module_version}) are empty"
        )
    if not check_valid_characters(
        ALLOWED_CHARACTERS, module_name
    ) or not check_valid_characters(ALLOWED_CHARACTERS, module_version):
        raise StorageInputError(
            f"Module name ({module_name}) or module version ({module_version}) contain invalid characters"
        )


def clear_module_data_input(
    module_name: str,
    module_version: str,
    module_hash: str,
) -> tuple[str, str, str]:
    """Validate and normalize module identity and hash values."""
    module_data = (module_name, module_version, module_hash)
    if any(not isinstance(value, str) or not value.strip() for value in module_data):
        raise StorageInputError(
            "Can't register module with following values: "
            f"{module_name}, {module_version}, {module_hash}"
        )
    return (
        module_name.strip(),
        module_version.strip(),
        module_hash.strip(),
    )
