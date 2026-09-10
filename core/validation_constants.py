"""Character sets used by module and storage input validation."""

from string import ascii_letters, digits, hexdigits

HEX_DIGITS = frozenset(hexdigits)
MODULE_IDENTITY_CHARACTERS = ascii_letters + digits + "_-."
SQL_COLUMN_NAME_CHARACTERS = frozenset(ascii_letters + digits + "_")
SQL_COLUMN_TYPE_CHARACTERS = frozenset(ascii_letters + digits + "_ (),")
CONTROL_CHARACTERS = frozenset(chr(code) for code in range(32))
INVALID_MODULE_NAME_CHARACTERS = frozenset('<>:"/\\|?*') | CONTROL_CHARACTERS
INVALID_MODULE_VERSION_CHARACTERS = frozenset("/\\") | CONTROL_CHARACTERS
