"""Backend-independent storage operations used by ModuleManager.

Implementations raise categories from core.storage.errors for expected
failures. They must not report a service failure as an absent object.
Resource creation and shutdown belong to the application, not these protocols.
Existing return values are preserved pending a separate result-type review."""

from enum import Enum
from pathlib import Path
from typing import Protocol


class ModuleAddResult(Enum):
    """Result of inserting a module hash without replacing an existing identity."""
    module_added = True
    module_exists_err = False


class HashDatabase(Protocol):
    """Caller-owned storage contract for versioned module hash registration."""
    def list_module_hashes(self) -> list[dict[str, str]]:
        """Return registered name/version/hash references in name/version order."""
        ...

    def add_module_hash(
        self,
        module_name: str,
        module_version: str,
        module_hash: str,
    ) -> ModuleAddResult:
        """Insert without replacing; report a duplicate through ModuleAddResult.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            module_hash: Expected module content hash.

        Returns:
            ModuleAddResult.module_added for a new registration or module_exists_err
            when name/version is already present.
        """
        ...

    def get_module_hash(self, module_name: str, module_version: str) -> str:
        """Return the hash, or an empty string if the identity is absent.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.

        Returns:
            The hash, or an empty string if the identity is absent.
        """
        ...

    def remove_module_hash(self, module_name: str, module_version: str) -> bool:
        """Return whether a record was removed; absence is False.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.

        Returns:
            Whether a record was removed; absence is False.
        """
        ...


class ModuleDatabase(Protocol):
    """Caller-owned archive storage contract with explicit absence and failures."""
    def check_module_stored(self, module_name: str, module_version: str) -> bool:
        """Return whether an archive exists; failures raise StorageError.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.

        Returns:
            Whether an archive exists; failures raise StorageError.
        """
        ...

    def save_module(
        self,
        module_name: str,
        module_version: str,
        archive: Path,
    ) -> None:
        """Save an archive; an observed duplicate raises StorageConflict.

        Atomic create-if-absent is not currently guaranteed. Callers must
        serialize registrations for the same module identity.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            archive: Archive file path used by this operation.
        """
        ...

    def retrieve_module(
        self,
        module_name: str,
        module_version: str,
        archive_path: Path,
    ) -> bool:
        """Return True after saving locally; absence raises StoredObjectNotFound.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            archive_path: Local destination path where the retrieved archive is
                saved.

        Returns:
            True after saving locally; absence raises StoredObjectNotFound.
        """
        ...

    def delete_module(self, module_name: str, module_version: str) -> bool:
        """Return whether an archive was deleted; absence is False.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.

        Returns:
            Whether an archive was deleted; absence is False.
        """
        ...
