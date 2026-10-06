"""Archive storage through SeaweedFS Filer, independent of server lifecycle."""

import tempfile
from collections.abc import Callable
from pathlib import Path
from urllib.parse import quote

import httpx

from core.models.storage_settings import FilerConfiguration
from core.storage.errors import (
    StorageAccessError,
    StorageCapacityError,
    StorageClosedError,
    StorageConfigurationError,
    StorageConflict,
    StorageError,
    StorageInputError,
    StorageIOError,
    StorageUnavailable,
    StoredObjectNotFound,
)
from core.storage.module_identity import check_input_metadata, clear_str


class SeaweedDB:
    """Store archives on an existing local or remote Filer server.

    Owns its HTTP client, never the server. The application calls close().
    before_upload optionally enforces a local disk policy and must raise
    StorageError on rejection. No server requests occur during construction.
    Writes are not atomic create-if-absent: serialize writes per module identity.
    A failed request can leave remote data; no automatic retries are performed.
    """

    def __init__(
        self,
        filer_url: str,
        *,
        max_archive_gb: float = 5,
        timeout: float = 60,
        before_upload: Callable[[int], None] | None = None,
    ) -> None:
        """Validate connection settings and create the owned HTTP client.

        Args:
            filer_url: HTTP(S) base URL of an existing Filer server.
            max_archive_gb: Maximum archive size in GB.
            timeout: HTTP request timeout in seconds.
            before_upload: Optional callback receiving archive bytes before upload.

        Raises:
            StorageConfigurationError: Settings or the upload callback are invalid.
        """
        try:
            settings = FilerConfiguration(
                filer_url=filer_url, max_archive_gb=max_archive_gb, timeout=timeout,
            )
        except (ValueError, TypeError) as error:
            raise StorageConfigurationError(str(error)) from error
        if before_upload is not None and not callable(before_upload):
            raise StorageConfigurationError("before_upload must be callable.")
        self.max_archive_gb = settings.max_archive_gb
        self._before_upload = before_upload
        self._client = httpx.Client(base_url=settings.filer_url, timeout=settings.timeout)

    def close(self) -> None:
        """Close this client's connections without stopping the Filer server."""
        try:
            self._client.close()
        except (httpx.HTTPError, OSError) as error:
            raise StorageIOError(
                "Failed to close the archive storage client."
            ) from error

    def _module_path(self, module_name: str, module_version: str) -> str:
        """Validate identity and client state before building a Filer path.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.

        Returns:
            Percent-encoded Filer path for a validated name/version after confirming
            the HTTP client is open.
        """
        module_name, module_version = clear_str(module_name, module_version)
        check_input_metadata(module_name, module_version)
        if module_name in {".", ".."} or module_version in {".", ".."}:
            raise StorageInputError("Module name and version cannot be dot segments.")
        if self._client.is_closed:
            raise StorageClosedError("The archive storage client is closed.")
        return (
            f"/modules/{quote(module_name, safe='')}/{quote(module_version, safe='')}"
        )

    def _check_response(self, response: httpx.Response) -> None:
        """Translate server responses into the shared semantic error contract.

        Args:
            response: HTTP response from Filer whose status is mapped to shared
                storage exceptions.
        """
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            status = response.status_code
            failures_by_status = {
                404: StoredObjectNotFound,
                409: StorageConflict,
                401: StorageAccessError,
                403: StorageAccessError,
                413: StorageCapacityError,
                507: StorageCapacityError,
                408: StorageUnavailable,
                429: StorageUnavailable,
                502: StorageUnavailable,
                503: StorageUnavailable,
                504: StorageUnavailable,
            }
            failure = failures_by_status.get(status, StorageError)
            raise failure(
                f"Filer rejected the operation with HTTP status {status}."
            ) from error

    def _validate_archive(self, archive: Path) -> tuple[Path, int]:
        """Validate an upload source and enforce the configured size limit.

        Args:
            archive: Archive file path used by this operation.

        Returns:
            Validated source Path and its size in bytes after applying the
            configured archive limit.

        Raises:
            StorageInputError: The upload source is not a valid file path.
            StorageIOError: Source metadata cannot be read.
            StorageCapacityError: Archive size exceeds the configured limit.
        """
        try:
            archive = Path(archive)
        except TypeError as error:
            raise StorageInputError(
                "Module archive must be a filesystem path."
            ) from error
        try:
            if not archive.is_file():
                raise StorageInputError(f"Module archive is not a file: {archive}")
            archive_size = archive.stat().st_size
        except OSError as error:
            raise StorageIOError(
                f"Failed to inspect module archive {archive}."
            ) from error
        if archive_size > self.max_archive_gb * 1024**3:
            raise StorageCapacityError(
                f"Module archive exceeds the {self.max_archive_gb} GB limit."
            )
        return archive, archive_size

    def check_module_stored(self, module_name: str, module_version: str) -> bool:
        """Return False only for a missing archive; service failures raise.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.

        Returns:
            False only for a missing archive; service failures raise.

        Raises:
            StorageUnavailable: Filer cannot be contacted.
            StorageError: A nonabsence HTTP or client failure prevents a reliable
                answer.
        """
        module_path = self._module_path(module_name, module_version)
        try:
            response = self._client.head(module_path)
        except httpx.TransportError as error:
            raise StorageUnavailable(
                "Cannot contact Filer to check the archive."
            ) from error
        except httpx.HTTPError as error:
            raise StorageError("Failed to check the archive.") from error
        if response.status_code == 404:
            return False
        self._check_response(response)
        return True

    def save_module(
        self,
        module_name: str,
        module_version: str,
        archive: Path,
    ) -> None:
        """Upload an archive; an observed existing identity raises StorageConflict.

        Checks for observed duplicates before upload but does not provide atomic
        create-if-absent across writers. A transport failure may occur after the
        remote write committed; no automatic retry is performed.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            archive: Archive file path used by this operation.

        Raises:
            StorageConflict: An archive with this name/version was observed before
                upload.
            StorageCapacityError: Archive limits or the before-upload reserve check
                reject the file.
            StorageError: Local I/O or remote transport/status prevents completing
                the upload.
        """
        if self.check_module_stored(module_name, module_version):
            raise StorageConflict(
                f"Module {module_name} version {module_version} already exists."
            )
        archive, archive_size = self._validate_archive(archive)
        if self._before_upload is not None:
            self._before_upload(archive_size)
        module_path = self._module_path(module_name, module_version)
        try:
            with archive.open("rb") as archive_file:
                response = self._client.post(
                    module_path,
                    files={"file": (archive.name, archive_file)},
                )
            self._check_response(response)
        except httpx.TransportError as error:
            raise StorageUnavailable(
                "Archive upload failed; the remote write outcome may be unknown."
            ) from error
        except httpx.HTTPError as error:
            raise StorageError("Failed to upload the archive.") from error
        except OSError as error:
            raise StorageIOError(
                f"Cannot read the upload archive {archive}."
            ) from error

    def retrieve_module(
        self,
        module_name: str,
        module_version: str,
        archive_path: Path,
    ) -> bool:
        """Download atomically to a local file; return True after replacement.

        A missing stored archive raises StoredObjectNotFound. Local failures
        raise StorageIOError. Incomplete downloads do not replace an existing
        destination. A cleanup failure is attached to the primary exception.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.
            archive_path: Local download destination; its parent directory must
                already exist.

        Returns:
            True after a complete download atomically replaces the destination.
            Missing archives raise StoredObjectNotFound rather than returning False.

        Raises:
            StoredObjectNotFound: The remote archive does not exist.
            StorageInputError: The destination parent is unavailable or the path is
                invalid.
            StorageError: Download, local publication, or remote status fails.
        """
        module_path = self._module_path(module_name, module_version)
        try:
            archive_path = Path(archive_path).resolve()
        except TypeError as error:
            raise StorageInputError(
                "Archive destination must be a filesystem path."
            ) from error
        except OSError as error:
            raise StorageIOError("Cannot resolve the archive destination.") from error
        try:
            if not archive_path.parent.is_dir():
                raise StorageInputError(
                    f"Archive destination directory does not exist: {archive_path.parent}"
                )
        except OSError as error:
            raise StorageIOError("Cannot inspect the archive destination.") from error

        temporary_archive_path = None
        try:
            try:
                with self._client.stream("GET", module_path) as response:
                    self._check_response(response)
                    with tempfile.NamedTemporaryFile(
                        mode="wb",
                        dir=archive_path.parent,
                        prefix=f".{archive_path.name}.",
                        suffix=".part",
                        delete=False,
                    ) as archive_file:
                        temporary_archive_path = Path(archive_file.name)
                        for chunk in response.iter_bytes():
                            archive_file.write(chunk)
                temporary_archive_path.replace(archive_path)
            except httpx.TransportError as error:
                raise StorageUnavailable("Archive download was interrupted.") from error
            except httpx.HTTPError as error:
                raise StorageError("Failed to download the archive.") from error
            except OSError as error:
                raise StorageIOError(
                    f"Cannot write archive to {archive_path}."
                ) from error
        except BaseException as error:
            # Cleanup also runs on cancellation, without replacing the primary failure.
            if temporary_archive_path is not None:
                try:
                    temporary_archive_path.unlink(missing_ok=True)
                except OSError as cleanup_error:
                    error.add_note(
                        f"Cannot remove incomplete archive {temporary_archive_path}: "
                        f"{cleanup_error}"
                    )
            raise
        return True

    def delete_module(self, module_name: str, module_version: str) -> bool:
        """Return whether the server deleted an archive; 404 means already absent.

        Args:
            module_name: Registered module name.
            module_version: Registered module version.

        Returns:
            Whether the server deleted an archive; 404 means already absent.
        """
        module_path = self._module_path(module_name, module_version)
        try:
            response = self._client.delete(module_path)
        except httpx.TransportError as error:
            raise StorageUnavailable(
                "Archive deletion failed; the remote outcome may be unknown."
            ) from error
        except httpx.HTTPError as error:
            raise StorageError("Failed to delete the archive.") from error
        if response.status_code == 404:
            return False
        self._check_response(response)
        return True
