
import json
import math
import platform
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import quote

import httpx
from pydantic import ValidationError

from core.seaweed_utils.dataclasses import SeaWeedConfig
from core.seaweed_utils.seaweed_errors import (
    IncorrectVolumePath,
    SeaweedInputFailure,
    SeaweedReadError,
    SeaweedStartFailure,
    SeaweedStopFailure,
    SeaweedWriteError,
)
from core.seaweed_utils.seaweed_states import (
    DEFAULT_LAUNCH_ARGS,
    DEFAULT_PROCESS_ARGS,
    SeaweedState,
)
from core.seaweed_utils.utils import (
    check_input_metadata,
    clear_str,
    free_port_finder,
)


class SeaweedDB:
    """Run a local SeaweedFS process and store module archives through Filer."""

    def __init__(
        self,
        volume_path: Path,
        volume_min_gb: float,
        config_path: Path | None = None,
    ) -> None:
        """Validate storage and start a private SeaweedFS standalone process.

        Args:
            volume_path: Existing directory used for persistent SeaweedFS data.
            volume_min_gb: Minimum free disk space required before startup.
            config_path: Optional SeaweedFS JSON configuration path. The
                version-controlled default is used when omitted.

        Raises:
            IncorrectVolumePath: If `volume_path` is not an existing directory.
            SeaweedStartFailure: If disk space is insufficient or SeaweedFS
                cannot be started.
        """
        volume_path = Path(volume_path).resolve()
        self.config_path = Path(config_path).resolve() if config_path else None
        self._volume_path_exists(volume_path)
        self.volume_path = volume_path
        self.volume_min_gb = volume_min_gb
        self.state = SeaweedState.STOPPED
        self._process: subprocess.Popen | None = None
        self._client: httpx.Client | None = None
        self._process_output = None

        self.default_launch_args = DEFAULT_LAUNCH_ARGS
        self.default_process_args = DEFAULT_PROCESS_ARGS

        self._what_os()
        self._free_space_startstop()
        self._start_seaweed()

    def _free_space_startstop(self):
        if not self.enough_free_space():
            raise SeaweedStartFailure(
            "Not enough free space to start SeaweedFS: "
                f"{self.remaining_free_space()/1024**3:.2f} GB available, {self.volume_min_gb:.2f} GB required."
            )

    def _what_os(self) -> None:
        """Store whether this instance is running on Windows or Linux.

        Raises:
            SeaweedStartFailure: If the operating system is not supported.
        """
        operating_system = platform.system().lower()
        if operating_system not in {"windows", "linux"}:
            raise SeaweedStartFailure(
                f"SeaweedFS is not supported on this OS: {platform.system()}"
            )
        self.operating_system = operating_system

    def _load_config(self) -> SeaWeedConfig:
        """Load and validate process and SeaweedFS command arguments."""
        config_path = self.config_path
        if config_path is None:
            config_path = (
                Path(__file__).resolve().parent.parent
                / "default_settings"
                / "seaweed_args.json"
            )

        if not config_path.is_file() or config_path.suffix.lower() != ".json":
            raise SeaweedStartFailure(
                f"SeaweedFS config must be an existing JSON file: {config_path}"
            )

        try:
            with config_path.open("r", encoding="utf-8") as config_file:
                config = json.load(config_file)
        except (OSError, json.JSONDecodeError) as error:
            raise SeaweedStartFailure(
                f"Failed to load SeaweedFS config {config_path}: {error}"
            ) from error

        if not isinstance(config, dict):
            raise SeaweedStartFailure("SeaweedFS config must contain a JSON object.")
        try:
            seaweed_cfg = SeaWeedConfig(**config)
        except ValidationError as error:
            raise SeaweedStartFailure(
                f"Incorrect parameters in config JSON file: {error}"
            ) from error
        return seaweed_cfg

    def _start_seaweed(self) -> None:
        """Start this instance's SeaweedFS process with Filer enabled."""
        if self.state == SeaweedState.RUNNING:
            return
        config = self._load_config()
        self.start_stop_timeout = config.process_args.start_stop_sec
        self.max_archive_gb = config.process_args.max_archive_gb
        executable_name = "weed.exe" if self.operating_system == "windows" else "weed"
        weed_executable = Path(__file__).parent / "seaweedfs" / executable_name
        if not weed_executable.is_file():
            raise SeaweedStartFailure(
                f"SeaweedFS executable does not exist: {weed_executable}"
            )

        selected_ports = None
        selected_ports = free_port_finder(100,100,config.start_args.ip_bind,config.start_args.master_port,config.start_args.volume_port,config.start_args.filer_port)

        if selected_ports == ():
            raise SeaweedStartFailure(
                "Could not find free ports for SeaweedFS after 100 attempts."
            )

        master_port, volume_port, filer_port = selected_ports
        command = [
            str(weed_executable),
            "server",
            f"-dir={self.volume_path}",
            f"-master.port={master_port}",
            f"-volume.port={volume_port}",
            f"-filer.port={filer_port}",
        ]
        for argument, value in config.start_args.model_dump(by_alias=True).items():
            if argument not in {"master.port", "volume.port", "filer.port"}:
                if isinstance(value, bool):
                    value = str(value).lower()
                command.append(f"-{argument}={value}")

        # The handle must remain open for the complete child-process lifetime.
        process_output = tempfile.TemporaryFile()  # noqa: SIM115
        try:
            self._process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=process_output,
                stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except OSError as error:
            process_output.close()
            self._process = None
            raise SeaweedStartFailure(
                f"Failed to start SeaweedFS: {error}"
            ) from error
        self._process_output = process_output

        start_result, err = self._post_start_check(
            master_port,volume_port,config.start_args.ip_bind,filer_port
        )
        if start_result == False:
            self._emergency_stop_debug(err)

    def _emergency_stop_debug(self, err: Exception | None) -> None:
        self._process_output.flush()
        self._process_output.seek(0)
        diagnostic_output = self._process_output.read().decode(
            "utf-8",
            errors="replace",
        )
        self._stop_seaweed()
        diagnostic_suffix = ""
        if diagnostic_output.strip():
            diagnostic_suffix = f" SeaweedFS output:\n{diagnostic_output.strip()}"
        if err is None:
            err = SeaweedStartFailure("SeaweedFS failed without an error.")
        raise SeaweedStartFailure(f"{err}{diagnostic_suffix}") from err


    def _post_start_check(self, 
        master_port: int,
        volume_port: int,
        binded_ip: str,
        filer_port: int,
    ) -> tuple[bool, Exception | None]:
        try:
            self.master_port = master_port
            self.volume_port = volume_port
            self.filer_port = filer_port
            client_host = binded_ip
            if client_host == "0.0.0.0":
                client_host = "127.0.0.1"
            self._client = httpx.Client(
                base_url=f"http://{client_host}:{filer_port}",
                timeout=self.start_stop_timeout,
            )

            for _ in range(self.start_stop_timeout):
                if self._process.poll() is not None:
                    break
                if self._check_availability():
                    return True, None
                time.sleep(1)
        except (httpx.HTTPError, OSError, ValueError) as error:
            return False, error
        return False, SeaweedStartFailure("Failed to start Seaweed after specified time")

    def _stop_seaweed(self) -> None:
        """Stop only the SeaweedFS process owned by this instance."""
        if self._client is not None:
            self._client.close()
            self._client = None

        if self._process is None or self._process.poll() is not None:
            self._process = None
            self.state = SeaweedState.STOPPED
            if self._process_output is not None:
                self._process_output.close()
                self._process_output = None
            return

        try:
            self._process.terminate()
            try:
                self._process.wait(timeout=self.start_stop_timeout)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
        except (OSError, subprocess.SubprocessError) as error:
            raise SeaweedStopFailure(
                f"Failed to stop SeaweedFS: {error}"
            ) from error
        finally:
            self._process = None
            self.state = SeaweedState.STOPPED
            if self._process_output is not None:
                self._process_output.close()
                self._process_output = None

    def _volume_path_exists(self, volume_path: Path) -> None:
        """Ensure that the configured volume path is an existing directory."""
        if not volume_path.is_dir():
            raise IncorrectVolumePath(
                f"SeaweedFS volume path is not an existing directory: {volume_path}"
            )

    def _check_availability(self) -> bool:
        """Return whether this instance's process and Filer are available."""
        if self._process is None or self._process.poll() is not None:
            self.state = SeaweedState.STOPPED
            return False
        if self._client is None:
            self.state = SeaweedState.STOPPED
            return False

        try:
            response = self._client.get("/")
        except httpx.HTTPError:
            self.state = SeaweedState.STOPPED
            return False

        if response.status_code != httpx.codes.OK:
            self.state = SeaweedState.STOPPED
            return False
        self.state = SeaweedState.RUNNING
        return True

    def is_available(self) -> bool:
        """Return whether this instance's Filer is currently available."""
        return self._check_availability()

    def remaining_free_space(self) -> float:
        if (
            isinstance(self.volume_min_gb, bool)
            or not isinstance(self.volume_min_gb, (int, float))
            or not math.isfinite(self.volume_min_gb)
            or self.volume_min_gb < 0
        ):
            raise SeaweedStartFailure("volume_min_gb must be a non-negative number.")
        try:
            free_bytes = shutil.disk_usage(self.volume_path).free
        except OSError as error:
            raise SeaweedStartFailure(
                f"Failed to inspect free space at {self.volume_path}: {error}"
            ) from error
        return free_bytes

    def enough_free_space(self) -> bool:
        """Raise when the volume disk has less than the configured free space."""
        free_bytes = self.remaining_free_space()
        required_bytes = self.volume_min_gb * 1024**3
        return not free_bytes < required_bytes

    def _module_path(self, module_name: str, module_version: str) -> str:
        """Validate module metadata and return its path in Filer."""
        module_name, module_version = clear_str(module_name, module_version)
        check_input_metadata(module_name, module_version)
        return (
            f"/modules/{quote(module_name, safe='')}/"
            f"{quote(module_version, safe='')}"
        )

    def _validate_archive(self, archive: Path) -> tuple[Path, int]:
        """Validate an upload archive and return its path and size."""
        try:
            archive = Path(archive)
        except TypeError as error:
            raise SeaweedInputFailure(
                "Module archive must be a filesystem path."
            ) from error
        if not archive.is_file():
            raise SeaweedWriteError(f"Module archive does not exist: {archive}")
        try:
            archive_size = archive.stat().st_size
        except OSError as error:
            raise SeaweedWriteError(
                f"Failed to inspect module archive {archive}: {error}"
            ) from error
        if archive_size > self.max_archive_gb * 1024**3:
            raise SeaweedWriteError(
                f"Module archive exceeds the {self.max_archive_gb} GB limit."
            )
        return archive, archive_size

    def _check_upload_space(self, archive_size: int) -> None:
        """Ensure an upload leaves the configured free-space reserve."""
        try:
            free_bytes = shutil.disk_usage(self.volume_path).free
        except OSError as error:
            raise SeaweedWriteError(
                f"Failed to inspect free space at {self.volume_path}: {error}"
            ) from error
        reserved_bytes = self.volume_min_gb * 1024**3
        if free_bytes - archive_size < reserved_bytes:
            raise SeaweedWriteError(
                "Not enough free space to save the module and preserve the "
                f"configured {self.volume_min_gb} GB reserve."
            )

    def check_module_stored(self, module_name: str, module_version: str) -> bool:
        """Return whether Filer contains the requested module and version."""
        module_path = self._module_path(module_name, module_version)
        if not self._check_availability() or self._client is None:
            raise SeaweedStartFailure("SeaweedFS is not running.")

        try:
            response = self._client.head(module_path)
        except httpx.HTTPError as error:
            raise SeaweedReadError(
                f"Failed to check module {module_name} version {module_version}."
            ) from error
        if response.status_code == httpx.codes.NOT_FOUND:
            return False
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            raise SeaweedReadError(
                "Filer rejected the module availability check with HTTP status "
                f"{response.status_code}."
            ) from error
        return True

    def save_module(
        self,
        module_name: str,
        module_version: str,
        archive: Path,
    ) -> None:
        """Upload an archive from a local path to `/modules/<name>/<version>`."""
        if self.check_module_stored(module_name, module_version):
            raise SeaweedWriteError(
                f"Module {module_name} version {module_version} already exists."
            )
        archive, archive_size = self._validate_archive(archive)
        self._check_upload_space(archive_size)
        module_path = self._module_path(module_name, module_version)
        try:
            with archive.open("rb") as archive_file:
                response = self._client.post(
                    module_path,
                    files={"file": (archive.name, archive_file)},
                )
            response.raise_for_status()
        except (OSError, httpx.HTTPError) as error:
            raise SeaweedWriteError(
                f"Failed to save module {module_name} version {module_version}."
            ) from error

    def retrieve_module(
        self,
        module_name: str,
        module_version: str,
        archive_path: Path,
    ) -> bool:
        """Download a module archive from Filer to a local file.

        Args:
            module_name: Stored module name.
            module_version: Stored module version.
            archive_path: Destination file path. Its parent must already exist.

        Returns:
            `True` after the complete archive has been written.

        Raises:
            SeaweedInputFailure: If module metadata is invalid.
            SeaweedStartFailure: If this instance's Filer is unavailable.
            SeaweedReadError: If the module is absent, Filer cannot provide it,
                or the destination cannot be written.
        """
        module_path = self._module_path(module_name, module_version)
        if not self._check_availability() or self._client is None:
            raise SeaweedStartFailure("SeaweedFS is not running.")
        
        try:
            archive_path = Path(archive_path).resolve()
        except (OSError, TypeError) as error:
            raise SeaweedInputFailure(
                "Module archive destination must be a filesystem path."
            ) from error
        if not archive_path.parent.is_dir():
            raise SeaweedReadError(
                "Module archive destination directory does not exist: "
                f"{archive_path.parent}"
            )

        temporary_archive_path = None
        try:
            with self._client.stream("GET", module_path) as response:
                if response.status_code == httpx.codes.NOT_FOUND:
                    raise SeaweedReadError(
                        f"Module {module_name} version {module_version} does not exist."
                    )
                response.raise_for_status()
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
            temporary_archive_path = None
        except SeaweedReadError:
            raise
        except (httpx.HTTPError, OSError) as error:
            raise SeaweedReadError(
                f"Failed to retrieve module {module_name} version {module_version}."
            ) from error
        finally:
            if temporary_archive_path is not None:
                try:
                    temporary_archive_path.unlink(missing_ok=True)
                except OSError as error:
                    raise SeaweedReadError(
                        "Failed to retrieve the module and remove its incomplete "
                        f"temporary archive {temporary_archive_path}."
                    ) from error

        return True

    def delete_module(self, module_name: str, module_version: str) -> bool:
        """Delete a stored module version through Filer.

        Returns:
            `True` when an existing module was deleted, or `False` when it was
            already absent.

        Raises:
            SeaweedInputFailure: If module metadata is invalid.
            SeaweedStartFailure: If this instance's Filer is unavailable.
            SeaweedWriteError: If Filer cannot delete the module.
        """
        if not self.check_module_stored(module_name, module_version):
            return False

        module_path = self._module_path(module_name, module_version)
        try:
            response = self._client.delete(module_path)
            response.raise_for_status()
        except httpx.HTTPError as error:
            raise SeaweedWriteError(
                f"Failed to delete module {module_name} version {module_version}."
            ) from error
        return True

    def stop_seaweed(self) -> None:
        """Stop this instance's SeaweedFS process."""
        self._stop_seaweed()

    def restart_seaweed(self, volume_path: Path | None = None) -> None:
        """Restart SeaweedFS, optionally using another existing data directory."""
        self._stop_seaweed()
        if volume_path is not None:
            volume_path = Path(volume_path).resolve()
            self._volume_path_exists(volume_path)
            self.volume_path = volume_path
        self._free_space_startstop()
        self._start_seaweed()
