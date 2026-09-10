"""Explicit lifecycle and local disk policy for an owned SeaweedFS process."""

import json
import math
import platform
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import httpx
from pydantic import ValidationError

from core.storage_errors import (
    StorageCapacityError,
    StorageConfigurationError,
    StorageError,
    StorageIOError,
    StorageUnavailable,
)
from utils.seaweed_utils.dataclasses import SeaWeedConfig
from utils.seaweed_utils.seaweed_states import SeaweedState
from utils.seaweed_utils.utils import free_port_finder


class SeaweedProcess:
    """Own one local server. Construct, start explicitly, then stop in finally.

    After start(), filer_url and max_archive_gb configure a SeaweedDB client.
    Pass check_upload_space as that client's before_upload callback to retain
    the local disk reserve. This class does not store or retrieve archives.
    """

    def __init__(
        self,
        volume_path: Path,
        volume_min_gb: float,
        config_path: Path | None = None,
    ) -> None:
        """Configure a local server without starting a process or opening a client.

        Args:
            volume_path: Existing directory used for persistent SeaweedFS data.
            volume_min_gb: Minimum free disk space required before startup.
            config_path: Optional SeaweedFS JSON configuration path. The
                version-controlled default is used when omitted.

        Raises:
            StorageConfigurationError: If the configured paths are invalid.
        """
        try:
            volume_path = Path(volume_path).resolve()
            self.config_path = (
                Path(config_path).resolve()
                if config_path is not None
                else Path(__file__).resolve().parent.parent
                / "default_settings"
                / "seaweed_args.json"
            )
        except (OSError, TypeError, ValueError) as error:
            raise StorageConfigurationError(
                "Invalid SeaweedFS configuration path."
            ) from error
        self._volume_path_exists(volume_path)
        self.volume_path = volume_path
        self.volume_min_gb = volume_min_gb
        self.state = SeaweedState.STOPPED
        self._process: subprocess.Popen | None = None
        self._client: httpx.Client | None = None
        self._process_output = None

        self.filer_url: str | None = None

    def _free_space_startstop(self):
        if not self.enough_free_space():
            raise StorageCapacityError(
                "Not enough free space to start SeaweedFS: "
                f"{self.remaining_free_space() / 1024**3:.2f} GB available, {self.volume_min_gb:.2f} GB required."
            )

    def _what_os(self) -> None:
        """Store whether this instance is running on Windows or Linux.

        Raises:
            StorageConfigurationError: If the operating system is not supported.
        """
        operating_system = platform.system().lower()
        if operating_system not in {"windows", "linux"}:
            raise StorageConfigurationError(
                f"SeaweedFS is not supported on this OS: {platform.system()}"
            )
        self.operating_system = operating_system

    def _load_config(self) -> SeaWeedConfig:
        """Load and validate process and SeaweedFS command arguments."""
        config_path = self.config_path
        try:
            if not config_path.is_file() or config_path.suffix.lower() != ".json":
                raise StorageConfigurationError(
                    f"SeaweedFS config must be an existing JSON file: {config_path}"
                )
            with config_path.open("r", encoding="utf-8") as config_file:
                config = json.load(config_file)
        except (OSError, ValueError) as error:
            raise StorageConfigurationError(
                f"Failed to load SeaweedFS config {config_path}: {error}"
            ) from error

        if not isinstance(config, dict):
            raise StorageConfigurationError(
                "SeaweedFS config must contain a JSON object."
            )
        try:
            seaweed_cfg = SeaWeedConfig(**config)
        except ValidationError as error:
            raise StorageConfigurationError(
                f"Incorrect parameters in config JSON file: {error}"
            ) from error
        if "dir" in (seaweed_cfg.start_args.model_extra or {}):
            raise StorageConfigurationError(
                "Set the SeaweedFS data directory through volume_path, not start_args.dir."
            )
        return seaweed_cfg

    def start(self) -> None:
        """Start this instance's SeaweedFS process with Filer enabled."""
        if self._process is not None and self._process.poll() is None:
            if self.is_available():
                return
            raise StorageUnavailable(
                "The owned SeaweedFS process is alive but unavailable; restart it explicitly."
            )
        self.stop()
        self._what_os()
        self._free_space_startstop()
        config = self._load_config()
        self.start_stop_timeout = config.process_args.start_stop_sec
        self.max_archive_gb = config.process_args.max_archive_gb
        executable_name = "weed.exe" if self.operating_system == "windows" else "weed"
        weed_executable = Path(__file__).parent / "seaweedfs" / executable_name
        if not weed_executable.is_file():
            raise StorageConfigurationError(
                f"SeaweedFS executable does not exist: {weed_executable}"
            )

        selected_ports = free_port_finder(
            100,
            100,
            config.start_args.ip_bind,
            config.start_args.master_port,
            config.start_args.volume_port,
            config.start_args.filer_port,
        )

        if selected_ports == ():
            raise StorageUnavailable(
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
        try:
            process_output = tempfile.TemporaryFile()  # noqa: SIM115
        except OSError as error:
            raise StorageIOError(
                "Cannot create SeaweedFS process output file."
            ) from error
        try:
            self._process = subprocess.Popen(
                command,
                # Relative CLI paths belong to the file that configured them.
                cwd=self.config_path.parent,
                stdin=subprocess.DEVNULL,
                stdout=process_output,
                stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except OSError as error:
            process_output.close()
            self._process = None
            raise StorageUnavailable(f"Failed to start SeaweedFS: {error}") from error
        self._process_output = process_output

        try:
            start_result, err = self._post_start_check(
                master_port, volume_port, config.start_args.ip_bind, filer_port
            )
        except BaseException as error:
            try:
                self.stop()
            except StorageError as cleanup_error:
                error.add_note(
                    f"SeaweedFS startup cleanup also failed: {cleanup_error}"
                )
            raise
        if not start_result:
            self._emergency_stop_debug(err)

    def _emergency_stop_debug(self, err: Exception | None) -> None:
        failure = StorageUnavailable(
            "SeaweedFS did not become available after startup."
        )
        try:
            self._process_output.flush()
            self._process_output.seek(0)
            diagnostic_output = self._process_output.read().decode(
                "utf-8", errors="replace"
            )
            if diagnostic_output.strip():
                failure.add_note(f"SeaweedFS output:\n{diagnostic_output.strip()}")
        except OSError as output_error:
            failure.add_note(f"Cannot read process output: {output_error}")
        try:
            self.stop()
        except StorageError as cleanup_error:
            failure.add_note(f"SeaweedFS startup cleanup also failed: {cleanup_error}")
        raise failure from err

    def _post_start_check(
        self,
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
            elif client_host == "::":
                client_host = "::1"
            if ":" in client_host:
                client_host = f"[{client_host}]"
            self.filer_url = f"http://{client_host}:{filer_port}"
            self._client = httpx.Client(
                base_url=self.filer_url,
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
        return False, StorageUnavailable("Failed to start Seaweed after specified time")

    def stop(self) -> None:
        """Stop owned resources; retain a live process handle if termination fails."""
        cleanup_error = None
        if self._client is not None:
            try:
                self._client.close()
                self._client = None
            except (httpx.HTTPError, OSError) as error:
                cleanup_error = error

        if self._process is not None and self._process.poll() is None:
            try:
                self._process.terminate()
                try:
                    self._process.wait(timeout=self.start_stop_timeout)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                    self._process.wait(timeout=5)
            except (OSError, subprocess.SubprocessError) as error:
                failure = StorageError("Failed to stop the owned SeaweedFS process.")
                if cleanup_error is not None:
                    failure.add_note(
                        f"Closing the health client also failed: {cleanup_error}"
                    )
                raise failure from error

        self._process = None
        self.state = SeaweedState.STOPPED
        self.filer_url = None
        if self._process_output is not None:
            try:
                self._process_output.close()
                self._process_output = None
            except OSError as error:
                if cleanup_error is None:
                    cleanup_error = error
                else:
                    cleanup_error.add_note(
                        f"Closing process output also failed: {error}"
                    )
        if cleanup_error is not None:
            raise StorageIOError(
                "Failed to close SeaweedFS resources."
            ) from cleanup_error

    def _volume_path_exists(self, volume_path: Path) -> None:
        """Ensure that the configured volume path is an existing directory."""
        try:
            is_directory = volume_path.is_dir()
        except OSError as error:
            raise StorageConfigurationError(
                "Cannot inspect the SeaweedFS volume."
            ) from error
        if not is_directory:
            raise StorageConfigurationError(
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
            raise StorageConfigurationError(
                "volume_min_gb must be a non-negative number."
            )
        try:
            free_bytes = shutil.disk_usage(self.volume_path).free
        except OSError as error:
            raise StorageIOError(
                f"Failed to inspect free space at {self.volume_path}: {error}"
            ) from error
        return free_bytes

    def enough_free_space(self) -> bool:
        """Return whether the configured disk reserve is available."""
        free_bytes = self.remaining_free_space()
        required_bytes = self.volume_min_gb * 1024**3
        return not free_bytes < required_bytes

    def check_upload_space(self, archive_size: int) -> None:
        """Ensure an upload leaves the configured free-space reserve."""
        free_bytes = self.remaining_free_space()
        reserved_bytes = self.volume_min_gb * 1024**3
        if free_bytes - archive_size < reserved_bytes:
            raise StorageCapacityError(
                "Not enough free space to save the module and preserve the "
                f"configured {self.volume_min_gb} GB reserve."
            )

    def restart(self, volume_path: Path | None = None) -> None:
        """Restart the server; recreate archive clients for the new filer_url."""
        if volume_path is not None:
            try:
                volume_path = Path(volume_path).resolve()
            except (TypeError, OSError, ValueError) as error:
                raise StorageConfigurationError(
                    "Invalid SeaweedFS volume path."
                ) from error
            self._volume_path_exists(volume_path)
        self.stop()
        if volume_path is not None:
            self.volume_path = volume_path
        self.start()
