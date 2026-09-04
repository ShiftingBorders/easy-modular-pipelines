import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
from pydantic import ValidationError

from core.seaweed import SeaweedDB
from core.seaweed_utils.dataclasses import SeaWeedConfig
from core.seaweed_utils.seaweed_errors import (
    IncorrectVolumePath,
    SeaweedInputFailure,
    SeaweedReadError,
    SeaweedStartFailure,
    SeaweedStopFailure,
    SeaweedWriteError,
)
from core.seaweed_utils.seaweed_states import SeaweedState
from core.seaweed_utils.utils import free_port_finder

VALID_CONFIG = {
    "process_args": {"start_stop_sec": 2, "max_archive_gb": 5},
    "start_args": {
        "ip": "127.0.0.1",
        "ip.bind": "127.0.0.1",
        "master.port": 9333,
        "volume.port": 8080,
        "filer": True,
        "filer.port": 8888,
        "master.defaultReplication": "000",
        "master.telemetry": False,
    },
}


class StubResponse:
    """Provide the small httpx response surface used by SeaweedDB."""

    def __init__(self, status_code=200, chunks=(), iteration_error=None):
        self.status_code = status_code
        self._chunks = tuple(chunks)
        self._iteration_error = iteration_error

    def raise_for_status(self):
        if self.status_code < 400:
            return
        request = httpx.Request("GET", "http://filer/modules/test/1")
        response = httpx.Response(self.status_code, request=request)
        raise httpx.HTTPStatusError(
            f"HTTP {self.status_code}", request=request, response=response
        )

    def iter_bytes(self):
        yield from self._chunks
        if self._iteration_error is not None:
            raise self._iteration_error


class StubStream:
    """Expose a response as an HTTP streaming context manager."""

    def __init__(self, response):
        self.response = response

    def __enter__(self):
        return self.response

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class MemoryFiler:
    """Store Filer objects in memory for public-operation tests."""

    def __init__(self, objects=None):
        self.objects = objects if objects is not None else {}
        self.closed = False
        self.requests = []

    def get(self, path):
        self.requests.append(("GET", path))
        return StubResponse(200)

    def head(self, path):
        self.requests.append(("HEAD", path))
        return StubResponse(200 if path in self.objects else 404)

    def post(self, path, files):
        self.requests.append(("POST", path))
        self.objects[path] = files["file"][1].read()
        return StubResponse(201)

    def stream(self, method, path):
        self.requests.append((method, path))
        if path not in self.objects:
            return StubStream(StubResponse(404))
        return StubStream(StubResponse(200, (self.objects[path],)))

    def delete(self, path):
        self.requests.append(("DELETE", path))
        if path not in self.objects:
            return StubResponse(404)
        del self.objects[path]
        return StubResponse(204)

    def close(self):
        self.closed = True


class SeaweedTestCase(unittest.TestCase):
    """Provide isolated paths and minimally initialized SeaweedDB instances."""

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_directory.cleanup)
        self.temp_path = Path(self.temp_directory.name)

    def write_json(self, name, value):
        path = self.temp_path / name
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def new_db(self, client=None, objects=None):
        db = SeaweedDB.__new__(SeaweedDB)
        db.config_path = None
        db.volume_path = self.temp_path
        db.volume_min_gb = 0
        db.state = SeaweedState.RUNNING
        db.start_stop_timeout = 2
        db.max_archive_gb = 5
        db.operating_system = "windows"
        db.default_launch_args = {}
        db.default_process_args = {}
        db._process = MagicMock()
        db._process.poll.return_value = None
        db._client = client or MemoryFiler(objects)
        db._process_output = None
        return db


class SeaweedConfigurationTests(SeaweedTestCase):
    """Verify volume, configuration, OS, and startup command handling."""

    def test_rejects_missing_or_non_directory_volume(self):
        ordinary_file = self.temp_path / "volume.file"
        ordinary_file.write_bytes(b"data")

        for path in (self.temp_path / "missing", ordinary_file):
            with self.subTest(path=path), self.assertRaises(IncorrectVolumePath):
                SeaweedDB(path, 0)

    def test_free_space_accepts_boundary_and_rejects_shortage(self):
        db = self.new_db()
        one_gib = 1024**3
        with patch(
            "core.seaweed.shutil.disk_usage",
            return_value=SimpleNamespace(free=one_gib),
        ):
            db.volume_min_gb = 1
            self.assertTrue(db.enough_free_space())
            db.volume_min_gb = 1 + 1 / one_gib
            self.assertFalse(db.enough_free_space())
            with self.assertRaises(SeaweedStartFailure):
                db._free_space_startstop()

    def test_rejects_invalid_free_space_reserve(self):
        db = self.new_db()
        invalid_values = (-1, True, "1", float("nan"), float("inf"))

        for value in invalid_values:
            with self.subTest(value=value):
                db.volume_min_gb = value
                with self.assertRaises(SeaweedStartFailure):
                    db.remaining_free_space()

    def test_wraps_disk_inspection_error(self):
        db = self.new_db()
        disk_error = OSError("disk unavailable")
        with (
            patch("core.seaweed.shutil.disk_usage", side_effect=disk_error),
            self.assertRaises(SeaweedStartFailure) as raised,
        ):
            db.remaining_free_space()

        self.assertIs(raised.exception.__cause__, disk_error)

    def test_rejects_invalid_config_files(self):
        directory = self.temp_path / "config.json"
        directory.mkdir()
        text_file = self.temp_path / "config.txt"
        text_file.write_text("{}", encoding="utf-8")
        invalid_json = self.temp_path / "invalid.json"
        invalid_json.write_text("{", encoding="utf-8")
        list_json = self.write_json("list.json", [])

        for path in (
            self.temp_path / "missing.json",
            directory,
            text_file,
            invalid_json,
            list_json,
        ):
            with self.subTest(path=path):
                db = self.new_db()
                db.config_path = path
                with self.assertRaises(SeaweedStartFailure):
                    db._load_config()

    def test_rejects_invalid_config_parameters_with_validation_cause(self):
        invalid_configs = (
            {},
            {"process_args": {}, "start_args": VALID_CONFIG["start_args"]},
            {
                "process_args": {"start_stop_sec": 1, "max_archive_gb": 5},
                "start_args": VALID_CONFIG["start_args"],
            },
            {
                "process_args": VALID_CONFIG["process_args"],
                "start_args": {**VALID_CONFIG["start_args"], "filer": False},
            },
            {
                "process_args": VALID_CONFIG["process_args"],
                "start_args": {**VALID_CONFIG["start_args"], "master.port": 55536},
            },
        )

        for number, config in enumerate(invalid_configs):
            with self.subTest(config=config):
                db = self.new_db()
                db.config_path = self.write_json(f"invalid_{number}.json", config)
                with self.assertRaises(SeaweedStartFailure) as raised:
                    db._load_config()
                self.assertIsInstance(raised.exception.__cause__, ValidationError)

    def test_loads_custom_and_cwd_independent_default_config(self):
        custom_config = {
            **VALID_CONFIG,
            "process_args": {"start_stop_sec": 3, "max_archive_gb": 4},
        }
        db = self.new_db()
        db.config_path = self.write_json("custom.json", custom_config)
        self.assertEqual(db._load_config().process_args.start_stop_sec, 3)

        db.config_path = None
        original_cwd = Path.cwd()
        try:
            os.chdir(self.temp_path)
            loaded_default = db._load_config()
        finally:
            os.chdir(original_cwd)
        self.assertEqual(loaded_default.start_args.filer_port, 8888)

    def test_wraps_config_read_error(self):
        db = self.new_db()
        db.config_path = self.write_json("config.json", VALID_CONFIG)
        read_error = OSError("read failed")
        with (
            patch("pathlib.Path.open", side_effect=read_error),
            self.assertRaises(SeaweedStartFailure) as raised,
        ):
            db._load_config()
        self.assertIs(raised.exception.__cause__, read_error)

    def test_selects_supported_operating_system(self):
        db = self.new_db()
        for reported, expected in (("Windows", "windows"), ("Linux", "linux")):
            with self.subTest(reported=reported), patch(
                "core.seaweed.platform.system", return_value=reported
            ):
                db._what_os()
                self.assertEqual(db.operating_system, expected)

        with (
            patch("core.seaweed.platform.system", return_value="Darwin"),
            self.assertRaises(SeaweedStartFailure),
        ):
            db._what_os()

    def test_builds_process_command_from_validated_config(self):
        config_value = {
            **VALID_CONFIG,
            "start_args": {
                **VALID_CONFIG["start_args"],
                "master.telemetry": True,
                "extra.option": "value",
            },
        }
        config = SeaWeedConfig(**config_value)
        db = self.new_db()
        db.state = SeaweedState.STOPPED
        process = MagicMock()
        process.poll.return_value = None

        with (
            patch.object(db, "_load_config", return_value=config),
            patch("core.seaweed.Path.is_file", return_value=True),
            patch("core.seaweed.free_port_finder", return_value=(9433, 8180, 8988)),
            patch("core.seaweed.subprocess.Popen", return_value=process) as popen,
            patch.object(db, "_post_start_check", return_value=(True, None)),
        ):
            db._start_seaweed()

        command = popen.call_args.args[0]
        self.assertTrue(command[0].endswith("weed.exe"))
        self.assertIn("server", command)
        self.assertIn(f"-dir={self.temp_path}", command)
        self.assertIn("-master.port=9433", command)
        self.assertIn("-volume.port=8180", command)
        self.assertIn("-filer.port=8988", command)
        self.assertIn("-filer=true", command)
        self.assertIn("-master.telemetry=true", command)
        self.assertIn("-extra.option=value", command)
        db._process_output.close()

    def test_rejects_missing_binary_and_exhausted_ports(self):
        config = SeaWeedConfig(**VALID_CONFIG)
        db = self.new_db()
        db.state = SeaweedState.STOPPED
        with (
            patch.object(db, "_load_config", return_value=config),
            patch("core.seaweed.Path.is_file", return_value=False),
            patch("core.seaweed.subprocess.Popen") as popen,
            self.assertRaises(SeaweedStartFailure),
        ):
            db._start_seaweed()
        popen.assert_not_called()

        with (
            patch.object(db, "_load_config", return_value=config),
            patch("core.seaweed.Path.is_file", return_value=True),
            patch("core.seaweed.free_port_finder", return_value=()),
            patch("core.seaweed.subprocess.Popen") as popen,
            self.assertRaises(SeaweedStartFailure),
        ):
            db._start_seaweed()
        popen.assert_not_called()

    def test_wraps_process_creation_error(self):
        config = SeaWeedConfig(**VALID_CONFIG)
        db = self.new_db()
        db.state = SeaweedState.STOPPED
        start_error = OSError("cannot execute")
        process_output = MagicMock()
        with (
            patch.object(db, "_load_config", return_value=config),
            patch("core.seaweed.Path.is_file", return_value=True),
            patch("core.seaweed.free_port_finder", return_value=(9333, 8080, 8888)),
            patch("core.seaweed.tempfile.TemporaryFile", return_value=process_output),
            patch("core.seaweed.subprocess.Popen", side_effect=start_error),
            self.assertRaises(SeaweedStartFailure) as raised,
        ):
            db._start_seaweed()
        self.assertIs(raised.exception.__cause__, start_error)
        process_output.close.assert_called_once_with()
        self.assertIsNone(db._process)


class SeaweedPortAndAvailabilityTests(SeaweedTestCase):
    """Verify port grouping, startup readiness, and Filer state."""

    def socket_factory(self, occupied_ports, created):
        class Probe:
            def bind(self, address):
                if address[1] in occupied_ports:
                    raise OSError("occupied")

            def close(self):
                return None

        def create_socket(*args):
            probe = Probe()
            created.append(probe)
            return probe

        return create_socket

    def test_port_finder_uses_first_free_or_next_complete_group(self):
        created = []
        with patch(
            "core.seaweed_utils.utils.socket.socket",
            side_effect=self.socket_factory(set(), created),
        ):
            self.assertEqual(
                free_port_finder(3, 100, "127.0.0.1", 9333, 8080, 8888),
                (9333, 8080, 8888),
            )

        for occupied in ({9333}, {19333}):
            with self.subTest(occupied=occupied), patch(
                "core.seaweed_utils.utils.socket.socket",
                side_effect=self.socket_factory(occupied, []),
            ):
                self.assertEqual(
                    free_port_finder(3, 100, "127.0.0.1", 9333, 8080, 8888),
                    (9433, 8180, 8988),
                )

        with patch(
            "core.seaweed_utils.utils.socket.socket",
            side_effect=self.socket_factory(set(), []),
        ):
            self.assertEqual(
                free_port_finder(2, 100, "127.0.0.1", 1000, 11000, 2000),
                (),
            )

    def test_port_finder_returns_empty_after_attempts_or_overflow(self):
        with patch(
            "core.seaweed_utils.utils.socket.socket",
            side_effect=self.socket_factory({9333, 9433}, []),
        ):
            self.assertEqual(
                free_port_finder(2, 100, "127.0.0.1", 9333, 8080, 8888), ()
            )

        self.assertEqual(
            free_port_finder(2, 100, "127.0.0.1", 55536, 8080, 8888), ()
        )

    def test_availability_tracks_process_client_and_http_result(self):
        db = self.new_db()
        self.assertTrue(db.is_available())
        self.assertIs(db.state, SeaweedState.RUNNING)

        db._client = None
        self.assertFalse(db.is_available())
        self.assertIs(db.state, SeaweedState.STOPPED)

        for response in (StubResponse(503), httpx.ConnectError("offline")):
            with self.subTest(response=response):
                client = MagicMock()
                if isinstance(response, Exception):
                    client.get.side_effect = response
                else:
                    client.get.return_value = response
                db = self.new_db(client=client)
                self.assertFalse(db.is_available())
                self.assertIs(db.state, SeaweedState.STOPPED)

        db = self.new_db()
        db._process.poll.return_value = 1
        self.assertFalse(db.is_available())
        self.assertIs(db.state, SeaweedState.STOPPED)

    def test_post_start_check_uses_loopback_for_wildcard_bind(self):
        db = self.new_db()
        db._client = None
        created_client = MagicMock()
        with (
            patch("core.seaweed.httpx.Client", return_value=created_client) as client,
            patch.object(db, "_check_availability", return_value=True),
        ):
            result = db._post_start_check(9333, 8080, "0.0.0.0", 8888)

        self.assertEqual(result, (True, None))
        client.assert_called_once_with(base_url="http://127.0.0.1:8888", timeout=2)
        self.assertEqual((db.master_port, db.volume_port, db.filer_port), (9333, 8080, 8888))

    def test_post_start_check_reports_exit_timeout_and_client_error(self):
        db = self.new_db()
        db._process.poll.return_value = 1
        with patch("core.seaweed.httpx.Client"):
            ready, error = db._post_start_check(9333, 8080, "127.0.0.1", 8888)
        self.assertFalse(ready)
        self.assertIsInstance(error, SeaweedStartFailure)

        db = self.new_db()
        with (
            patch("core.seaweed.httpx.Client"),
            patch.object(db, "_check_availability", return_value=False),
            patch("core.seaweed.time.sleep"),
        ):
            ready, error = db._post_start_check(9333, 8080, "127.0.0.1", 8888)
        self.assertFalse(ready)
        self.assertIsInstance(error, SeaweedStartFailure)

        db = self.new_db()
        client_error = ValueError("bad URL")
        with patch("core.seaweed.httpx.Client", side_effect=client_error):
            self.assertEqual(
                db._post_start_check(9333, 8080, "127.0.0.1", 8888),
                (False, client_error),
            )

    def test_failed_start_stops_process_and_includes_diagnostics(self):
        db = self.new_db()
        cause = OSError("not ready")

        with tempfile.TemporaryFile() as output:
            output.write(b"start\xfffailed")
            db._process_output = output
            with self.assertRaises(SeaweedStartFailure) as raised:
                db._emergency_stop_debug(cause)

        self.assertIs(raised.exception.__cause__, cause)
        self.assertIn("start�failed", str(raised.exception))
        self.assertIsNone(db._process)
        self.assertIsNone(db._client)
        self.assertIsNone(db._process_output)
        self.assertIs(db.state, SeaweedState.STOPPED)


class SeaweedModuleOperationTests(SeaweedTestCase):
    """Verify archive validation and Filer CRUD behavior."""

    def test_save_check_retrieve_and_delete_archive(self):
        db = self.new_db()
        archive = self.temp_path / "module.bin"
        archive.write_bytes(b"module bytes")
        destination = self.temp_path / "retrieved.bin"

        with patch(
            "core.seaweed.shutil.disk_usage",
            return_value=SimpleNamespace(free=10 * 1024**3),
        ):
            self.assertIsNone(db.save_module("module", "1.0", archive))
        self.assertTrue(db.check_module_stored("module", "1.0"))
        self.assertTrue(db.retrieve_module("module", "1.0", destination))
        self.assertEqual(destination.read_bytes(), b"module bytes")
        self.assertTrue(db.delete_module("module", "1.0"))
        self.assertFalse(db.check_module_stored("module", "1.0"))
        self.assertFalse(db.delete_module("module", "1.0"))
        with self.assertRaises(SeaweedReadError):
            db.retrieve_module("module", "1.0", destination)

    def test_normalizes_names_and_keeps_versions_independent(self):
        db = self.new_db()
        first = self.temp_path / "first.bin"
        second = self.temp_path / "second.bin"
        first.write_bytes(b"first")
        second.write_bytes(b"second")

        with patch(
            "core.seaweed.shutil.disk_usage",
            return_value=SimpleNamespace(free=10 * 1024**3),
        ):
            db.save_module(" module ", " 1.0 ", first)
            db.save_module("module", "2.0", second)

        first_out = self.temp_path / "first.out"
        second_out = self.temp_path / "second.out"
        self.assertTrue(db.check_module_stored("module", "1.0"))
        db.retrieve_module(" module ", " 1.0 ", first_out)
        db.retrieve_module("module", "2.0", second_out)
        self.assertEqual(first_out.read_bytes(), b"first")
        self.assertEqual(second_out.read_bytes(), b"second")

    def test_rejects_invalid_metadata_before_filer_request(self):
        invalid_values = ("", "   ", None, 1, [], {}, "bad/name", "модуль", "a?b")
        operations = (
            lambda db, value: db.check_module_stored(value, "1.0"),
            lambda db, value: db.save_module(value, "1.0", self.temp_path / "a"),
            lambda db, value: db.retrieve_module(value, "1.0", self.temp_path / "out"),
            lambda db, value: db.delete_module(value, "1.0"),
        )

        for value in invalid_values:
            for operation in operations:
                with self.subTest(value=value, operation=operation):
                    client = MemoryFiler()
                    db = self.new_db(client=client)
                    with self.assertRaises(SeaweedInputFailure):
                        operation(db, value)
                    self.assertEqual(client.requests, [])

    def test_rejects_duplicate_without_overwriting(self):
        db = self.new_db()
        first = self.temp_path / "first.bin"
        second = self.temp_path / "second.bin"
        first.write_bytes(b"first")
        second.write_bytes(b"second")
        with patch(
            "core.seaweed.shutil.disk_usage",
            return_value=SimpleNamespace(free=10 * 1024**3),
        ):
            db.save_module("module", "1", first)
            with self.assertRaises(SeaweedWriteError):
                db.save_module("module", "1", second)

        destination = self.temp_path / "result.bin"
        db.retrieve_module("module", "1", destination)
        self.assertEqual(destination.read_bytes(), b"first")

    def test_validates_archive_path_and_size_boundaries(self):
        db = self.new_db()
        empty = self.temp_path / "empty.bin"
        empty.touch()
        db.max_archive_gb = 0
        self.assertEqual(db._validate_archive(empty), (empty, 0))

        one_byte = self.temp_path / "one.bin"
        one_byte.write_bytes(b"x")
        db.max_archive_gb = 1 / 1024**3
        self.assertEqual(db._validate_archive(one_byte), (one_byte, 1))
        db.max_archive_gb = 0
        with self.assertRaises(SeaweedWriteError):
            db._validate_archive(one_byte)

        for invalid in (None, self.temp_path / "missing", self.temp_path):
            with self.subTest(invalid=invalid):
                expected = SeaweedInputFailure if invalid is None else SeaweedWriteError
                with self.assertRaises(expected):
                    db._validate_archive(invalid)

    def test_checks_upload_reserve_boundary_and_errors(self):
        db = self.new_db()
        db.volume_min_gb = 1
        reserved = 1024**3
        with patch(
            "core.seaweed.shutil.disk_usage",
            return_value=SimpleNamespace(free=reserved + 10),
        ):
            db._check_upload_space(10)
            with self.assertRaises(SeaweedWriteError):
                db._check_upload_space(11)

        disk_error = OSError("disk failed")
        with (
            patch("core.seaweed.shutil.disk_usage", side_effect=disk_error),
            self.assertRaises(SeaweedWriteError) as raised,
        ):
            db._check_upload_space(1)
        self.assertIs(raised.exception.__cause__, disk_error)

    def test_operations_fail_without_running_filer(self):
        db = self.new_db()
        client = db._client
        db._process = None
        operations = (
            lambda: db.check_module_stored("module", "1"),
            lambda: db.save_module("module", "1", self.temp_path / "archive"),
            lambda: db.retrieve_module("module", "1", self.temp_path / "out"),
            lambda: db.delete_module("module", "1"),
        )
        for operation in operations:
            with self.subTest(operation=operation), self.assertRaises(
                SeaweedStartFailure
            ):
                operation()
        self.assertEqual(client.requests, [])

    def test_retrieve_validates_destination_before_stream(self):
        db = self.new_db()
        client = db._client
        for destination, error in (
            (None, SeaweedInputFailure),
            (self.temp_path / "missing" / "out.bin", SeaweedReadError),
        ):
            with self.subTest(destination=destination), self.assertRaises(error):
                db.retrieve_module("module", "1", destination)
        self.assertFalse(any(method == "GET" and path != "/" for method, path in client.requests))

    def test_retrieve_atomically_replaces_existing_destination(self):
        db = self.new_db(objects={"/modules/module/1": b"new bytes"})
        destination = self.temp_path / "archive.bin"
        destination.write_bytes(b"old bytes")

        self.assertTrue(db.retrieve_module("module", "1", destination))

        self.assertEqual(destination.read_bytes(), b"new bytes")
        self.assertEqual(list(self.temp_path.glob(".*.part")), [])

    def test_failed_retrieve_preserves_destination_and_removes_part(self):
        read_error = httpx.ReadError(
            "stream failed", request=httpx.Request("GET", "http://filer/module")
        )
        client = MagicMock()
        client.get.return_value = StubResponse(200)
        client.stream.return_value = StubStream(
            StubResponse(200, (b"partial",), read_error)
        )
        db = self.new_db(client=client)
        destination = self.temp_path / "archive.bin"
        destination.write_bytes(b"old bytes")

        with self.assertRaises(SeaweedReadError) as raised:
            db.retrieve_module("module", "1", destination)

        self.assertIs(raised.exception.__cause__, read_error)
        self.assertEqual(destination.read_bytes(), b"old bytes")
        self.assertEqual(list(self.temp_path.glob(".*.part")), [])

    def test_cleanup_error_is_wrapped(self):
        read_error = httpx.ReadError(
            "stream failed", request=httpx.Request("GET", "http://filer/module")
        )
        cleanup_error = OSError("cannot unlink")
        client = MagicMock()
        client.get.return_value = StubResponse(200)
        client.stream.return_value = StubStream(
            StubResponse(200, (b"partial",), read_error)
        )
        db = self.new_db(client=client)

        with (
            patch("pathlib.Path.unlink", side_effect=cleanup_error),
            self.assertRaises(SeaweedReadError) as raised,
        ):
            db.retrieve_module("module", "1", self.temp_path / "archive.bin")

        self.assertIs(raised.exception.__cause__, cleanup_error)
        self.assertIn("temporary archive", str(raised.exception))

    def test_failed_save_closes_source_archive(self):
        archive = self.temp_path / "archive.bin"
        archive.write_bytes(b"bytes")
        client = MagicMock()
        client.get.return_value = StubResponse(200)
        client.head.return_value = StubResponse(404)
        upload_error = httpx.WriteError(
            "upload failed", request=httpx.Request("POST", "http://filer/module")
        )
        observed_file = None

        def fail_upload(path, files):
            nonlocal observed_file
            observed_file = files["file"][1]
            raise upload_error

        client.post.side_effect = fail_upload
        db = self.new_db(client=client)
        with (
            patch(
                "core.seaweed.shutil.disk_usage",
                return_value=SimpleNamespace(free=10 * 1024**3),
            ),
            self.assertRaises(SeaweedWriteError) as raised,
        ):
            db.save_module("module", "1", archive)

        self.assertIs(raised.exception.__cause__, upload_error)
        self.assertTrue(observed_file.closed)

    def test_http_failures_are_converted_by_operation(self):
        request = httpx.Request("GET", "http://filer/module")

        client = MagicMock()
        client.get.return_value = StubResponse(200)
        client.head.return_value = StubResponse(500)
        db = self.new_db(client=client)
        with self.assertRaises(SeaweedReadError) as raised:
            db.check_module_stored("module", "1")
        self.assertIsInstance(raised.exception.__cause__, httpx.HTTPStatusError)

        client = MagicMock()
        client.get.return_value = StubResponse(200)
        client.stream.return_value = StubStream(StubResponse(500))
        db = self.new_db(client=client)
        with self.assertRaises(SeaweedReadError) as raised:
            db.retrieve_module("module", "1", self.temp_path / "out")
        self.assertIsInstance(raised.exception.__cause__, httpx.HTTPStatusError)

        archive = self.temp_path / "archive.bin"
        archive.write_bytes(b"bytes")
        client = MagicMock()
        client.get.return_value = StubResponse(200)
        client.head.return_value = StubResponse(404)
        client.post.return_value = StubResponse(500)
        db = self.new_db(client=client)
        with (
            patch(
                "core.seaweed.shutil.disk_usage",
                return_value=SimpleNamespace(free=10 * 1024**3),
            ),
            self.assertRaises(SeaweedWriteError) as raised,
        ):
            db.save_module("module", "1", archive)
        self.assertIsInstance(raised.exception.__cause__, httpx.HTTPStatusError)

        client = MagicMock()
        client.get.return_value = StubResponse(200)
        client.head.return_value = StubResponse(200)
        client.delete.return_value = StubResponse(500)
        db = self.new_db(client=client)
        with self.assertRaises(SeaweedWriteError) as raised:
            db.delete_module("module", "1")
        self.assertIsInstance(raised.exception.__cause__, httpx.HTTPStatusError)

        client = MagicMock()
        client.get.return_value = StubResponse(200)
        client.head.side_effect = httpx.ConnectError("offline", request=request)
        db = self.new_db(client=client)
        with self.assertRaises(SeaweedReadError) as raised:
            db.check_module_stored("module", "1")
        self.assertIsInstance(raised.exception.__cause__, httpx.ConnectError)

    def test_objects_remain_available_to_new_process_instance(self):
        objects = {}
        first_db = self.new_db(objects=objects)
        archive = self.temp_path / "archive.bin"
        archive.write_bytes(b"persistent")
        with patch(
            "core.seaweed.shutil.disk_usage",
            return_value=SimpleNamespace(free=10 * 1024**3),
        ):
            first_db.save_module("module", "1", archive)
        first_db.stop_seaweed()

        second_db = self.new_db(objects=objects)
        destination = self.temp_path / "restored.bin"
        self.assertTrue(second_db.check_module_stored("module", "1"))
        second_db.retrieve_module("module", "1", destination)
        self.assertEqual(destination.read_bytes(), b"persistent")


class SeaweedLifecycleTests(SeaweedTestCase):
    """Verify process shutdown and restart behavior."""

    def test_stop_is_repeatable_and_closes_resources(self):
        client = MagicMock()
        db = self.new_db(client=client)
        process = db._process
        output = MagicMock()
        db._process_output = output

        db.stop_seaweed()
        db.stop_seaweed()

        process.terminate.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=2)
        client.close.assert_called_once_with()
        output.close.assert_called_once_with()
        self.assertIsNone(db._process)
        self.assertIsNone(db._client)
        self.assertIsNone(db._process_output)
        self.assertIs(db.state, SeaweedState.STOPPED)

    def test_stop_kills_process_after_timeout(self):
        db = self.new_db()
        process = db._process
        process.wait.side_effect = (subprocess.TimeoutExpired("weed", 2), None)

        db.stop_seaweed()

        process.terminate.assert_called_once_with()
        process.kill.assert_called_once_with()
        self.assertEqual(
            process.wait.call_args_list[1].kwargs,
            {"timeout": 5},
        )
        self.assertIs(db.state, SeaweedState.STOPPED)

    def test_stop_wraps_process_error_and_still_cleans_state(self):
        db = self.new_db()
        process_error = OSError("terminate failed")
        db._process.terminate.side_effect = process_error
        output = MagicMock()
        db._process_output = output

        with self.assertRaises(SeaweedStopFailure) as raised:
            db.stop_seaweed()

        self.assertIs(raised.exception.__cause__, process_error)
        self.assertIsNone(db._process)
        self.assertIsNone(db._client)
        self.assertIsNone(db._process_output)
        self.assertIs(db.state, SeaweedState.STOPPED)
        output.close.assert_called_once_with()

    def test_restart_keeps_or_changes_volume_path(self):
        db = self.new_db()
        original_path = db.volume_path
        new_path = self.temp_path / "new-volume"
        new_path.mkdir()

        with (
            patch.object(db, "_stop_seaweed") as stop,
            patch.object(db, "_free_space_startstop") as check_space,
            patch.object(db, "_start_seaweed") as start,
        ):
            db.restart_seaweed()
            self.assertEqual(db.volume_path, original_path)
            db.restart_seaweed(new_path)
            self.assertEqual(db.volume_path, new_path.resolve())

        self.assertEqual(stop.call_count, 2)
        self.assertEqual(check_space.call_count, 2)
        self.assertEqual(start.call_count, 2)

    def test_restart_failure_leaves_old_process_stopped(self):
        db = self.new_db()
        missing = self.temp_path / "missing"
        with self.assertRaises(IncorrectVolumePath):
            db.restart_seaweed(missing)
        self.assertIsNone(db._process)
        self.assertIs(db.state, SeaweedState.STOPPED)

        db = self.new_db()
        with (
            patch.object(
                db,
                "_free_space_startstop",
                side_effect=SeaweedStartFailure("not enough space"),
            ),
            self.assertRaises(SeaweedStartFailure),
        ):
            db.restart_seaweed()
        self.assertIsNone(db._process)
        self.assertIs(db.state, SeaweedState.STOPPED)

        db = self.new_db()
        with (
            patch.object(db, "_free_space_startstop"),
            patch.object(
                db, "_start_seaweed", side_effect=SeaweedStartFailure("failed")
            ),
            self.assertRaises(SeaweedStartFailure),
        ):
            db.restart_seaweed()
        self.assertIsNone(db._process)
        self.assertIs(db.state, SeaweedState.STOPPED)


if __name__ == "__main__":
    unittest.main()
