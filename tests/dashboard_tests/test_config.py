"""Configuration and import behavior from the approved dashboard plan."""

import importlib
import json
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from dashboard.config import load_settings
from dashboard.icmp import ICMPMonitor
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    settings_document,
    temporary_directory,
    write_settings,
)


class ConfigurationTests(unittest.TestCase):
    def test_project_paths_overrides_and_history_limits(self):
        path = write_settings(
            self.directory,
            project_root="project",
            history_max_events=12,
            history_max_bytes=4096,
            system_api_token_env="EMP_TEST_TOKEN",
        )
        config = load_settings(path)
        self.assertEqual(config["project_root"], self.directory / "project")
        self.assertEqual(config["history_max_events"], 12)
        absolute = str(self.directory / "other")
        self.assertEqual(
            load_settings(path, {"project_root": absolute})["project_root"],
            Path(absolute),
        )
        for options in (
            {"history_max_events": True},
            {"history_max_bytes": 0},
            {"project_root": []},
            {"system_api_token_env": "bad name"},
        ):
            with (
                self.subTest(options=options),
                self.assertRaises((TypeError, ValueError)),
            ):
                load_settings(path, options)

    def setUp(self) -> None:
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        self.directory = Path(temporary.name)

    def test_import_does_not_start_processes_or_create_runtime_files(self) -> None:
        import dashboard

        with (
            patch("asyncio.create_subprocess_exec") as launch,
            patch("pathlib.Path.mkdir") as mkdir,
        ):
            importlib.reload(dashboard)
        launch.assert_not_called()
        mkdir.assert_not_called()

    def test_defaults_do_not_connect_or_start_icmp(self) -> None:
        config = load_settings(write_settings(self.directory))
        self.assertIsNone(config["system_api_url"])
        monitor = ICMPMonitor(self.directory / "state")
        self.assertEqual(monitor.settings["host"], "www.google.com")
        self.assertFalse(monitor.settings["enabled"])
        self.assertEqual(monitor.snapshot()["status"], "disabled")
        self.assertFalse((self.directory / "state").exists())

    def test_relative_state_path_uses_config_directory_from_other_cwd(self) -> None:
        config_path = write_settings(self.directory, state_directory="nested/state")
        elsewhere = self.directory / "elsewhere"
        elsewhere.mkdir()
        previous = Path.cwd()
        try:
            os.chdir(elsewhere)
            config = load_settings(config_path)
        finally:
            os.chdir(previous)
        self.assertEqual(config["state_directory"], self.directory / "nested/state")

    def test_preserves_absolute_state_path(self) -> None:
        path = self.directory / "custom"
        self.assertEqual(
            load_settings(write_settings(self.directory, state_directory=str(path)))[
                "state_directory"
            ],
            path,
        )

    def test_rejects_relative_configuration_path(self) -> None:
        with self.assertRaisesRegex(ValueError, "absolute"):
            load_settings(Path("settings.json"))

    def test_supports_utf8_bom(self) -> None:
        path = write_settings(self.directory)
        path.write_text(json.dumps(settings_document()), encoding="utf-8-sig")
        self.assertEqual(load_settings(path)["port"], 8765)

    def test_rejects_corrupt_or_wrong_configuration_without_rewriting(self) -> None:
        path = self.directory / "settings.json"
        for content in [b"", b'{"host":', b"[]", b"null", b"\xff", b"{}"]:
            with self.subTest(content=content):
                path.write_bytes(content)
                with self.assertRaises((ValueError, UnicodeError)):
                    load_settings(path)
                self.assertEqual(path.read_bytes(), content)

    def test_requires_exact_settings_fields(self) -> None:
        path = write_settings(self.directory)
        for field in settings_document():
            document = settings_document()
            del document[field]
            with self.subTest(field=field):
                path.write_text(json.dumps(document), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_settings(path)
        path.write_text(json.dumps(settings_document(extra=True)), encoding="utf-8")
        with self.assertRaises(ValueError):
            load_settings(path)

    def test_numeric_boundaries(self) -> None:
        for field, low, high in [
            ("port", 1, 65535),
            ("max_response_bytes", 1024, 67108864),
            ("request_timeout_seconds", 0.1, 60),
            ("refresh_seconds", 1, 600),
        ]:
            for value in [low, high]:
                with self.subTest(field=field, value=value):
                    self.assertEqual(
                        load_settings(write_settings(self.directory, **{field: value}))[
                            field
                        ],
                        value,
                    )
            for value in [
                low - 1,
                high + 1,
                True,
                "5",
                None,
                float("nan"),
                float("inf"),
            ]:
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises((TypeError, ValueError)),
                ):
                    load_settings(write_settings(self.directory, **{field: value}))

    def test_rejects_empty_host_and_state_directory(self) -> None:
        for field in ["host", "state_directory"]:
            for value in ["", " ", None, 12]:
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValueError),
                ):
                    load_settings(write_settings(self.directory, **{field: value}))

    def test_normalizes_valid_system_base_url(self) -> None:
        for url in ["http://localhost:8000/api", "https://example.org/api/"]:
            with self.subTest(url=url):
                self.assertEqual(
                    load_settings(write_settings(self.directory, system_api_url=url))[
                        "system_api_url"
                    ],
                    url.rstrip("/") + "/",
                )

    def test_rejects_invalid_system_urls(self) -> None:
        for url in [
            12,
            "",
            "file:///tmp/db",
            "http://",
            "http://u:p@example.org",
            "http://example.org?q=1",
            "http://example.org#part",
            "http://example.org:bad",
            "http://example.org:99999",
            "http://example.org:0",
            "http://exa mple.org",
            "http://example.org/\napi",
        ]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                load_settings(write_settings(self.directory, system_api_url=url))
