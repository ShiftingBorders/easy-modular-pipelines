"""Dashboard entry point and explicit launch configuration."""

import contextlib
import io
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from dashboard.__main__ import main
from tests.dashboard_tests.helpers import (
    cleanup_directory,
    temporary_directory,
    write_settings,
)


class CommandLineTests(unittest.TestCase):
    def test_precache_cli_needs_no_http_and_propagates_worker_override(self):
        """T015/T046/T051/T095: documented precache flags do not import/start HTTP."""
        arguments = [
            "dashboard",
            "--config",
            str(self.config),
            "--mode",
            "precache",
            "--cache-workers",
            "3",
        ]
        with (
            patch.object(sys, "argv", arguments),
            patch.dict(sys.modules, {"uvicorn": None}),
            patch("dashboard.__main__.precache", return_value=0) as build,
            self.assertRaises(SystemExit) as caught,
        ):
            main()
        self.assertEqual(caught.exception.code, 0)
        self.assertEqual(build.call_args.args[0]["cache_workers"], 3)

    def test_precache_failure_and_interrupt_exit_codes(self):
        """T051: configuration/pool failures and interruption have distinct exits."""
        arguments = ["dashboard", "--config", str(self.config), "--mode", "precache"]
        for error, code in (
            (RuntimeError("pool failed"), 2),
            (KeyboardInterrupt(), 130),
        ):
            with (
                self.subTest(code=code),
                patch.object(sys, "argv", arguments),
                patch("dashboard.__main__.precache", side_effect=error),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as caught,
            ):
                main()
            self.assertEqual(caught.exception.code, code)

    def setUp(self) -> None:
        temporary = temporary_directory()
        self.addCleanup(cleanup_directory, temporary)
        self.directory = Path(temporary.name)
        self.config = write_settings(self.directory)
        self.server = SimpleNamespace(run=Mock())

    def test_passes_explicit_host_port_and_one_worker_to_server(self) -> None:
        arguments = [
            "dashboard",
            "--config",
            str(self.config),
            "--host",
            "127.0.0.1",
            "--port",
            "9002",
        ]
        with (
            patch.object(sys, "argv", arguments),
            patch.dict(sys.modules, {"uvicorn": self.server}),
        ):
            main()
        self.server.run.assert_called_once()
        self.assertEqual(self.server.run.call_args.kwargs["host"], "127.0.0.1")
        self.assertEqual(self.server.run.call_args.kwargs["port"], 9002)
        self.assertEqual(self.server.run.call_args.kwargs["workers"], 1)
        self.assertEqual(
            self.server.run.call_args.args[0].state.settings["state_directory"],
            self.directory / ".state",
        )
        self.assertFalse((self.directory / ".state").exists())

    def test_uses_configured_host_and_port_without_overrides(self) -> None:
        self.config = write_settings(self.directory, host="localhost", port=9123)
        with (
            patch.object(sys, "argv", ["dashboard", "--config", str(self.config)]),
            patch.dict(sys.modules, {"uvicorn": self.server}),
        ):
            main()
        self.assertEqual(self.server.run.call_args.kwargs["host"], "localhost")
        self.assertEqual(self.server.run.call_args.kwargs["port"], 9123)

    def test_invalid_port_does_not_start_server(self) -> None:
        for port in ["0", "65536", "-1", "text"]:
            with (
                self.subTest(port=port),
                patch.object(
                    sys,
                    "argv",
                    ["dashboard", "--config", str(self.config), "--port", port],
                ),
                patch.dict(sys.modules, {"uvicorn": self.server}),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                with self.assertRaises(SystemExit) as caught:
                    main()
                self.assertEqual(caught.exception.code, 2)
        self.server.run.assert_not_called()

    def test_missing_uvicorn_has_actionable_uv_command(self) -> None:
        output = io.StringIO()
        with (
            patch.object(sys, "argv", ["dashboard", "--config", str(self.config)]),
            patch.dict(sys.modules, {"uvicorn": None}),
            contextlib.redirect_stderr(output),
            self.assertRaises(SystemExit) as caught,
        ):
            main()
        self.assertEqual(caught.exception.code, 2)
        self.assertIn("uv run --with uvicorn", output.getvalue())

    def test_help_does_not_require_uvicorn_or_start_server(self) -> None:
        output = io.StringIO()
        with (
            patch.object(sys, "argv", ["dashboard", "--help"]),
            patch.dict(sys.modules, {"uvicorn": None}),
            contextlib.redirect_stdout(output),
            self.assertRaises(SystemExit) as caught,
        ):
            main()
        self.assertEqual(caught.exception.code, 0)
        self.assertIn("--config", output.getvalue())
