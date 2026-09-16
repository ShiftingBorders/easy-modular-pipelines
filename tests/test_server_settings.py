"""Approved A: real configuration files, cwd and side-effect-free imports."""

import json
import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import cli
from core.runner_utils.runtimeio import read_json, write_json
from core.serverruntime import DEFAULT_CONFIG, load_server_settings
from tests.helpers.dag import REPOSITORY


class ServerSettingsTests(unittest.TestCase):
    def setUp(self):
        parent = REPOSITORY / ".artifacts/tmp/server-settings"
        parent.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(self.cleanup)
        self.root = Path(self.temporary.name)
        self.config = self.root / "server.json"
        write_json(
            self.config, {"project_root": "project", "hash_config_path": "hashes.json"}
        )

    def cleanup(self):
        self.assertTrue(
            self.root.resolve().is_relative_to(
                REPOSITORY / ".artifacts/tmp/server-settings"
            )
        )
        deadline = time.monotonic() + 3
        while True:
            try:
                self.temporary.cleanup()
                return
            except OSError as error:
                # Windows scanners may briefly retain a deleted directory entry.
                if (
                    getattr(error, "winerror", None) not in (5, 32, 145)
                    or time.monotonic() >= deadline
                ):
                    raise
                time.sleep(0.025)

    def test_relative_paths_use_their_own_layer_and_ignore_caller_cwd(self):
        old = Path.cwd()
        try:
            os.chdir(self.root.parent)
            settings = load_server_settings(self.config)
        finally:
            os.chdir(old)
        self.assertEqual(settings.project_root, self.root / "project")
        self.assertEqual(settings.hash_config_path, self.root / "hashes.json")
        self.assertEqual(
            settings.resource_config_path,
            DEFAULT_CONFIG.with_name("resource_collector.json"),
        )
        alternate = self.root / "custom.json"
        write_json(
            alternate,
            {
                "project_root": str(self.root),
                "hash_config_path": "db/config.json",
                "resource_config_path": "collector.json",
            },
        )
        settings = load_server_settings(alternate)
        self.assertEqual(settings.project_root, self.root)
        self.assertEqual(settings.hash_config_path, self.root / "db/config.json")
        self.assertEqual(settings.resource_config_path, self.root / "collector.json")

    def test_invalid_fields_types_ranges_and_urls_are_rejected_before_runtime_io(self):
        defaults = read_json(DEFAULT_CONFIG)
        base = {
            "project_root": str(self.root / "project"),
            "hash_config_path": str(self.root / "hashes.json"),
        }
        cases = [
            {"unknown": 1},
            {"schema_version": 2},
            {"port": 0},
            {"port": 65536},
            {"filer_url": "file:///tmp"},
            {"filer_url": "http://user:secret@localhost"},
            {"token_env": ""},
            {"max_pending_commands": 10, "max_command_records": 10},
            {"max_response_bytes": 2048, "max_cached_result_bytes": 1024},
        ]
        for name in (
            key for key, value in defaults.items() if isinstance(value, (int, float))
        ):
            for value in (True, "1", -1, 0):
                cases.append({name: value})
        if os.name == "nt":
            cases.extend(
                ({"project_root": "C:relative"}, {"hash_config_path": "\\rooted"})
            )
        for index, changed in enumerate(cases):
            config = self.root / f"bad-{index}.json"
            write_json(config, {**base, **changed})
            with (
                self.subTest(values=changed),
                self.assertRaises((ValueError, TypeError)),
            ):
                load_server_settings(config)
        self.assertFalse((self.root / "project").exists())
        self.assertFalse((self.root / "hashes.json").exists())

    def test_missing_files_invalid_json_and_relative_library_config_are_explicit(self):
        bad = self.root / "invalid.json"
        bad.write_text("{", encoding="utf-8")
        for path in (self.root / "absent.json", bad, Path("relative.json")):
            with self.subTest(path=path), self.assertRaises((ValueError, OSError)):
                load_server_settings(path)
        with self.assertRaises(ValueError):
            load_server_settings(self.config, {"project_root": "relative"})

    def test_import_and_openapi_do_not_open_stores_or_spawn_processes(self):
        script = "import json,multiprocessing,webserver,cli; print(json.dumps({'children':len(multiprocessing.active_children()),'paths':len(webserver.app.openapi()['paths'])}))"
        result = subprocess.run(
            [
                "uv",
                "run",
                "--project",
                str(REPOSITORY),
                "--no-sync",
                "python",
                "-B",
                "-c",
                script,
            ],
            cwd=self.root,
            env={**os.environ, "PYTHONPATH": str(REPOSITORY)},
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=20,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {"children": 0, "paths": 8})
        self.assertEqual({path.name for path in self.root.iterdir()}, {"server.json"})

    def test_cli_config_and_secret_validation_do_not_echo_tokens(self):
        config = self.root / "cli.json"
        write_json(
            config,
            {"server_url": "http://127.0.0.1:9000/api/", "token_env": "EMP_TEST_TOKEN"},
        )
        settings = cli.load_settings(config, {})
        with (
            patch.dict(os.environ, {"EMP_TEST_TOKEN": "secret with spaces"}),
            self.assertRaises(ValueError) as raised,
        ):
            cli.APIClient(settings)
        self.assertNotIn("secret with spaces", str(raised.exception))
        for name, value in (
            ("server_url", "http://user:secret@example.test"),
            ("request_timeout_seconds", 0),
            ("max_response_bytes", True),
        ):
            with self.subTest(name=name), self.assertRaises((ValueError, TypeError)):
                cli.load_settings(config, {name: value})

    def test_cli_server_help_and_environment_configuration_fail_without_runtime(self):
        for entry in ("cli.py", "webserver.py"):
            result = subprocess.run(
                [
                    "uv",
                    "run",
                    "--project",
                    str(REPOSITORY),
                    "--no-sync",
                    "python",
                    str(REPOSITORY / entry),
                    "--help",
                ],
                cwd=self.root,
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("usage:", result.stdout)
        result = subprocess.run(
            [
                "uv",
                "run",
                "--project",
                str(REPOSITORY),
                "--no-sync",
                "python",
                str(REPOSITORY / "webserver.py"),
            ],
            cwd=self.root,
            env={**os.environ, "EMP_SERVER_CONFIG": "relative.json"},
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("config_path must be absolute", result.stderr)
