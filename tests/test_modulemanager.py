"""Approved scenarios from .artifacts/test-plans/modulemanager_scenarios.md."""

import hashlib
import io
import json
import lzma
import os
import platform
import shutil
import stat
import subprocess
import tarfile
import tempfile
import unittest
from email import policy
from email.parser import BytesParser
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx

from core.hashdb import HashDB
from core.modulemanager import ModuleManager
from core.seaweed import SeaweedDB
from core.storage_contracts import ModuleAddResult
from core.storage_errors import (
    StorageConflict,
    StorageError,
    StorageInputError,
    StorageUnavailable,
    StoredObjectNotFound,
)
from core.validation_constants import (
    CONTROL_CHARACTERS,
    HEX_DIGITS,
    INVALID_MODULE_NAME_CHARACTERS,
    INVALID_MODULE_VERSION_CHARACTERS,
    MODULE_IDENTITY_CHARACTERS,
    SQL_COLUMN_NAME_CHARACTERS,
    SQL_COLUMN_TYPE_CHARACTERS,
)
from utils.hashdb_utils.hashdb_states import ColumnValidationResult
from utils.hashdb_utils.hashdb_validation import validate_column_desc
from utils.modulemanager_utils.modulemanager_errors import HashMismatch
from utils.seaweed_utils.utils import ALLOWED_CHARACTERS, check_input_metadata


class ModuleManagerTestCase(unittest.TestCase):
    """Real files/SQLite and a real SeaweedDB client using an in-memory HTTP peer."""

    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory(prefix="modulemanager-tests-")
        self.addCleanup(self.workspace.cleanup)
        self.root = Path(self.workspace.name).resolve()
        self.source = self.root / "source"
        self.source.mkdir()
        self._write_manifest(self.source, "demo", "1")
        (self.source / "main.py").write_bytes(b"print('module')\n")
        (self.source / "nested").mkdir()
        (self.source / "nested" / "data.bin").write_bytes(b"\x00\xff\x01")
        (self.source / "empty.txt").touch()
        self.storage = self.root / "modules"
        self.work = self.root / "work"
        self.work.mkdir()
        self.sentinel = self.work / "unrelated.txt"
        self.sentinel.write_bytes(b"keep")
        (self.work / "unrelated-folder").mkdir()
        (self.work / "unrelated-folder" / "keep.bin").write_bytes(b"also keep")
        self.original_work = self._snapshot(self.work)
        schema = {
            "Mname": "VARCHAR(255) NOT NULL",
            "MVersion": "VARCHAR(255) NOT NULL",
            "MHash": "VARCHAR(255) NOT NULL",
        }
        (self.root / "schema.json").write_text(json.dumps(schema), encoding="utf-8")
        config = self.root / "config.json"
        config.write_text(
            json.dumps({"schema_path": "schema.json", "db_path": "hash.db"}),
            encoding="utf-8",
        )
        self.hashes = HashDB(config)
        self.addCleanup(self.hashes.close_connection)
        self.objects = {}
        self.requests = []
        client = httpx.Client(
            base_url="http://filer.test",
            transport=httpx.MockTransport(self._handle_filer),
        )
        with patch("core.seaweed.httpx.Client", return_value=client):
            self.archives = SeaweedDB("http://filer.test")
        self.addCleanup(self.archives.close)
        self.manager = ModuleManager(
            self.storage, self.hashes, self.archives, self.work
        )

    def _write_manifest(self, folder, name, version):
        """Provide a real module contract without bypassing production validation."""
        (folder / "module.yaml").write_text(
            json.dumps(
                {
                    "schema_version": 2,
                    "name": name,
                    "version": version,
                    "role": "stage",
                    "implementation": "full",
                    "commands": {"start": ["python", "-B", "main.py"]},
                    "defaults": {},
                }
            ),
            encoding="utf-8",
        )

    def _handle_filer(self, request):
        path = request.url.path
        self.requests.append((request.method, path))
        if request.method == "HEAD":
            return httpx.Response(200 if path in self.objects else 404)
        if request.method == "POST":
            if path in self.objects:
                return httpx.Response(409)
            message = BytesParser(policy=policy.default).parsebytes(
                f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode()
                + request.read()
            )
            parts = list(message.iter_parts())
            if len(parts) != 1:
                raise AssertionError("Expected one uploaded archive")
            self.objects[path] = parts[0].get_payload(decode=True)
            return httpx.Response(201)
        if request.method == "GET":
            if path not in self.objects:
                return httpx.Response(404)
            return httpx.Response(200, content=self.objects[path])
        if request.method == "DELETE":
            if path not in self.objects:
                return httpx.Response(404)
            del self.objects[path]
            return httpx.Response(204)
        raise AssertionError(f"Unexpected HTTP method: {request.method}")

    def _snapshot(self, folder):
        return {
            path.relative_to(folder).as_posix(): (
                None if path.is_dir() else path.read_bytes()
            )
            for path in folder.rglob("*")
            if not path.is_symlink() and not path.is_junction()
        }

    def _assert_rejected(self, error_type, action):
        before = self._snapshot(self.root)
        objects = self.objects.copy()
        writes = [
            request for request in self.requests if request[0] in {"POST", "DELETE"}
        ]
        with self.assertRaises(error_type):
            action()
        self.assertEqual(self._snapshot(self.root), before)
        self.assertEqual(self.objects, objects)
        self.assertEqual(
            [request for request in self.requests if request[0] in {"POST", "DELETE"}],
            writes,
        )

    def _register(self, name="demo", version="1"):
        return self.manager.register_module(name, version, self.source)

    def _seed_hash(self, digest=None, name="demo", version="1"):
        if digest is None:
            digest = self.manager.module_hash(name, self.source)
        self.hashes.add_module_hash(name, version, digest)
        return digest

    def _archive_bytes(self, entries):
        """Create independent package fixtures, including deliberately invalid tar entries."""
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:xz", preset=0) as archive:
            for name, data in entries:
                member = tarfile.TarInfo(name) if isinstance(name, str) else name
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
        return buffer.getvalue()

    def _store_package(self, code=None, digest=None, extra=()):
        digest = self._seed_hash(digest)
        if code is None:
            code = self._archive_bytes(
                [
                    (path, data)
                    for path, data in self._snapshot(self.source).items()
                    if data is not None
                ]
            )
        self.objects["/modules/demo/1"] = self._archive_bytes(
            [
                ("hash.txt", (digest + "\n").encode()),
                ("code.tar.xz", code),
                *extra,
            ]
        )
        return digest

    def _assert_work_clean(self):
        self.assertEqual(self._snapshot(self.work), self.original_work)

    def _operations(self, name="demo", version="1"):
        return {
            "module_hash": lambda: self.manager.module_hash(name, self.source),
            "register": lambda: self.manager.register_module(
                name, version, self.source
            ),
            "unregister": lambda: self.manager.unregister_module(name, version),
            "upload": lambda: self.manager.upload_module(
                name, version, self.source / "main.py"
            ),
            "extract": lambda: self.manager.extract_module(name, version),
            "validate": lambda: self.manager.validate_module(
                name, version, self.source
            ),
        }


class ArgumentTests(ModuleManagerTestCase):
    def test_constructor_rejects_invalid_path_types_without_io(self):
        """ARG-01."""
        for parameter in ("module_storage_path", "temp_folder"):
            for value in (None, 1, True, b"path", [], object()):
                with self.subTest(parameter=parameter, value=value):
                    arguments = {
                        "module_storage_path": self.storage,
                        "hash_db": self.hashes,
                        "module_db": self.archives,
                        "temp_folder": self.work,
                    }
                    arguments[parameter] = value
                    self._assert_rejected(
                        TypeError,
                        lambda arguments=arguments: ModuleManager(**arguments),
                    )
        self.assertEqual(self.requests, [])

    def test_constructor_rejects_invalid_path_values(self):
        """ARG-02; link scenarios are in PlatformTests."""
        for parameter in ("module_storage_path", "temp_folder"):
            for value, error in [
                ("", ValueError),
                ("  ", ValueError),
                ("bad\x00path", ValueError),
                (Path("relative"), ValueError),
                (self.source / "main.py", NotADirectoryError),
            ]:
                with self.subTest(parameter=parameter, value=value):
                    arguments = {
                        "module_storage_path": self.storage,
                        "hash_db": self.hashes,
                        "module_db": self.archives,
                        "temp_folder": self.work,
                    }
                    arguments[parameter] = value
                    self._assert_rejected(
                        error, lambda arguments=arguments: ModuleManager(**arguments)
                    )

    def test_constructor_rejects_invalid_ignore_collection_and_elements(self):
        """ARG-03."""
        for value in ("cache", [], {}, 1, {1}, {None}, {b"cache"}):
            with self.subTest(value=value):
                self._assert_rejected(
                    TypeError,
                    lambda value=value: ModuleManager(
                        self.storage, self.hashes, self.archives, self.work, value
                    ),
                )
        self._assert_rejected(
            ValueError,
            lambda: ModuleManager(
                self.storage, self.hashes, self.archives, self.work, {""}
            ),
        )
        ignored = {"cache", Path("nested")}
        manager = ModuleManager(
            str(self.storage), self.hashes, self.archives, str(self.work), ignored
        )
        ignored.clear()
        self.assertEqual(manager.ignore_folders, {Path("cache"), Path("nested")})

    def test_constructor_checks_each_storage_method_without_calling_it(self):
        """ARG-04: structural compatibility, not inheritance."""
        contracts = {
            "hash_db": ("add_module_hash", "get_module_hash", "remove_module_hash"),
            "module_db": (
                "check_module_stored",
                "save_module",
                "retrieve_module",
                "delete_module",
            ),
        }
        for dependency, methods in contracts.items():
            for value in (None, 3, HashDB, SeaweedDB):
                with self.subTest(dependency=dependency, value=value):
                    arguments = {
                        "module_storage_path": self.storage,
                        "hash_db": self.hashes,
                        "module_db": self.archives,
                        "temp_folder": self.work,
                    }
                    arguments[dependency] = value
                    self._assert_rejected(
                        TypeError,
                        lambda arguments=arguments: ModuleManager(**arguments),
                    )
            for method in methods:
                for missing in (True, False):
                    attributes = {name: Mock() for name in methods}
                    if missing:
                        del attributes[method]
                    else:
                        attributes[method] = 1
                    storage = SimpleNamespace(**attributes)
                    arguments = {
                        "module_storage_path": self.storage,
                        "hash_db": self.hashes,
                        "module_db": self.archives,
                        "temp_folder": self.work,
                    }
                    arguments[dependency] = storage
                    with self.subTest(
                        dependency=dependency, method=method, missing=missing
                    ):
                        with self.assertRaisesRegex(
                            TypeError, f"{dependency}.{method}"
                        ):
                            ModuleManager(**arguments)
                        for value in attributes.values():
                            if isinstance(value, Mock):
                                value.assert_not_called()
        hash_stub = SimpleNamespace(**{name: Mock() for name in contracts["hash_db"]})
        archive_stub = SimpleNamespace(
            **{name: Mock() for name in contracts["module_db"]}, extra=Mock()
        )
        manager = ModuleManager(self.storage, hash_stub, archive_stub, self.work)
        self.assertIs(manager.hash_db, hash_stub)
        self.assertIs(manager.module_db, archive_stub)
        self.assertEqual(self.requests, [])

    def test_all_public_operations_reject_invalid_name_types(self):
        """ARG-05."""
        for value in (None, 1, True, b"demo", [], {}):
            for operation, action in self._operations(name=value).items():
                with self.subTest(value=value, operation=operation):
                    self._assert_rejected(TypeError, action)

    def test_all_public_operations_reject_invalid_name_values(self):
        """ARG-06."""
        for value in (
            "",
            "  ",
            ".",
            "..",
            "a/b",
            "a\\b",
            "a:b",
            "bad*",
            "bad?",
            "bad<",
            'bad"',
            "bad|",
            "bad\x01",
            "CON",
            "LPT1.txt",
            "demo.",
        ):
            for operation, action in self._operations(name=value).items():
                with self.subTest(value=value, operation=operation):
                    self._assert_rejected(ValueError, action)

    def test_version_operations_reject_invalid_types_and_values(self):
        """ARG-07, ARG-08."""
        cases = [(value, TypeError) for value in (None, 1, True, b"1", [], {})]
        cases += [
            (value, ValueError)
            for value in ("", "  ", ".", "..", "a/b", "a\\b", "1\x01")
        ]
        for value, error in cases:
            operations = self._operations(version=value)
            del operations["module_hash"]
            for operation, action in operations.items():
                with self.subTest(value=value, operation=operation):
                    self._assert_rejected(error, action)

    def test_public_explicit_paths_reject_invalid_inputs(self):
        """ARG-09, ARG-15."""
        cases = [(value, TypeError) for value in (1, True, b"path", [], object())]
        cases += [(value, ValueError) for value in ("", "  ", "bad\x00", "relative")]
        for value, error in cases:
            actions = {
                "hash": lambda value=value: self.manager.module_hash("demo", value),
                "register": lambda value=value: self.manager.register_module(
                    "demo", "1", value
                ),
                "validate": lambda value=value: self.manager.validate_module(
                    "demo", "1", value
                ),
                "validate_with_hash": lambda value=value: self.manager.validate_module(
                    "demo", "1", value, "a" * 64
                ),
                "extract": lambda value=value: self.manager.extract_module(
                    "demo", "1", value
                ),
                "upload": lambda value=value: self.manager.upload_module(
                    "demo", "1", value
                ),
            }
            for operation, action in actions.items():
                with self.subTest(value=value, operation=operation):
                    self._assert_rejected(error, action)
        self._assert_rejected(
            TypeError, lambda: self.manager.upload_module("demo", "1", None)
        )

    def test_missing_sources_and_files_instead_of_folders(self):
        """ARG-10."""
        self._seed_hash()
        for source, error in (
            (self.root / "missing", FileNotFoundError),
            (self.source / "main.py", NotADirectoryError),
        ):
            with self.subTest(source=source):
                self._assert_rejected(
                    error,
                    lambda source=source: self.manager.register_module(
                        "demo", "1", source
                    ),
                )
                self._assert_rejected(
                    error,
                    lambda source=source: self.manager.validate_module(
                        "demo", "1", source
                    ),
                )
        self._assert_rejected(
            FileNotFoundError,
            lambda: self.manager.upload_module("demo", "1", self.root / "missing"),
        )
        self._assert_rejected(
            ValueError, lambda: self.manager.upload_module("demo", "1", self.source)
        )

    def test_hash_falls_back_to_installed_folder(self):
        """ARG-11."""
        shutil.copytree(self.source, self.storage / "demo")
        expected = self.manager.module_hash("demo")
        for path in (self.root / "missing", self.source / "main.py"):
            with self.subTest(path=path):
                self.assertEqual(self.manager.module_hash("demo", path), expected)

    def test_invalid_hash_types_and_values(self):
        """ARG-12, ARG-13, ARG-15; invalid hashes fail even without a DB record."""
        for value in (1, True, b"a" * 64, [], {}):
            with self.subTest(value=value):
                self._assert_rejected(
                    TypeError,
                    lambda value=value: self.manager.validate_module(
                        "demo", "1", module_hash=value
                    ),
                )
                self._assert_rejected(
                    TypeError,
                    lambda value=value: self.manager._add_hash_file(value, self.source),
                )
        self._assert_rejected(
            TypeError, lambda: self.manager._add_hash_file(None, self.source)
        )
        for value in ("", "a" * 63, "a" * 65, "g" * 64, " " + "a" * 64 + " "):
            with self.subTest(value=value):
                self._assert_rejected(
                    ValueError,
                    lambda value=value: self.manager.validate_module(
                        "demo", "1", module_hash=value
                    ),
                )
                self._assert_rejected(
                    ValueError,
                    lambda value=value: self.manager._add_hash_file(value, self.source),
                )
        digest = self._seed_hash()
        self.assertTrue(self.manager.validate_module("demo", "1", self.source, None))
        self.assertTrue(
            self.manager.validate_module("demo", "1", module_hash=digest.upper())
        )
        self.manager._add_hash_file(digest.upper(), self.source)
        self.assertEqual(
            (self.source / "hash.txt").read_bytes(), (digest + "\n").encode()
        )

    def test_invalid_registered_hashes(self):
        """ARG-14."""
        for value in (None, 1, b"a" * 64, "x" * 64, "a" * 63):
            with (
                self.subTest(value=value),
                patch.object(self.hashes, "get_module_hash", return_value=value),
            ):
                self._assert_rejected(
                    ValueError,
                    lambda: self.manager.validate_module("demo", "1", self.source),
                )
                self._assert_rejected(
                    ValueError, lambda: self.manager.extract_module("demo", "1")
                )

    def test_overlapping_work_and_destination_paths(self):
        """ARG-16."""
        for work in (self.source, self.source / "work"):
            manager = ModuleManager(self.storage, self.hashes, self.archives, work)
            self._assert_rejected(
                ValueError,
                lambda manager=manager: manager.register_module(
                    "demo", "1", self.source
                ),
            )
        for target in (self.source, self.source / "child", self.root):
            self._assert_rejected(
                ValueError,
                lambda target=target: self.manager._replace_folder(target, self.source),
            )
        archive = self.source / "input.tar.xz"
        archive.write_bytes(self._archive_bytes([]))
        self._assert_rejected(
            ValueError, lambda: self.manager._uncompress_folder(archive, self.source)
        )
        self._assert_rejected(
            ValueError,
            lambda: self.manager.extract_module(
                "demo", "1", self.storage / "demo" / "tmp"
            ),
        )


class LifecycleTests(ModuleManagerTestCase):
    def test_register_extract_validate_change_and_remove(self):
        """FLOW-01..04, FLOW-07, CLEAN-06."""
        original = self._snapshot(self.source)
        self.assertIs(self._register(), True)
        digest = self.hashes.get_module_hash("demo", "1")
        self.assertEqual(len(digest), 64)
        self.assertIn("/modules/demo/1", self.objects)
        self.assertEqual(self._snapshot(self.source), original)
        installed = self.manager.extract_module("demo", "1")
        self.assertEqual(installed, self.storage / "demo")
        self.assertEqual(self._snapshot(installed), original)
        self.assertIs(self.manager.validate_module("demo", "1"), True)
        package = self.objects.copy()
        (installed / "main.py").write_bytes(b"changed")
        self.assertIs(self.manager.validate_module("demo", "1"), False)
        self.assertEqual(self.hashes.get_module_hash("demo", "1"), digest)
        self.assertEqual(self.objects, package)
        self.assertIs(self.manager.unregister_module("demo", "1"), True)
        self.assertEqual(self.hashes.get_module_hash("demo", "1"), "")
        self.assertEqual(self.objects, {})
        self.assertTrue(installed.is_dir())
        self.assertIs(self.manager.unregister_module("demo", "1"), False)
        self.assertIs(self.manager.validate_module("demo", "1"), False)
        self._assert_work_clean()


class StorageFailureTests(ModuleManagerTestCase):
    def test_partial_registrations_conflict_and_can_be_removed(self):
        """STATE-01, STATE-02."""
        for hash_present, archive_present in ((True, False), (False, True)):
            with self.subTest(hash=hash_present, archive=archive_present):
                if hash_present:
                    self._seed_hash()
                if archive_present:
                    self.objects["/modules/demo/1"] = b"existing"
                self._assert_rejected(StorageConflict, self._register)
                self.assertIs(self.manager.unregister_module("demo", "1"), True)
                self.assertEqual(self.objects, {})
                self.assertEqual(self.hashes.get_module_hash("demo", "1"), "")
                self.assertIs(self.manager.unregister_module("demo", "1"), False)

    def test_read_failures_propagate_before_registration(self):
        """STATE-03."""
        for storage, method in (
            (self.hashes, "get_module_hash"),
            (self.archives, "check_module_stored"),
        ):
            error = StorageUnavailable("offline")
            with (
                self.subTest(method=method),
                patch.object(storage, method, side_effect=error),
            ):
                before = self._snapshot(self.root)
                with self.assertRaises(StorageUnavailable) as raised:
                    self._register()
                self.assertIs(raised.exception, error)
                self.assertEqual(self._snapshot(self.root), before)
                self.assertEqual(self.objects, {})

    def test_failed_upload_rolls_back_hash(self):
        """STATE-04, CLEAN-06."""
        error = StorageUnavailable("upload failed")
        with (
            patch.object(self.archives, "save_module", side_effect=error),
            self.assertRaises(StorageUnavailable) as raised,
        ):
            self._register()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.hashes.get_module_hash("demo", "1"), "")
        self.assertEqual(self.objects, {})
        self._assert_work_clean()

    def test_rollback_failures_are_notes_on_upload_error(self):
        """STATE-05."""
        for rollback in (False, StorageUnavailable("rollback failed")):
            upload_error = StorageUnavailable("upload failed")
            options = (
                {"side_effect": rollback}
                if isinstance(rollback, Exception)
                else {"return_value": rollback}
            )
            with (
                self.subTest(rollback=rollback),
                patch.object(self.archives, "save_module", side_effect=upload_error),
                patch.object(self.hashes, "remove_module_hash", **options),
            ):
                with self.assertRaises(StorageUnavailable) as raised:
                    self._register()
                self.assertIs(raised.exception, upload_error)
                self.assertIn("rollback", " ".join(upload_error.__notes__).lower())
            self.assertTrue(self.hashes.get_module_hash("demo", "1"))
            self.hashes.remove_module_hash("demo", "1")
            self._assert_work_clean()

    def test_unexpected_upload_failure_is_not_compensated(self):
        """STATE-06."""
        error = RuntimeError("programming failure")
        with (
            patch.object(self.archives, "save_module", side_effect=error),
            patch.object(
                self.hashes, "remove_module_hash", wraps=self.hashes.remove_module_hash
            ) as remove,
        ):
            with self.assertRaises(RuntimeError) as raised:
                self._register()
            remove.assert_not_called()
        self.assertIs(raised.exception, error)
        self.assertTrue(self.hashes.get_module_hash("demo", "1"))
        self._assert_work_clean()

    def test_duplicate_insert_rechecks_without_deleting_other_registration(self):
        """STATE-07."""
        real_add = self.hashes.add_module_hash
        for matching, archive_exists in ((True, True), (False, True), (True, False)):

            def competing_insert(
                name, version, digest, matching=matching, archive_exists=archive_exists
            ):
                real_add(name, version, digest if matching else "b" * 64)
                if archive_exists:
                    self.objects["/modules/demo/1"] = b"competitor archive"
                return ModuleAddResult.module_exists_err

            with (
                self.subTest(matching=matching, archive_exists=archive_exists),
                patch.object(
                    self.hashes, "add_module_hash", side_effect=competing_insert
                ),
                patch.object(
                    self.hashes,
                    "remove_module_hash",
                    wraps=self.hashes.remove_module_hash,
                ) as remove,
                patch.object(
                    self.archives, "save_module", wraps=self.archives.save_module
                ) as save,
            ):
                if matching and archive_exists:
                    self.assertIs(self._register(), False)
                else:
                    with self.assertRaises(StorageConflict):
                        self._register()
                save.assert_not_called()
                remove.assert_not_called()
            self.assertTrue(self.hashes.get_module_hash("demo", "1"))
            self.hashes.remove_module_hash("demo", "1")
            self.objects.clear()
            self._assert_work_clean()

    def test_archive_deletion_failure_preserves_hash(self):
        """STATE-08."""
        digest = self._seed_hash()
        self.objects["/modules/demo/1"] = b"existing"
        error = StorageUnavailable("delete failed")
        with (
            patch.object(self.archives, "delete_module", side_effect=error),
            patch.object(
                self.hashes, "remove_module_hash", wraps=self.hashes.remove_module_hash
            ) as remove,
        ):
            with self.assertRaises(StorageUnavailable) as raised:
                self.manager.unregister_module("demo", "1")
            remove.assert_not_called()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.hashes.get_module_hash("demo", "1"), digest)
        self.assertEqual(self.objects["/modules/demo/1"], b"existing")

    def test_hash_deletion_failure_can_be_retried(self):
        """STATE-09."""
        self._seed_hash()
        self.objects["/modules/demo/1"] = b"existing"
        error = StorageUnavailable("hash delete failed")
        with (
            patch.object(self.hashes, "remove_module_hash", side_effect=error),
            self.assertRaises(StorageUnavailable) as raised,
        ):
            self.manager.unregister_module("demo", "1")
        self.assertIs(raised.exception, error)
        self.assertIn("was removed", " ".join(error.__notes__))
        self.assertEqual(self.objects, {})
        self.assertIs(self.manager.unregister_module("demo", "1"), True)
        self.assertIs(self.manager.unregister_module("demo", "1"), False)

    def test_missing_registration_and_download_failures_preserve_installation(self):
        """STATE-10."""
        shutil.copytree(self.source, self.storage / "demo")
        self._assert_rejected(
            StoredObjectNotFound, lambda: self.manager.extract_module("demo", "1")
        )
        self._seed_hash()
        self._assert_rejected(
            StoredObjectNotFound, lambda: self.manager.extract_module("demo", "1")
        )
        self.objects["/modules/demo/1"] = b"existing"
        for result in (False, StorageUnavailable("download failed")):
            options = (
                {"side_effect": result}
                if isinstance(result, Exception)
                else {"return_value": result}
            )
            with (
                self.subTest(result=result),
                patch.object(self.archives, "retrieve_module", **options),
            ):
                self._assert_rejected(
                    StorageError, lambda: self.manager.extract_module("demo", "1")
                )
            self._assert_work_clean()

    def test_direct_upload_only_saves_archive(self):
        """STATE-11."""
        archive = self.source / "main.py"
        self.assertIs(self.manager.upload_module(" demo ", "1", str(archive)), True)
        self.assertEqual(self.objects["/modules/demo/1"], archive.read_bytes())
        self.assertEqual(self.hashes.get_module_hash("demo", "1"), "")
        with self.assertRaises(StorageConflict):
            self.manager.upload_module("demo", "1", archive)
        error = StorageUnavailable("offline")
        with (
            patch.object(self.archives, "save_module", side_effect=error),
            self.assertRaises(StorageUnavailable) as raised,
        ):
            self.manager.upload_module("other", "1", archive)
        self.assertIs(raised.exception, error)


class FileAndPackageTests(ModuleManagerTestCase):
    def test_hash_ignores_creation_order_location_and_metadata(self):
        """FILE-01."""
        other = self.root / "other"
        other.mkdir()
        for relative, content in reversed(list(self._snapshot(self.source).items())):
            if content is not None:
                path = other / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
        expected = self.manager.module_hash("demo", self.source)
        self.assertEqual(self.manager.module_hash("demo", other), expected)
        file = other / "main.py"
        os.utime(file, (1000000000, 1000000000))
        file.chmod(stat.S_IREAD if os.name == "nt" else 0o600)
        try:
            self.assertEqual(self.manager.module_hash("demo", other), expected)
        finally:
            file.chmod(stat.S_IREAD | stat.S_IWRITE)

    def test_hash_changes_for_each_file_change(self):
        """FILE-02."""
        initial = self.manager.module_hash("demo", self.source)
        actions = ("rename", "move", "add", "delete", "content")
        for action in actions:
            with self.subTest(action=action):
                other = self.root / action
                shutil.copytree(self.source, other)
                file = other / "main.py"
                if action == "rename":
                    file.rename(other / "renamed.py")
                elif action == "move":
                    file.rename(other / "nested" / "main.py")
                elif action == "add":
                    (other / "new.txt").touch()
                elif action == "delete":
                    file.unlink()
                else:
                    file.write_bytes(b"changed")
                self.assertNotEqual(self.manager.module_hash("demo", other), initial)

    def test_empty_folders_and_empty_files(self):
        """FILE-03."""
        folder = self.root / "empty"
        folder.mkdir()
        empty_hash = hashlib.sha256(b"").hexdigest()
        self.assertEqual(self.manager.module_hash("demo", folder), empty_hash)
        (folder / "nested" / "empty").mkdir(parents=True)
        self.assertEqual(self.manager.module_hash("demo", folder), empty_hash)
        (folder / "file").touch()
        self.assertNotEqual(self.manager.module_hash("demo", folder), empty_hash)
        for ignored in (folder, folder.parent):
            manager = ModuleManager(
                self.storage, self.hashes, self.archives, self.work, {ignored}
            )
            self.assertEqual(manager.module_hash("demo", folder), empty_hash)

    def test_relative_and_absolute_exclusions(self):
        """FILE-04."""
        for ignored in ("nested", self.source / "nested"):
            with self.subTest(ignored=ignored):
                manager = ModuleManager(
                    self.storage, self.hashes, self.archives, self.work, {ignored}
                )
                self.assertEqual(
                    manager._collect_module_files(self.source),
                    [Path("empty.txt"), Path("main.py"), Path("module.yaml")],
                )
                before = manager.module_hash("demo", self.source)
                (self.source / "nested" / "data.bin").write_bytes(b"ignored change")
                self.assertEqual(manager.module_hash("demo", self.source), before)
        other = self.root / "copy"
        shutil.copytree(self.source, other)
        manager = ModuleManager(
            self.storage,
            self.hashes,
            self.archives,
            self.work,
            {self.source / "nested"},
        )
        self.assertIn(Path("nested/data.bin"), manager._collect_module_files(other))

    def test_service_package_is_not_filtered(self):
        """FILE-05."""
        # The absolute work root is an ancestor of package, but not of the code.
        manager = ModuleManager(
            self.storage, self.hashes, self.archives, self.work, {self.work}
        )
        self.assertTrue(manager.register_module("demo", "1", self.source))
        with tarfile.open(
            fileobj=io.BytesIO(self.objects["/modules/demo/1"]), mode="r:xz"
        ) as archive:
            self.assertEqual(set(archive.getnames()), {"hash.txt", "source.tar.xz"})
        # Use a reader whose code exclusions do not exclude its own extraction root.
        self.manager.extract_module("demo", "1")
        self.assertTrue(self.manager.validate_module("demo", "1"))

    def test_binary_block_boundaries_and_unicode_file_names_round_trip(self):
        """FILE-06."""
        for size in (65535, 65536, 65537, 131073):
            (self.source / f"данные-{size}.bin").write_bytes(
                (b"\x00\xff\r\n" * (size // 4 + 1))[:size]
            )
        self._register()
        target = self.manager.extract_module("demo", "1")
        self.assertEqual(self._snapshot(target), self._snapshot(self.source))
        self.assertTrue(self.manager.validate_module("demo", "1"))

    def test_file_name_and_content_boundaries_are_unambiguous(self):
        """FILE-07: both naive name+content streams would be b'abc'."""
        first = self.root / "first"
        second = self.root / "second"
        first.mkdir()
        second.mkdir()
        (first / "a").write_bytes(b"bc")
        (second / "ab").write_bytes(b"c")
        self.assertNotEqual(
            self.manager.module_hash("demo", first),
            self.manager.module_hash("demo", second),
        )

    def test_strict_external_package_schema(self):
        """PACKAGE-01."""
        digest = self._seed_hash()
        code = self._archive_bytes([])
        directory = tarfile.TarInfo("extra")
        directory.type = tarfile.DIRTYPE
        good = [("hash.txt", digest.encode()), ("code.tar.xz", code)]
        cases = [
            good + [("extra.txt", b"extra")],
            good + [(directory, b"")],
            [("hash.txt", digest.encode()), ("nested/code.tar.xz", code)],
            [("code.tar.xz", code)],
            [("hash.txt", digest.encode())],
            good + [("other.tar.xz", code)],
        ]
        shutil.copytree(self.source, self.storage / "demo")
        for entries in cases:
            with self.subTest(names=[str(entry[0]) for entry in entries]):
                self.objects["/modules/demo/1"] = self._archive_bytes(entries)
                self._assert_rejected(
                    ValueError, lambda: self.manager.extract_module("demo", "1")
                )

    def test_duplicate_paths_in_both_archives(self):
        """PACKAGE-02, including normalized and Windows case aliases."""
        variants = [("a", "a"), ("a", "./a"), ("dir/a", "dir//a")]
        variants.append(("name", "NAME") if os.name == "nt" else ("name", "./name"))
        archive_path = self.root / "duplicate.tar.xz"
        for first, second in variants:
            for package in (False, True):
                with self.subTest(first=first, second=second, package=package):
                    archive_path.write_bytes(
                        self._archive_bytes([(first, b"one"), (second, b"two")])
                    )
                    self._assert_rejected(
                        ValueError,
                        lambda package=package: self.manager._uncompress_folder(
                            archive_path, self.root / "destination", package=package
                        ),
                    )

    def test_unsafe_tar_members_are_rejected_without_writing_outside(self):
        """PACKAGE-03."""
        entries = [
            (name, b"bad")
            for name in (
                "/outside",
                "../outside",
                "a/../../outside",
                "a\\b",
                "C:outside",
            )
        ]
        for kind in (
            tarfile.SYMTYPE,
            tarfile.LNKTYPE,
            tarfile.FIFOTYPE,
            tarfile.CHRTYPE,
        ):
            member = tarfile.TarInfo("link")
            member.type = kind
            member.linkname = "../outside"
            entries.append((member, b""))
        archive = self.root / "unsafe.tar.xz"
        for member in entries:
            with self.subTest(member=member[0]):
                archive.write_bytes(self._archive_bytes([member]))
                self._assert_rejected(
                    ValueError,
                    lambda: self.manager._uncompress_folder(
                        archive, self.root / "target"
                    ),
                )

    def test_corrupted_and_truncated_archives_preserve_target(self):
        """PACKAGE-04."""
        self._seed_hash()
        shutil.copytree(self.source, self.storage / "demo")
        valid = self._archive_bytes([("data", b"hello" * 100)])
        for content in (b"not an archive", valid[: len(valid) // 2], b"PK\x03\x04zip"):
            with self.subTest(content=content[:12]):
                self.objects["/modules/demo/1"] = content
                before = self._snapshot(self.storage)
                with self.assertRaises((tarfile.TarError, lzma.LZMAError, EOFError)):
                    self.manager.extract_module("demo", "1")
                self.assertEqual(self._snapshot(self.storage), before)
                self._assert_work_clean()

    def test_package_and_code_hash_mismatches_preserve_target(self):
        """PACKAGE-05."""
        digest = self._seed_hash()
        shutil.copytree(self.source, self.storage / "demo")
        code = self._archive_bytes([("main.py", b"wrong code")])
        for package_hash in ("f" * 64, digest):
            with self.subTest(package_hash=package_hash):
                self.objects["/modules/demo/1"] = self._archive_bytes(
                    [("hash.txt", package_hash.encode()), ("code.tar.xz", code)]
                )
                self._assert_rejected(
                    HashMismatch, lambda: self.manager.extract_module("demo", "1")
                )

    def test_code_can_contain_its_own_hash_and_archive(self):
        """PACKAGE-06."""
        (self.source / "hash.txt").write_bytes(b"module data, not package metadata")
        (self.source / "asset.tar.xz").write_bytes(b"opaque module asset")
        self._register()
        target = self.manager.extract_module("demo", "1")
        self.assertEqual(self._snapshot(target), self._snapshot(self.source))
        self.assertTrue(self.manager.validate_module("demo", "1"))

    def test_failed_compression_and_hash_write_preserve_previous_files(self):
        """PACKAGE-07."""
        output = self.root / "output"
        output.mkdir()
        archive = output / "source.tar.xz"
        archive.write_bytes(b"previous archive")
        with patch.object(tarfile.TarFile, "add", side_effect=OSError("read failed")):
            self._assert_rejected(
                OSError, lambda: self.manager._compress_folder(self.source, output)
            )
        self.assertEqual(archive.read_bytes(), b"previous archive")
        hash_file = self.source / "hash.txt"
        hash_file.write_bytes(b"previous hash")
        with patch.object(Path, "write_text", side_effect=OSError("write failed")):
            self._assert_rejected(
                OSError, lambda: self.manager._add_hash_file("a" * 64, self.source)
            )
        self.assertEqual(hash_file.read_bytes(), b"previous hash")


class RegistrationRepeatTests(ModuleManagerTestCase):
    def test_identical_registration_is_false_and_different_code_conflicts(self):
        """FLOW-05, FLOW-06."""
        self._register()
        objects = self.objects.copy()
        digest = self.hashes.get_module_hash("demo", "1")
        with patch.object(
            self.manager,
            "_compress_folder",
            side_effect=AssertionError("must not repack"),
        ):
            self.assertIs(self._register(), False)
            (self.source / "main.py").write_bytes(b"different code")
            with self.assertRaises(StorageConflict):
                self._register()
        self.assertEqual(self.objects, objects)
        self.assertEqual(self.hashes.get_module_hash("demo", "1"), digest)
        self.assertEqual(sum(method == "POST" for method, _ in self.requests), 1)

    def test_versions_names_and_reregistration_are_independent(self):
        """FLOW-08, FLOW-09."""
        for name, version in (("demo", "1"), ("demo", "2"), ("other", "1")):
            source = self.root / f"source-{name}-{version}"
            shutil.copytree(self.source, source)
            self._write_manifest(source, name, version)
            self.assertIs(self.manager.register_module(name, version, source), True)
        self.manager.unregister_module("demo", "1")
        self.assertEqual(set(self.objects), {"/modules/demo/2", "/modules/other/1"})
        self.assertTrue(self.hashes.get_module_hash("demo", "2"))
        self.assertTrue(self.hashes.get_module_hash("other", "1"))
        self.assertIs(self._register(), True)
        self.manager.extract_module("demo", "1")
        self.assertTrue(self.manager.validate_module("demo", "1"))

    def test_default_source_strings_and_normalized_name(self):
        """FLOW-10, FLOW-11."""
        shutil.copytree(self.source, self.storage / "demo")
        manager = ModuleManager(
            str(self.storage), self.hashes, self.archives, str(self.work)
        )
        self.assertIs(manager.register_module(" demo ", "1"), True)
        self.assertIs(manager.register_module("demo", "1", str(self.source)), False)
        target = manager.extract_module(" demo ", "1", str(self.work))
        self.assertEqual(target.name, "demo")
        self.assertTrue(manager.validate_module(" demo ", "1"))
        self.assertTrue(manager.validate_module("demo", "1", str(self.source)))
        self.assertTrue(manager.unregister_module(" demo ", "1"))

    def test_extract_replaces_existing_module_only_after_validation(self):
        """FLOW-02 and CLEAN-01: successful replacement removes stale files."""
        self._register()
        target = self.storage / "demo"
        target.mkdir(parents=True)
        (target / "stale.txt").write_bytes(b"old")
        self.manager.extract_module("demo", "1")
        self.assertEqual(self._snapshot(target), self._snapshot(self.source))
        self._assert_work_clean()


class ReplacementAndCleanupTests(ModuleManagerTestCase):
    def _old_target(self):
        target = self.root / "target"
        target.mkdir()
        (target / "old.txt").write_bytes(b"old")
        return target

    def test_copy_failure_preserves_source_and_target(self):
        """CLEAN-01."""
        target = self._old_target()
        with patch(
            "core.modulemanager.shutil.copytree", side_effect=OSError("copy failed")
        ):
            self._assert_rejected(
                OSError, lambda: self.manager._replace_folder(target, self.source)
            )

    def test_install_failure_restores_previous_folder(self):
        """CLEAN-02."""
        target = self._old_target()
        original_rename = Path.rename
        error = OSError("install failed")

        def rename(path, destination):
            if path.name == "new" and destination == target:
                raise error
            return original_rename(path, destination)

        with patch.object(Path, "rename", rename), self.assertRaises(OSError) as raised:
            self.manager._replace_folder(target, self.source)
        self.assertIs(raised.exception, error)
        self.assertEqual(self._snapshot(target), {"old.txt": b"old"})
        self.assertEqual(list(self.root.glob(".backup-*")), [])
        self.assertEqual(list(self.root.glob(".replace-*")), [])

    def test_restore_failure_keeps_backup_and_original_error(self):
        """CLEAN-03."""
        target = self._old_target()
        original_rename = Path.rename
        install_error = OSError("install failed")

        def rename(path, destination):
            if destination == target and path.name == "new":
                raise install_error
            if destination == target and path.name == "previous":
                raise OSError("restore failed")
            return original_rename(path, destination)

        with patch.object(Path, "rename", rename), self.assertRaises(OSError) as raised:
            self.manager._replace_folder(target, self.source)
        self.assertIs(raised.exception, install_error)
        backups = list(self.root.glob(".backup-*/previous"))
        self.assertEqual(len(backups), 1)
        self.assertEqual((backups[0] / "old.txt").read_bytes(), b"old")
        self.assertIn(str(backups[0]), " ".join(install_error.__notes__))
        self.assertFalse(target.exists())

    def test_backup_cleanup_error_reports_completed_installation(self):
        """CLEAN-04."""
        target = self._old_target()
        original_rmtree = shutil.rmtree
        cleanup_error = OSError("backup cleanup failed")

        def remove(path, *args, **kwargs):
            if Path(path).name.startswith(".backup-"):
                raise cleanup_error
            return original_rmtree(path, *args, **kwargs)

        with (
            patch("core.modulemanager.shutil.rmtree", side_effect=remove),
            self.assertRaises(OSError) as raised,
        ):
            self.manager._replace_folder(target, self.source)
        self.assertIs(raised.exception, cleanup_error)
        self.assertEqual(self._snapshot(target), self._snapshot(self.source))
        backup = next(self.root.glob(".backup-*"))
        notes = " ".join(cleanup_error.__notes__)
        self.assertIn("already installed", notes)
        self.assertIn(str(backup), notes)

    def test_primary_failures_survive_temporary_cleanup_errors(self):
        """CLEAN-05 across copy, compression, extraction, hash writing, registration and installation."""
        valid_archive = self.root / "input.tar.xz"
        valid_archive.write_bytes(self._archive_bytes([("data", b"data")]))
        self._store_package()
        other_source = self.root / "other-source"
        shutil.copytree(self.source, other_source)
        self._write_manifest(other_source, "other", "1")
        original_cleanup = tempfile.TemporaryDirectory.cleanup
        cases = (
            (
                ".replace-",
                "copytree",
                lambda: self.manager._replace_folder(self.root / "target", self.source),
            ),
            (
                ".compress-",
                "add",
                lambda: self.manager._compress_folder(self.source, self.root / "out"),
            ),
            (
                ".extract-",
                "extractall",
                lambda: self.manager._uncompress_folder(
                    valid_archive, self.root / "unpacked"
                ),
            ),
            (
                ".hash-",
                "write_text",
                lambda: self.manager._add_hash_file("a" * 64, self.source),
            ),
            (
                "register-module-",
                "_compress_folder",
                lambda: self.manager.register_module("other", "1", other_source),
            ),
            (
                "extract-module-",
                "retrieve_module",
                lambda: self.manager.extract_module("demo", "1"),
            ),
        )
        targets = {
            "copytree": shutil,
            "add": tarfile.TarFile,
            "extractall": tarfile.TarFile,
            "write_text": Path,
            "_compress_folder": self.manager,
            "retrieve_module": self.archives,
        }
        for prefix, method, action in cases:
            primary = OSError(f"{prefix} primary failure")
            retained = []

            def cleanup(temporary, prefix=prefix, retained=retained):
                if Path(temporary.name).name.startswith(prefix):
                    retained.append(temporary)
                    raise OSError("cleanup failure")
                return original_cleanup(temporary)

            with (
                self.subTest(prefix=prefix),
                patch.object(tempfile.TemporaryDirectory, "cleanup", cleanup),
                patch.object(targets[method], method, side_effect=primary),
            ):
                with self.assertRaises(OSError) as raised:
                    action()
                self.assertIs(raised.exception, primary)
                self.assertTrue(retained)
                self.assertIn(retained[0].name, " ".join(primary.__notes__))
            for temporary in retained:
                original_cleanup(temporary)
        self._assert_work_clean()

    def test_successful_install_reports_temporary_cleanup_failure(self):
        """CLEAN-04 for the outer extraction operation."""
        self._store_package()
        original_cleanup = tempfile.TemporaryDirectory.cleanup
        retained = []
        error = OSError("cleanup failure")

        def cleanup(temporary):
            if Path(temporary.name).name.startswith("extract-module-"):
                retained.append(temporary)
                raise error
            return original_cleanup(temporary)

        with (
            patch.object(tempfile.TemporaryDirectory, "cleanup", cleanup),
            self.assertRaises(OSError) as raised,
        ):
            self.manager.extract_module("demo", "1")
        self.assertIs(raised.exception, error)
        target = self.storage / "demo"
        self.assertEqual(self._snapshot(target), self._snapshot(self.source))
        notes = " ".join(error.__notes__)
        self.assertIn("already installed", notes)
        self.assertIn(str(target), notes)
        self.assertIn(retained[0].name, notes)
        for temporary in retained:
            original_cleanup(temporary)

    def test_interruption_still_cleans_operation_workspace(self):
        """CLEAN-07."""
        error = KeyboardInterrupt("cancel registration")
        with (
            patch.object(self.manager, "_compress_folder", side_effect=error),
            self.assertRaises(KeyboardInterrupt) as raised,
        ):
            self._register()
        self.assertIs(raised.exception, error)
        self.assertEqual(self.hashes.get_module_hash("demo", "1"), "")
        self.assertEqual(self.objects, {})
        self._assert_work_clean()


class PlatformTests(ModuleManagerTestCase):
    def _directory_link(self, path, target):
        """Exercise real Windows junctions or Linux directory symlinks."""
        operating_system = platform.system()
        self.assertIn(operating_system, {"Windows", "Linux"})
        if operating_system == "Windows":
            subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(path), str(target)],
                check=True,
                capture_output=True,
                text=True,
                creationflags=subprocess.CREATE_NO_WINDOW,
            )
            self.assertTrue(path.is_junction())
        else:
            path.symlink_to(target, target_is_directory=True)
            self.assertTrue(path.is_symlink())

    def test_current_platform_rejects_root_directory_links(self):
        """OS-01, OS-02, ARG-02, ARG-09."""
        link = self.root / "linked"
        self._directory_link(link, self.source)
        self._seed_hash()
        actions = (
            lambda: ModuleManager(link, self.hashes, self.archives, self.work),
            lambda: ModuleManager(self.storage, self.hashes, self.archives, link),
            lambda: self.manager.module_hash("demo", link),
            lambda: self.manager.register_module("demo", "1", link),
            lambda: self.manager.validate_module("demo", "1", link),
            lambda: self.manager.extract_module("demo", "1", link),
            lambda: self.manager._replace_folder(self.root / "target", link),
            lambda: self.manager._replace_folder(link, self.source),
        )
        for action in actions:
            with self.subTest(action=action), self.assertRaises(ValueError):
                action()
        self.assertEqual(self.objects, {})

    def test_nested_directory_links_are_not_archived_or_hashed(self):
        """OS-02: follow the native directory-link scenario without skipping it."""
        outside = self.root / "outside"
        outside.mkdir()
        (outside / "secret").write_bytes(b"outside module")
        expected = self.manager.module_hash("demo", self.source)
        self._directory_link(self.source / "linked", outside)
        self.assertEqual(self.manager.module_hash("demo", self.source), expected)
        archive_path = self.manager._compress_folder(self.source, self.root / "out")
        with tarfile.open(archive_path, "r:xz") as archive:
            self.assertEqual(
                set(archive.getnames()),
                {"main.py", "module.yaml", "empty.txt", "nested/data.bin"},
            )
        with self.assertRaises(ValueError):
            self.manager._replace_folder(self.root / "target", self.source)
        self.assertEqual((outside / "secret").read_bytes(), b"outside module")

    def test_file_links_and_broken_links_are_excluded(self):
        """OS-02: real file/broken symlinks on supported Windows and Linux runners."""
        expected = self.manager.module_hash("demo", self.source)
        link = self.source / "file-link"
        broken = self.source / "broken-link"
        try:
            link.symlink_to(self.sentinel)
        except OSError as error:
            if os.name == "nt" and getattr(error, "winerror", None) == 1314:
                self.skipTest(
                    "Windows file symlinks require Developer Mode or the symlink privilege."
                )
            raise
        broken.symlink_to(self.root / "absent")
        self.assertTrue(link.is_symlink())
        self.assertTrue(broken.is_symlink())
        self.assertEqual(self.manager.module_hash("demo", self.source), expected)
        archive_path = self.manager._compress_folder(self.source, self.root / "out")
        with tarfile.open(archive_path, "r:xz") as archive:
            self.assertNotIn("file-link", archive.getnames())
            self.assertNotIn("broken-link", archive.getnames())
        with self.assertRaises(ValueError):
            self.manager._replace_folder(self.root / "target", self.source)
        for path in (link, broken):
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.manager.module_hash("demo", path)

    def test_parent_directory_link_is_allowed(self):
        """OS-03."""
        actual = self.root / "actual"
        actual.mkdir()
        shutil.copytree(self.source, actual / "demo")
        parent = self.root / "parent-link"
        self._directory_link(parent, actual)
        source = parent / "demo"
        self.assertEqual(self.manager._validate_folder_path(source), actual / "demo")
        self.assertEqual(
            self.manager.module_hash("demo", source),
            self.manager.module_hash("demo", self.source),
        )
        manager = ModuleManager(
            parent / "installed", self.hashes, self.archives, self.work
        )
        self.assertTrue(manager.register_module("demo", "1", source))
        target = manager.extract_module("demo", "1")
        self.assertEqual(target, actual / "installed" / "demo")
        self.assertTrue(manager.validate_module("demo", "1"))

    def test_absolute_paths_work_after_changing_current_directory(self):
        """OS-04."""
        previous = Path.cwd()
        self.addCleanup(os.chdir, previous)
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir()
        expected = self.manager.module_hash("demo", self.source)
        os.chdir(elsewhere)
        self.assertEqual(self.manager.module_hash("demo", str(self.source)), expected)
        self.assertTrue(self.manager.register_module("demo", "1", str(self.source)))
        self.manager.extract_module("demo", "1", str(self.work))
        self.assertTrue(self.manager.validate_module("demo", "1"))
        with self.assertRaises(ValueError):
            self.manager.module_hash("demo", Path("relative"))


class ValidationConstantTests(unittest.TestCase):
    def test_imported_character_sets_preserve_the_fixed_contract(self):
        """CONST-01; expectations are independent literals."""
        letters_and_digits = (
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        )
        self.assertEqual(HEX_DIGITS, frozenset("0123456789abcdefABCDEF"))
        self.assertEqual(
            set(MODULE_IDENTITY_CHARACTERS), set(letters_and_digits + "_-.")
        )
        self.assertEqual(ALLOWED_CHARACTERS, MODULE_IDENTITY_CHARACTERS)
        self.assertEqual(
            SQL_COLUMN_NAME_CHARACTERS, frozenset(letters_and_digits + "_")
        )
        self.assertEqual(
            SQL_COLUMN_TYPE_CHARACTERS, frozenset(letters_and_digits + "_ (),")
        )
        self.assertEqual(len(CONTROL_CHARACTERS), 32)
        self.assertIn("\x00", CONTROL_CHARACTERS)
        self.assertIn("\x1f", CONTROL_CHARACTERS)
        self.assertNotIn(" ", CONTROL_CHARACTERS)
        self.assertTrue(set('<>:"/\\|?*').issubset(INVALID_MODULE_NAME_CHARACTERS))
        self.assertTrue(set("/\\\n\x00").issubset(INVALID_MODULE_VERSION_CHARACTERS))
        self.assertNotIn("_", INVALID_MODULE_NAME_CHARACTERS)

    def test_sql_and_archive_identity_validators_use_unchanged_rules(self):
        """CONST-02, with public manager/hash validation covered by ArgumentTests."""
        self.assertIs(
            validate_column_desc("Mname_2", "VARCHAR(255) NOT NULL"),
            ColumnValidationResult.column_valid,
        )
        self.assertIs(
            validate_column_desc("name-bad", "TEXT"),
            ColumnValidationResult.column_name_invalid_char,
        )
        self.assertIs(
            validate_column_desc("name", "TEXT; DROP TABLE MAIN"),
            ColumnValidationResult.column_type_invalid_char,
        )
        self.assertIsNone(check_input_metadata("Module_2.test", "1.0-beta"))
        for name, version in (("bad/name", "1"), ("demo", "bad!"), ("модуль", "1")):
            with (
                self.subTest(name=name, version=version),
                self.assertRaises(StorageInputError),
            ):
                check_input_metadata(name, version)
