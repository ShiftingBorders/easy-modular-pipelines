"""Isolated real files, SQLite and loopback HTTP for archive test groups A-J."""

import asyncio
import copy
import hashlib
import io
import json
import shutil
import sqlite3
import subprocess
import tarfile
import tempfile
import threading
import unittest
from email import policy
from email.parser import BytesParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote
from uuid import uuid4

import yaml

from core.experimentarchiver import ExperimentArchiver
from core.hashdb import HashDB
from core.modulemanager import ModuleManager
from core.runner_utils.experimentrunner import ExperimentRunner
from core.runner_utils.runtimeio import read_json, write_json
from core.seaweed import SeaweedDB
from tests.helpers.dag import REPOSITORY, process_running, terminate_owned, wait_until

TEMP_ROOT = REPOSITORY / ".artifacts/tmp/archive-tests"


class FilerRequest(BaseHTTPRequestHandler):
    """Implement only the HTTP object operations used by the real SeaweedDB client."""

    def handle_object(self):
        filer = self.server.filer
        parts = unquote(self.path).strip("/").split("/")
        if (
            len(parts) != 3
            or parts[0] != "modules"
            or any(p in (".", "..") for p in parts)
        ):
            self.send_error(400)
            return
        target = filer.root.joinpath(*parts)
        if not target.resolve().is_relative_to(filer.root):
            self.send_error(400)
            return
        filer.requests.append((self.command, self.path, threading.get_ident()))
        body = b""
        if self.command == "POST":
            size = int(self.headers["Content-Length"])
            if size > 4 * 1024 * 1024:
                self.send_error(413)
                return
            payload = self.rfile.read(size)
            filer.entered.set()
            if not filer.release.wait(15):
                self.send_error(503)
                return
            failure = filer.fail_paths.get(self.path, filer.fail_post)
            if failure:
                self.send_error(failure)
                return
            message = BytesParser(policy=policy.default).parsebytes(
                f"Content-Type: {self.headers['Content-Type']}\r\n\r\n".encode()
                + payload
            )
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(next(message.iter_parts()).get_payload(decode=True))
            if filer.disconnect_after_write:
                self.close_connection = True
                return
            status = 201
        elif self.command == "DELETE":
            status = 204 if target.exists() else 404
            target.unlink(missing_ok=True)
        else:
            status = 200 if target.is_file() else 404
            if status == 200 and self.command == "GET":
                body = target.read_bytes()
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    do_HEAD = do_GET = do_POST = do_DELETE = handle_object

    def log_message(self, *_args):
        pass


class DiskFiler:
    def __init__(self, root):
        self.root = root.resolve()
        self.root.mkdir()
        self.requests = []
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()
        self.fail_post = 0
        self.fail_paths = {}
        self.disconnect_after_write = False
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FilerRequest)
        self.server.filer = self
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.02}
        )
        self.thread.start()

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(10)
        if self.thread.is_alive():
            raise RuntimeError("The fixture HTTP server did not stop.")


class ArchiveWorkspace:
    def __init__(self):
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=TEMP_ROOT)
        self.root = Path(self.temporary.name).resolve()
        self.filer = DiskFiler(self.root / "filer")
        self.clients = []
        self.runners = []
        self.gates = []
        self.children = []
        self.child_files = []
        self.child_owners = []
        self.source = self.root / "source"
        self.target = self.root / "target"
        self.hashes, self.storage, self.manager = self.project(self.source)
        self.target_hashes, self.target_storage, self.target_manager = self.project(
            self.target
        )
        self.config = self.root / "archive-settings.json"
        settings = read_json(REPOSITORY / "default_settings/experiment_archiver.json")
        settings["min_free_bytes"] = 0
        write_json(self.config, settings)
        self.archiver = ExperimentArchiver(
            self.source, self.manager, config_path=self.config
        )
        self.importer = ExperimentArchiver(
            self.target, self.target_manager, config_path=self.config
        )
        self.archive = self.root / "experiment.tar.xz"
        self.destination = self.target / "imports" / "portable"

    def project(self, root):
        root.mkdir()
        shutil.copy2(
            REPOSITORY / "default_settings/hash_db_schema.json", root / "schema.json"
        )
        write_json(
            root / "hashes.json",
            {"schema_path": "schema.json", "db_path": "hashes.sqlite"},
        )
        hashes = HashDB(root / "hashes.json")
        storage = SeaweedDB(self.filer.url, timeout=10)
        self.clients.append((hashes, storage))
        manager = ModuleManager(root / "modules", hashes, storage, root / "work")
        return hashes, storage, manager

    def module(self, name="portable-stage", version="1", *, interface=None):
        folder = self.source / "modules" / name / version
        folder.mkdir(parents=True)
        helper = "archive_stage.py" if interface is None else "archive_service.py"
        shutil.copy2(Path(__file__).with_name(helper), folder / "main.py")
        if interface == "socket":
            shutil.copy2(
                Path(__file__).with_name("service_process.py"),
                folder / "service_process.py",
            )
        definition = {
            "schema_version": 1,
            "name": name,
            "version": version,
            "role": "stage" if interface is None else "service",
            "implementation": "action" if interface == "commands" else "full",
            "commands": {"start": ["python", "-B", "main.py"]},
            "defaults": {} if interface is None else {"auto_increment": False},
        }
        if interface is not None:
            definition["service_interface"] = interface
        if interface == "commands":
            definition["commands"]["stop"] = [
                "python",
                "-B",
                "main.py",
                "--action",
                "stop",
            ]
        (folder / "module.yaml").write_text(
            yaml.safe_dump(definition), encoding="utf-8"
        )
        digest = self.manager.module_hash(name, folder)
        self.hashes.add_module_hash(name, version, digest)
        return {"name": name, "version": version, "hash": digest}

    def template(self, *, services=False, versions=False, gate=None):
        first = self.module()
        modules = [first, first, self.module(version="2")] if versions else [first]
        stages = [
            {
                "stage_id": str(uuid4()),
                "module": module,
                "settings": {
                    "label": chr(65 + i),
                    **({"gate": str(gate)} if gate else {}),
                },
                "timeout_seconds": 30,
                "errors": {
                    "retries": 0,
                    "retry_delay_seconds": 1,
                    "on_exhausted": "pause",
                },
            }
            for i, module in enumerate(modules)
        ]
        services_list = []
        if services:
            for interface in ("socket", "commands"):
                service = {
                    "service_id": str(uuid4()),
                    "module": self.module(f"portable-{interface}", interface=interface),
                    "settings": {},
                }
                if interface == "socket":
                    service.update(
                        heartbeat={"interval_seconds": 1, "grace_seconds": 10},
                        command_timeout_seconds=30,
                        on_command_timeout="pause",
                        state_required=True,
                        errors={
                            "retries": 0,
                            "retry_delay_seconds": 1,
                            "on_exhausted": "pause",
                        },
                    )
                services_list.append(service)
        inputs = self.source / "inputs"
        (inputs / "tree/empty").mkdir(parents=True)
        (inputs / "source.txt").write_bytes(b"portable input\n")
        (inputs / "tree/данные с пробелом.txt").write_text(
            "test data", encoding="utf-8"
        )
        (inputs / "tree/zero.bin").write_bytes(b"")
        return {
            "schema_version": 1,
            "name": "portable-archive",
            "cycles": 2 if versions else 1,
            "keep_attempts": 1,
            "start_timeout": 30,
            "runner_timeout_margin_seconds": 2,
            "stages": stages,
            "services": services_list,
            "resources": [
                {
                    "name": "source.txt",
                    "path": "inputs/source.txt",
                    "hash": hashlib.sha256(b"portable input\n").hexdigest(),
                },
                {"name": "tree", "path": "inputs/tree", "hash": None},
            ],
            "unknown_state": {
                "timeout_seconds": 10,
                "on_timeout": "pause",
                "recovery_limit": 3,
                "on_recovery_limit": "stop",
            },
            "snapshots": {"mode": "off", "keep": 2},
            "storage": {"min_snapshot_free_bytes": 0},
            "logging": {
                **read_json(REPOSITORY / "default_settings/logging.json"),
                "min_free_bytes": 0,
            },
        }

    async def prepare(self, *, stopped=True, release_owner=True, **options):
        self.definition = self.template(**options)
        self.template_path = self.source / "experiment.yaml"
        self.template_path.write_text(yaml.safe_dump(self.definition), encoding="utf-8")
        self.runner = ExperimentRunner(
            self.source, self.manager, archive_config_path=self.config
        )
        self.runners.append(self.runner)
        await self.runner.run(self.template_path, "archive-source", delayed_start=True)
        await asyncio.wait_for(self.runner._ready.wait(), 65)
        if self.runner.get_state()["phase"] != "waiting":
            raise AssertionError(self.runner.get_state())
        self.state = self.runner._state
        if stopped:
            await self.runner.stop()
            if release_owner:
                await self.runner.close()
        return self.state

    async def create(self):
        self.result = await self.archiver.create(self.state, self.archive)
        return self.result

    def target_runner(self):
        runner = ExperimentRunner(
            self.target, self.target_manager, archive_config_path=self.config
        )
        self.runners.append(runner)
        return runner

    async def start_child(
        self, action, *, project=None, phase="none", destination=None
    ):
        ownership = self.root / f"child-{uuid4()}.json"
        output = ownership.with_suffix(".log").open("wb")
        self.child_files.append(output)
        self.child_owners.append(ownership)
        arguments = [
            "uv",
            "run",
            "--project",
            str(REPOSITORY),
            "--no-sync",
            "python",
            "-B",
            "-m",
            "tests.helpers.archive_owner",
            "--ownership",
            str(ownership),
            "--action",
            action,
            "--project-root",
            str(project or self.target),
            "--archive",
            str(self.archive),
            "--destination",
            str(destination or self.destination),
            "--config",
            str(self.config),
            "--filer-url",
            self.filer.url,
            "--phase",
            phase,
        ]
        process = await asyncio.create_subprocess_exec(
            *arguments,
            cwd=REPOSITORY,
            stdout=output,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.children.append(process)

        def ready():
            if not ownership.exists():
                return None
            try:
                return read_json(ownership)
            except (PermissionError, json.JSONDecodeError):
                return None

        record = await wait_until(ready, timeout=20)
        return process, ownership, record["process"]

    async def close(self):
        self.filer.release.set()
        for gate in self.gates:
            gate.touch(exist_ok=True)
        for child in self.children:
            if child.returncode is None:
                for path in self.child_owners:
                    if path.exists():
                        terminate_owned(read_json(path)["process"])
                child.terminate()
            await asyncio.wait_for(child.wait(), 15)
        for output in self.child_files:
            output.close()
        for runner in reversed(self.runners):
            if not runner._closed:
                await runner.stop()
                await runner.close()
        for record in self.root.glob(
            "*/experiments/*/shared_artifacts/**/process.json"
        ):
            data = read_json(record)
            for key in ("executor", "stage", "process"):
                if data.get(key):
                    terminate_owned(data[key])
                    await wait_until(
                        lambda pid=data[key]["pid"]: not process_running(pid)
                    )
        for hashes, storage in self.clients:
            storage.close()
            hashes.close_connection()
        self.filer.close()
        if (
            self.root.is_symlink()
            or self.root.is_junction()
            or not self.root.is_relative_to(TEMP_ROOT.resolve())
        ):
            raise ValueError("Unsafe archive fixture cleanup target.")
        for attempt in range(10):
            try:
                self.temporary.cleanup()
                break
            except OSError as error:
                if getattr(error, "winerror", None) not in (5, 32, 145) or attempt == 9:
                    raise
                await asyncio.sleep(0.05)


class ArchiveTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.w = ArchiveWorkspace()
        self.addAsyncCleanup(self.w.close)

    async def source_archive(self, **options):
        await self.w.prepare(**options)
        return await self.w.create()

    def assert_work_clean(self):
        self.assertEqual(list(self.w.root.rglob("experiment-archive-*")), [])
        self.assertEqual(list(self.w.root.rglob("register-module-*")), [])


def inventory(root):
    return {
        p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


def journal_events(config):
    settings = read_json(Path(config))
    connection = sqlite3.connect(
        Path(settings["logging"]["db_path"]).as_uri() + "?mode=ro", uri=True
    )
    try:
        return [
            json.loads(row[0])
            for row in connection.execute(
                "SELECT event_json FROM events ORDER BY cursor"
            )
        ]
    finally:
        connection.close()


def archive_members(path):
    with tarfile.open(path, "r:xz") as archive:
        return [
            (
                copy.copy(member),
                archive.extractfile(member).read() if member.isfile() else None,
            )
            for member in archive.getmembers()
        ]


def write_archive(path, members, *, manifest_change=None):
    members = copy.deepcopy(members)
    with tarfile.open(path, "w:xz", format=tarfile.USTAR_FORMAT, preset=0) as archive:
        for member, content in members:
            if member.name == "manifest.json" and manifest_change is not None:
                document = json.loads(content)
                manifest_change(document)
                content = json.dumps(document, ensure_ascii=False).encode("utf-8")
            if content is not None:
                member.size = len(content)
            archive.addfile(member, None if content is None else io.BytesIO(content))
    return path
