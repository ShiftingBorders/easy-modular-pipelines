"""Real local files, hashes, SQLite and owned processes for service tests."""

from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
from pathlib import Path
from uuid import uuid4

import yaml

from core.experimentassembler import ExperimentAssembler
from core.hashdb import HashDB
from core.modulemanager import ModuleManager
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.launch import ModuleLauncher
from core.runner_utils.runtimeio import read_json, write_json
from core.runner_utils.services import ServiceManager
from core.runner_utils.state import RunnerState, RunnerStateStore
from core.seaweed import SeaweedDB
from tests.helpers.dag import REPOSITORY, process_running, terminate_owned

TEMP_ROOT = REPOSITORY / ".artifacts/tmp/service-tests"


async def wait_for(predicate, timeout=15):
    async with asyncio.timeout(timeout):
        while not (value := predicate()):
            await asyncio.sleep(0.025)
        return value


class ServiceWorkspace:
    def __init__(self) -> None:
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=TEMP_ROOT)
        self.root = Path(self.temporary.name).resolve()
        write_json(
            self.root / "schema.json",
            {
                "Mname": "VARCHAR(255) NOT NULL",
                "MVersion": "VARCHAR(255) NOT NULL",
                "MHash": "VARCHAR(255) NOT NULL",
            },
        )
        write_json(
            self.root / "hashes.json",
            {"schema_path": "schema.json", "db_path": "hashes.sqlite"},
        )
        self.hashes = HashDB(self.root / "hashes.json")
        # Only local hashing is exercised. This real archive client performs no network calls.
        self.archives = SeaweedDB("http://127.0.0.1:1")
        self.modules = ModuleManager(
            self.root / "modules", self.hashes, self.archives, self.root / "work"
        )
        self.experiment = self.root / "experiment"
        self.experiment.mkdir()
        self.template = {
            "schema_version": 1,
            "name": "services",
            "cycles": 1,
            "keep_attempts": 1,
            "start_timeout": 30,
            "runner_timeout_margin_seconds": 2,
            "stages": [],
            "services": [],
            "resources": [],
            "unknown_state": {
                "timeout_seconds": 10,
                "on_timeout": "pause",
                "recovery_limit": 3,
                "on_recovery_limit": "stop",
            },
            "snapshots": {"mode": "off", "keep": 3},
            "storage": {"min_snapshot_free_bytes": 0},
            "logging": read_json(REPOSITORY / "default_settings/logging.json"),
        }
        self.state = RunnerState(
            str(uuid4()),
            self.experiment,
            str(uuid4()),
            self.experiment / "experiment.yaml",
            str(uuid4()),
            "schema_version: 1\n",
            self.template,
            "paused",
        )
        self.journal = RunnerJournal()
        self.journal.open(self.state, create=True)
        self.store = RunnerStateStore()
        self.assembler = ExperimentAssembler(self.root, self.modules)
        self.launcher = ModuleLauncher(self.assembler, self.journal)
        self.manager = ServiceManager(self.launcher, self.journal, self.store)
        self.managers = [self.manager]
        self.controls = {}
        self.module_counter = 0

    def service(
        self,
        *,
        interface="socket",
        implementation="full",
        required=True,
        policy="pause",
        retries=3,
        name=None,
    ):
        self.module_counter += 1
        name = name or f"service-{self.module_counter}"
        code = self.experiment / "modules" / name / "1"
        code.mkdir(parents=True)
        shutil.copy2(Path(__file__).with_name("service_process.py"), code / "main.py")
        commands = {"start": ["python", "-B", "main.py"]}
        if implementation == "action":
            commands["stop"] = ["python", "-B", "main.py", "--action", "stop"]
        module = {
            "schema_version": 1,
            "name": name,
            "version": "1",
            "role": "service",
            "implementation": implementation,
            "service_interface": interface,
            "commands": commands,
            "defaults": {"nested": {"keep": 1, "replace": [1]}, "nullable": 1},
        }
        (code / "module.yaml").write_text(yaml.safe_dump(module), encoding="utf-8")
        digest = self.modules.module_hash(name, target_folder=code)
        self.hashes.add_module_hash(name, "1", digest)
        service_id = str(uuid4())
        controls = self.root / "controls" / service_id
        controls.mkdir(parents=True)
        self.controls[service_id] = controls
        definition = {
            "service_id": service_id,
            "module": {"name": name, "version": "1", "hash": digest},
            "settings": {
                "controls": str(controls),
                "nested": {"replace": [2]},
                "nullable": None,
            },
        }
        if interface == "socket":
            definition.update(
                heartbeat={"interval_seconds": 1, "grace_seconds": 10},
                command_timeout_seconds=30,
                on_command_timeout=policy,
                state_required=required,
                errors={
                    "retries": retries,
                    "retry_delay_seconds": 1,
                    "on_exhausted": "pause",
                },
            )
        self.state.template["services"].append(definition)
        self.state.template_yaml = yaml.safe_dump(self.state.template, sort_keys=False)
        self.state.template_path.write_text(self.state.template_yaml, encoding="utf-8")
        self.store.save(self.state)
        return definition

    def trace(self, definition):
        path = self.controls[definition["service_id"]] / "trace.jsonl"
        if not path.exists():
            return []
        # A final, incomplete line is still being published by a real process.
        lines = path.read_text(encoding="utf-8").splitlines()
        result = []
        for line in lines:
            try:
                result.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return result

    def events(self, kind=None):
        rows = self.journal.client.read_events(limit=1000)["events"]
        return [
            row["event"]
            for row in rows
            if kind is None or row["event"]["event_type"] == kind
        ]

    def replacement_manager(self):
        manager = ServiceManager(self.launcher, self.journal, self.store)
        self.managers.append(manager)
        self.manager = manager
        return manager

    async def close(self):
        for controls in self.controls.values():
            (controls / "release-fault").touch()
            for name in (
                "hold-start",
                "hold-stop",
                "hold-freeze",
                "silent-heartbeat",
                "ignore-shutdown",
                "fail-health",
                "fail-unfreeze",
            ):
                (controls / name).unlink(missing_ok=True)
        # Assertions run before this fallback. Teardown cannot manufacture their outcomes.
        for manager in self.managers:
            await manager.close()
        identities = []
        for controls in self.controls.values():
            for row in self.trace({"service_id": controls.name}):
                if row["event"] == "started":
                    identities.append(row["process"])
        for identity in identities:
            terminate_owned(identity)
            await wait_for(
                lambda pid=identity["pid"]: not process_running(pid), timeout=10
            )
        for manager in self.managers:
            for process in [*manager._processes.values(), *manager._action_processes]:
                if process.poll() is None:
                    process.terminate()
                await asyncio.to_thread(process.wait, 10)
        self.journal.close()
        self.archives.close()
        self.hashes.close_connection()
        if (
            self.root.is_symlink()
            or self.root.is_junction()
            or not self.root.resolve().is_relative_to(TEMP_ROOT.resolve())
        ):
            raise ValueError("Unsafe test cleanup target.")
        for attempt in range(10):
            try:
                self.temporary.cleanup()
                break
            except OSError as error:
                # Windows can retain a delete-pending SQLite file briefly after exit.
                if getattr(error, "winerror", None) not in (32, 145) or attempt == 9:
                    raise
                await asyncio.sleep(0.1)
