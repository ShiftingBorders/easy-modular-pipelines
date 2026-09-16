"""Real A/B/C experiments for the approved snapshots.md plan."""

import asyncio
import hashlib
import json
import subprocess
from pathlib import Path

from core.runner_utils.experimentrunner import ExperimentRunner
from core.runner_utils.runtimeio import read_json
from tests.helpers.dag import REPOSITORY, process_running, terminate_owned
from tests.helpers.service_integration import ServiceDagWorkspace
from tests.helpers.services import wait_for


class SnapshotWorkspace:
    def __init__(self, *, services=True, mode="off", keep=8, cycles=2):
        self.files = ServiceDagWorkspace()
        self.root = self.files.root
        self.manager = self.files.manager
        self.socket = self.commands = None
        if services:
            self.socket = self.files.service(name="snapshot-state")
            self.socket["settings"]["auto_increment"] = False
            self.commands = self.files.service(
                name="snapshot-commands", interface="commands", implementation="action"
            )
        self.stages = [
            self.files.stage(settings={"label": name}) for name in ("A", "B", "C")
        ]
        self.template = self.files.template(self.stages, cycles=cycles)
        self.template["snapshots"] = {"mode": mode, "keep": keep}
        self.template_path = self.files.write_template(self.template)
        self.runner = ExperimentRunner(self.root, self.manager)
        self.runners = [self.runner]
        self._closed = False
        self.owners = []

    async def start_owner(
        self, operation, phase, *, action="crash", snapshot=None, experiment_id=None
    ):
        output = (self.root / f"owner-{len(self.owners)}.log").open("wb")
        arguments = [
            "uv",
            "run",
            "python",
            "-m",
            "tests.helpers.snapshot_owner",
            "--root",
            str(self.root),
            "--experiment",
            experiment_id or self.runner._state.experiment_id,
            "--operation",
            operation,
            "--phase",
            phase,
            "--action",
            action,
        ]
        if snapshot is not None:
            arguments.extend(["--snapshot", snapshot])
        process = await asyncio.create_subprocess_exec(
            *arguments,
            cwd=REPOSITORY,
            stdout=output,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.owners.append((process, output))
        return process

    async def launch(self):
        self.template_path = self.files.write_template(self.template)
        await self.runner.run(self.template_path, delayed_start=True)
        await asyncio.wait_for(self.runner._ready.wait(), 90)
        if self.runner.get_state()["phase"] != "waiting":
            raise AssertionError(self.runner.get_state())
        return self.runner

    def replacement(self):
        self.runner = ExperimentRunner(self.root, self.manager)
        self.runners.append(self.runner)
        return self.runner

    async def value(self, value=None):
        command = "get_value" if value is None else "set_value"
        reply = await self.runner._services.request(
            self.runner._state,
            self.socket["service_id"],
            command,
            {} if value is None else {"value": value},
        )
        if reply["result"] != "success":
            raise AssertionError(reply)
        return reply["data"]["value"]

    def archive(self, snapshot_id):
        return (
            self.root
            / "snapshots"
            / self.runner._state.experiment_directory.name
            / snapshot_id
        )

    def manifests(self):
        root = self.root / "snapshots" / self.runner._state.experiment_directory.name
        return sorted(
            (read_json(path) for path in root.glob("*/manifest.json")),
            key=lambda item: item["sequence"],
        )

    def actions(self):
        path = self.files.files.controls[self.commands["service_id"]] / "actions.jsonl"
        if not path.is_file():
            return []
        return [
            json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
        ]

    async def close(self):
        if self._closed:
            return
        self._closed = True
        (self.root / "release-owner").touch()
        for process, output in self.owners:
            if process.returncode is None:
                for name in ("owner-fault.json", "owner-ready.json"):
                    path = self.root / name
                    if path.is_file():
                        terminate_owned(read_json(path)["process"])
                try:
                    await asyncio.wait_for(process.wait(), 10)
                except TimeoutError:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), 10)
            output.close()
        # Fault observations are complete. Release only this fixture's gates,
        # then reap known processes even when a deliberately broken restore failed.
        for gate in self.files.gates:
            gate.touch(exist_ok=True)
        for controls in self.files.files.controls.values():
            (controls / "release-fault").touch()
            for name in (
                "hold-start",
                "hold-stop",
                "hold-freeze",
                "hold-save",
                "ignore-shutdown",
                "fail-health",
                "silent-heartbeat",
                "fail-unfreeze",
                "fail-load",
                "fail-save",
            ):
                (controls / name).unlink(missing_ok=True)
        for runner in reversed(self.runners):
            self.files.files.managers.append(runner._services)
            if not runner._closed and runner._state is not None:
                try:
                    if not getattr(runner._journal, "_opened", False):
                        runner._journal.open(runner._state, create=False)
                    async with asyncio.timeout(65):
                        await runner.stop()
                except (OSError, RuntimeError, ValueError):
                    # Invalid journals/directories are intentional test inputs.
                    # OS cleanup below never infers ownership from a PID alone.
                    pass
            await runner.close()
        identities = []
        for path in self.root.glob("experiments/*/shared_artifacts/**/process.json"):
            record = read_json(path)
            identities.extend(
                record[key]
                for key in ("stage", "executor", "process")
                if record.get(key)
            )
        for identity in identities:
            terminate_owned(identity)
        for identity in identities:
            await wait_for(lambda pid=identity["pid"]: not process_running(pid), 15)
        await self.files.close()


def file_inventory(directory: Path) -> dict[str, str]:
    """Compare persisted bytes independently of the production manifest validator."""
    return {
        path.relative_to(directory).as_posix(): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for path in directory.rglob("*")
        if path.is_file()
    }
