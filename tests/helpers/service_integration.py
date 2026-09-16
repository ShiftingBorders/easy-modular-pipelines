"""Real local modules and a controller session for the approved integration plan."""

import copy
import json
import shutil
from pathlib import Path
from uuid import uuid4

import yaml

from tests.helpers.dag import DagSession
from tests.helpers.services import ServiceWorkspace, wait_for


class ServiceDagWorkspace:
    def __init__(self):
        self.files = ServiceWorkspace()
        self.root = self.files.root
        self.manager = self.files.modules
        self.hashes = self.files.hashes
        self.gates = []
        self.stage_count = 0
        self.session = None

    def service(self, **options):
        definition = self.files.service(**options)
        module = definition["module"]
        relative = Path(module["name"]) / module["version"]
        shutil.copytree(
            self.files.experiment / "modules" / relative,
            self.root / "modules" / relative,
        )
        return definition

    def stage(self, *, settings=None, retries=0, on_exhausted="pause", timeout=None):
        self.stage_count += 1
        name = f"stage-{self.stage_count}"
        code = self.root / "modules" / name / "1"
        code.mkdir(parents=True)
        shutil.copy2(Path(__file__).with_name("dag_stage.py"), code / "main.py")
        (code / "module.yaml").write_text(
            yaml.safe_dump(
                {
                    "schema_version": 1,
                    "name": name,
                    "version": "1",
                    "role": "stage",
                    "implementation": "full",
                    "commands": {"start": ["python", "-B", "main.py"]},
                    "defaults": {},
                }
            ),
            encoding="utf-8",
        )
        digest = self.manager.module_hash(name, target_folder=code)
        self.hashes.add_module_hash(name, "1", digest)
        return {
            "stage_id": str(uuid4()),
            "module": {"name": name, "version": "1", "hash": digest},
            "settings": {} if settings is None else settings,
            "timeout_seconds": timeout,
            "errors": {
                "retries": retries,
                "retry_delay_seconds": 1,
                "on_exhausted": on_exhausted,
            },
        }

    def template(self, stages=None, *, services=None, cycles=1):
        template = copy.deepcopy(self.files.state.template)
        template["stages"] = [self.stage()] if stages is None else stages
        if services is not None:
            template["services"] = services
        template["cycles"] = cycles
        return template

    def write_template(self, template, name="template.yaml"):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(template), encoding="utf-8")
        return path

    def gate(self):
        path = self.root / f"release-{uuid4()}"
        self.gates.append(path)
        return path

    async def launch(self, template, *, paused=True):
        if self.session is None:
            self.session = DagSession(self)
            await self.session.start()
        reply = await self.session.send(
            "run",
            {
                "template_path": str(self.write_template(template)),
                "delayed_start": paused,
            },
        )
        if reply["result"] != "success":
            raise AssertionError(reply)
        await wait_for(
            lambda: self.session.runner.get_state()["phase"] != "starting", timeout=100
        )
        return self.session.runner

    def trace(self, service):
        return self.files.trace(service)

    def stage_trace(self):
        state = self.session.runner._state
        if state is None:
            return []
        path = state.experiment_directory / "shared_data/trace.jsonl"
        if not path.exists():
            return []
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows

    async def close(self):
        # Release only fixture-owned faults before fallback cleanup; assertions have
        # already observed real outcomes, and the default product timers stay intact.
        for controls in self.files.controls.values():
            (controls / "release-fault").touch()
            for name in (
                "hold-start",
                "hold-stop",
                "hold-freeze",
                "hold-save",
                "ignore-shutdown",
                "fail-health",
                "silent-heartbeat",
            ):
                (controls / name).unlink(missing_ok=True)
        try:
            if self.session is not None:
                runner = self.session.runner
                self.files.managers.append(runner._services)
                # Fault assertions are complete. A fresh cleanup client permits
                # explicit shutdown after tests deliberately closed/failed the owner.
                if runner._state is not None and (
                    runner._closed or runner._journal.client._failed
                ):
                    runner._journal.close()
                    runner._journal.open(runner._state, create=False)
                await self.session.close()
        finally:
            await self.files.close()
