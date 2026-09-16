"""Real isolated HTTP server/CLI processes; no shared ports or machine settings."""

import asyncio
import codecs
import json
import os
import subprocess
from uuid import uuid4

import httpx
import psutil
import yaml

from core.runner_utils.runtimeio import process_identity, read_json, write_json
from tests.helpers.archives import ArchiveTestCase
from tests.helpers.dag import REPOSITORY, process_running, terminate_owned


class HTTPServer:
    def __init__(
        self, workspace, *, project=None, settings=None, env=None, fault_mode="normal"
    ):
        self.w = workspace
        self.project = project or workspace.source
        self.control = workspace.root / f"http-{uuid4()}"
        self.control.mkdir()
        self.config = self.control / "server.json"
        document = {
            "project_root": os.path.relpath(self.project, self.control),
            "hash_config_path": os.path.relpath(
                self.project / "hashes.json", self.control
            ),
            "filer_url": workspace.filer.url,
            "archive_config_path": str(workspace.config),
            **(settings or {}),
        }
        write_json(self.config, document)
        self.settings = document
        self.env = {
            **os.environ,
            "PYTHONPATH": str(REPOSITORY),
            "PYTHONIOENCODING": "utf-8",
            **(env or {}),
        }
        self.token = self.env.get(document.get("token_env"))
        self.process = None
        self.output = None
        self.client = None
        self.url = None
        self.owned = {}
        self._closed = False
        self.fault_mode = fault_mode

    def observe_children(self):
        for identity in list(self.owned.values()):
            try:
                if (
                    not process_running(identity["pid"])
                    or process_identity(identity["pid"]) != identity
                ):
                    continue
                for child in psutil.Process(identity["pid"]).children(recursive=True):
                    observed = process_identity(child.pid)
                    self.owned[child.pid] = observed
            except (OSError, psutil.Error):
                continue

    async def start(self, *, expect_ready=True, cwd=None):
        self.output = (self.control / "server.log").open("wb")
        self.process = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--project",
            str(REPOSITORY),
            "--no-sync",
            "python",
            "-B",
            "-m",
            "tests.helpers.webserver_process",
            "--config",
            str(self.config),
            "--control",
            str(self.control),
            "--fault-mode",
            self.fault_mode,
            cwd=cwd or REPOSITORY,
            env=self.env,
            stdout=self.output,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if not expect_ready:
            return self
        async with asyncio.timeout(40):
            while True:
                if self.process.returncode is not None:
                    raise AssertionError(
                        (self.control / "server.log").read_text(
                            encoding="utf-8", errors="replace"
                        )
                    )
                try:
                    ready = read_json(self.control / "ready.json")
                except (FileNotFoundError, PermissionError, json.JSONDecodeError):
                    await asyncio.sleep(0.025)
                    continue
                break
        self.url = ready["url"]
        for key in ("process", "controller"):
            self.owned[ready[key]["pid"]] = ready[key]
        self.client = httpx.AsyncClient(
            base_url=self.url,
            timeout=15,
            trust_env=False,
            headers={}
            if self.token is None
            else {"Authorization": f"Bearer {self.token}"},
        )
        self.observe_children()
        return self

    async def get(self, path, **params):
        response = await self.client.get(path, params=params)
        if response.status_code != 200:
            raise AssertionError((response.status_code, response.text))
        return response.json()

    async def submit(self, name, args=None, *, target=None, command_id=None):
        body = {
            "command": name,
            "args": args or {},
            "command_id": command_id or str(uuid4()),
        }
        if target is not None:
            body["target"] = target
        response = await self.client.post("/commands", json=body)
        if response.status_code != 202:
            raise AssertionError((response.status_code, response.text))
        return response.json()

    async def result(self, identifier, *, timeout=45):
        async with asyncio.timeout(timeout):
            while True:
                result = await self.get("/commands/" + identifier)
                if result["state"] != "pending":
                    return result
                await asyncio.sleep(0.025)

    async def command(self, name, args=None, *, target=None, timeout=45):
        receipt = await self.submit(name, args, target=target)
        return await self.result(receipt["command_id"], timeout=timeout)

    async def wait_state(self, phase, *, timeout=45):
        async with asyncio.timeout(timeout):
            while True:
                state = await self.get("/state")
                if state["phase"] == phase:
                    self.observe_children()
                    return state
                if state["phase"] == "failed" and phase != "failed":
                    raise AssertionError(state)
                await asyncio.sleep(0.025)

    async def launch(self, template, *, paused=True, experiment_id=None):
        path = self.project / f"template-{uuid4()}.yaml"
        path.write_text(yaml.safe_dump(template, allow_unicode=True), encoding="utf-8")
        args = {"template_path": str(path), "delayed_start": paused}
        if experiment_id:
            args["experiment_id"] = experiment_id
        result = await self.command("run", args)
        if result["result"] != "success":
            raise AssertionError(result)
        return await self.wait_state("waiting" if paused else "completed")

    async def close(self):
        if self._closed:
            return
        self._closed = True
        self.observe_children()
        owner_file = self.control / "controller-owner.json"
        if owner_file.exists():
            identity = read_json(owner_file)["process"]
            self.owned[identity["pid"]] = identity
        for path in self.control.rglob("*.entered"):
            path.with_suffix(".release").touch()
        (self.control / "release").touch()
        (self.control / "stop").touch()
        if self.client is not None:
            await self.client.aclose()
        try:
            if self.process is not None and self.process.returncode is None:
                try:
                    await asyncio.wait_for(
                        self.process.wait(),
                        self.settings.get("shutdown_timeout_seconds", 60) + 10,
                    )
                except TimeoutError:
                    owner = self.control / "owner.json"
                    if owner.exists():
                        terminate_owned(read_json(owner)["process"])
                    await asyncio.wait_for(self.process.wait(), 15)
        finally:
            for identity in reversed(list(self.owned.values())):
                terminate_owned(identity)
            if self.output is not None:
                self.output.close()


class HTTPCLI:
    def __init__(self, workspace, server):
        self.w = workspace
        self.server = server
        self.process = None
        self.reader = self.stderr = None
        self.responses = asyncio.Queue()
        self.ownership = workspace.root / f"cli-{uuid4()}.json"
        self.stdout = bytearray()

    async def start(self, arguments, *, env=None, cwd=None):
        self.process = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--project",
            str(REPOSITORY),
            "--no-sync",
            "python",
            "-B",
            "-u",
            "-m",
            "tests.helpers.cli_process",
            "--ownership",
            str(self.ownership),
            "--url",
            self.server.url,
            "--json",
            *arguments,
            cwd=cwd or REPOSITORY,
            env={**os.environ, "PYTHONPATH": str(REPOSITORY), **(env or {})},
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.reader = asyncio.create_task(self.read())
        self.stderr = asyncio.create_task(self.process.stderr.read())
        return self

    async def read(self):
        buffer = ""
        decoder = codecs.getincrementaldecoder("utf-8")()
        parser = json.JSONDecoder()
        while chunk := await self.process.stdout.read(4096):
            self.stdout.extend(chunk)
            buffer += decoder.decode(chunk)
            while buffer.strip():
                buffer = buffer.lstrip()
                try:
                    document, end = parser.raw_decode(buffer)
                except json.JSONDecodeError:
                    break
                buffer = buffer[end:]
                self.responses.put_nowait(document)

    async def send(self, line, *, timeout=45):
        self.process.stdin.write((line + "\n").encode("utf-8"))
        await self.process.stdin.drain()
        return await asyncio.wait_for(self.responses.get(), timeout)

    async def finish(self, *, timeout=45):
        await asyncio.wait_for(self.process.wait(), timeout)
        await self.reader
        return (
            self.process.returncode,
            self.stdout.decode("utf-8"),
            (await self.stderr).decode("utf-8", errors="replace"),
        )

    async def close(self):
        if self.process is not None and self.process.returncode is None:
            if not self.process.stdin.is_closing():
                self.process.stdin.close()
            try:
                await asyncio.wait_for(self.process.wait(), 3)
            except TimeoutError:
                if self.ownership.exists():
                    terminate_owned(read_json(self.ownership)["process"])
                await asyncio.wait_for(self.process.wait(), 15)
        for task in (self.reader, self.stderr):
            if task is not None:
                await task


class ServerTestCase(ArchiveTestCase):
    async def start_server(self, **options):
        server = HTTPServer(self.w, **options)
        self.addAsyncCleanup(server.close)
        await server.start()
        return server

    async def start_cli(self, server, arguments, **options):
        client = HTTPCLI(self.w, server)
        self.addAsyncCleanup(client.close)
        return await client.start(arguments, **options)
