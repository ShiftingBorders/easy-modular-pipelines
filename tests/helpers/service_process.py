"""Small independent TCP service for the approved service test plan."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import struct
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

from core.logger import OperationLogger
from core.runner_utils.runtimeio import process_identity, read_json, write_json


class PythonService:
    def __init__(self, context: dict) -> None:
        self.context = context
        self.identity = {
            key: context["context"][key]
            for key in ("experiment_id", "participant_id", "participant_instance_id")
        }
        self.directory = Path(context["artifacts_directory"])
        self.controls = Path(context["settings"]["controls"])
        self.data = Path(context["module_data_directory"])
        self.logger = OperationLogger(Path(context["logging_config_path"]))
        self.clients = {}
        self.tasks = set()
        self.work_lock = asyncio.Lock()
        self.counter_lock = asyncio.Lock()
        self.finished = asyncio.Event()
        self.current = None
        self.pending = []
        self.used = set()
        self.counter = 0
        self.loaded = False
        self.frozen = False
        self.client_number = 0

    async def publish_counter(self) -> None:
        # Windows readers temporarily prevent atomic replacement. Serialize writes
        # so freeze also waits for a tick that is retrying such a sharing violation.
        async with self.counter_lock:
            deadline = time.monotonic() + 1
            while True:
                try:
                    write_json(
                        self.data / "counter.json",
                        {"counter": self.counter, "frozen": self.frozen},
                    )
                    return
                except PermissionError as error:
                    if (
                        getattr(error, "winerror", None) not in (5, 32)
                        or time.monotonic() >= deadline
                    ):
                        raise
                    await asyncio.sleep(0.01)

    def trace(self, event: str, **data) -> None:
        with (self.controls / "trace.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(
                json.dumps(
                    {
                        "event": event,
                        "pid": os.getpid(),
                        "at": time.monotonic(),
                        **self.context["context"],
                        **self.identity,
                        **data,
                    }
                )
                + "\n"
            )

    def fault(self, phase: str) -> None:
        path = self.controls / "fault.json"
        if not path.exists():
            return
        fault = read_json(path)
        if fault["phase"] != phase:
            return
        write_json(
            self.controls / "fault-entered.json",
            {"phase": phase, "process": process_identity(os.getpid())},
        )
        if fault["action"] == "crash":
            os._exit(19)
        while not (self.controls / "release-fault").exists():
            time.sleep(0.05)

    async def send(self, writer, message, *, malformed=False) -> None:
        payload = b"\xff" if malformed else json.dumps(message).encode("utf-8")
        frame = struct.pack("!Q", len(payload)) + payload
        async with self.clients[writer]:
            if (self.controls / "split-frames").exists():
                for chunk in (frame[:3], frame[3:8], frame[8:]):
                    writer.write(chunk)
                    await writer.drain()
                    await asyncio.sleep(0)
            else:
                writer.write(frame)
                await writer.drain()

    async def read(self, reader, *, hold=False) -> dict:
        size = int.from_bytes(await reader.readexactly(8), "big")
        if hold and (self.controls / "pause-reading").exists():
            write_json(self.controls / "reading-paused.json", {"pid": os.getpid()})
            while (self.controls / "pause-reading").exists():
                await asyncio.sleep(0.05)
        return json.loads(await reader.readexactly(size))

    async def client(self, reader, writer) -> None:
        self.clients[writer] = asyncio.Lock()
        self.client_number += 1
        client_number = self.client_number
        try:
            hello = await self.read(reader)
            if (
                hello.get("identity") != self.identity
                or hello.get("token") != self.token
            ):
                return
            await self.send(
                writer,
                {
                    "protocol_version": 2,
                    "message_type": "hello",
                    "result": "success",
                    "data": self.identity,
                },
            )
            while not self.finished.is_set():
                request = await self.read(reader, hold=client_number == 1)
                request_id = request["request_id"]
                if request_id in self.used:
                    self.trace("duplicate", request_id=request_id)
                    return
                self.used.add(request_id)
                if any(
                    request.get(key) != value for key, value in self.identity.items()
                ):
                    self.trace("wrong_identity", request=request)
                    return
                command = request["command"]
                self.trace("received", command=command, request_id=request_id)
                if command == "heartbeat":
                    injected = self.controls / "heartbeat-replies.json"
                    if injected.exists():
                        for message in read_json(injected)["messages"]:
                            await self.send(
                                writer, {"request_id": request_id, **message}
                            )
                        injected.unlink()
                    silence = self.controls / "silent-heartbeat"
                    if silence.exists() and silence.read_text().strip() in (
                        "",
                        self.identity["participant_instance_id"],
                    ):
                        continue
                    while (self.controls / "hold-start").exists():
                        await asyncio.sleep(0.05)
                    malformed = self.controls / "malformed"
                    invalid = malformed.exists()
                    if invalid:
                        remaining = int(malformed.read_text()) - 1
                        if remaining:
                            malformed.write_text(str(remaining))
                        else:
                            malformed.unlink()
                    await self.send(
                        writer,
                        {
                            "protocol_version": 2,
                            "message_type": "response",
                            "request_id": request_id,
                            "result": "fail"
                            if (self.controls / "fail-health").exists()
                            or (
                                self.loaded
                                and self.context["settings"].get("fail_after_load")
                            )
                            else "success",
                            "data": {"counter": self.counter, "frozen": self.frozen},
                        },
                        malformed=invalid,
                    )
                elif command == "command_state":
                    await self.send(
                        writer,
                        {
                            "protocol_version": 2,
                            "message_type": "response",
                            "request_id": request_id,
                            "result": "success",
                            "data": {"current": self.current, "pending": self.pending},
                        },
                    )
                elif command == "shutdown":
                    self.action_marker("stop")
                    while (self.controls / "hold-stop").exists():
                        await asyncio.sleep(0.05)
                    if (self.controls / "ignore-shutdown").exists():
                        continue
                    if (self.controls / "fail-stop").exists():
                        await self.send(
                            writer,
                            {
                                "protocol_version": 2,
                                "message_type": "response",
                                "request_id": request_id,
                                "result": "fail",
                                "data": {"reason": "internal_stop_failed"},
                            },
                        )
                        continue
                    await self.send(
                        writer,
                        {
                            "protocol_version": 2,
                            "message_type": "response",
                            "request_id": request_id,
                            "result": "success",
                            "data": {"shutdown": True},
                        },
                    )
                    self.finished.set()
                    return
                else:
                    entry = {
                        "request_id": request_id,
                        "command": command,
                        "args": request["args"],
                    }
                    self.pending.append(entry)
                    task = asyncio.create_task(self.work(entry))
                    self.tasks.add(task)
                    task.add_done_callback(self.tasks.discard)
        except (OSError, asyncio.IncompleteReadError, json.JSONDecodeError):
            return
        finally:
            self.clients.pop(writer, None)
            writer.transport.abort()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def work(self, entry: dict) -> None:
        async with self.work_lock:
            self.pending.remove(entry)
            self.current = entry
            command, args = entry["command"], entry["args"]
            self.trace("work_started", **entry)
            if args.get("gate"):
                while not Path(args["gate"]).exists():
                    await asyncio.sleep(0.05)
            result = "success"
            data = args
            if command == "freeze_writes":
                self.trace("freeze_entered")
                while (self.controls / "hold-freeze").exists():
                    await asyncio.sleep(0.05)
                self.fault("before_freeze")
                self.frozen = True
                await self.publish_counter()
                data = {"frozen": True}
            elif command == "save_state":
                self.fault("save_state")
                while (self.controls / "hold-save").exists():
                    await asyncio.sleep(0.05)
                if (self.controls / "fail-save").exists():
                    result, data = "fail", {"reason": "export rejected"}
                elif (self.controls / "null-state").exists():
                    data = {"state_path": None}
                elif (self.controls / "escape-state").exists():
                    data = {"state_path": "../outside.json"}
                elif (self.controls / "export-path.json").exists():
                    data = read_json(self.controls / "export-path.json")
                else:
                    path = Path(args["output_directory"]) / "counter-state.json"
                    write_json(path, {"counter": self.counter})
                    data = {
                        "state_path": path.relative_to(
                            Path(self.context["experiment_directory"])
                        ).as_posix()
                    }
            elif command == "load_state":
                while (
                    self.context["settings"].get("hold_load")
                    and not (self.controls / "release-load").exists()
                ):
                    await asyncio.sleep(0.025)
                if (self.controls / "fail-load").exists() or self.context[
                    "settings"
                ].get("reject_state"):
                    result, data = "fail", {"reason": "restore rejected"}
                else:
                    self.counter = read_json(
                        Path(self.context["experiment_directory"]) / args["state_path"]
                    )["counter"]
                    self.loaded = True
                    data = {"loaded": self.counter}
            elif command == "unfreeze_writes":
                if (self.controls / "fail-unfreeze").exists():
                    result, data = "fail", {"reason": "unfreeze rejected"}
                else:
                    self.frozen = False
                    data = {"frozen": False}
            elif command == "set_value":
                if self.frozen:
                    result, data = "fail", {"reason": "writes are frozen"}
                elif type(args.get("value")) is not int:
                    result, data = "fail", {"reason": "value must be an integer"}
                else:
                    self.counter = args["value"]
                    await self.publish_counter()
                    data = {"value": self.counter}
            elif command == "get_value":
                data = {"value": self.counter}
            elif command == "fail":
                result, data = "fail", {"reason": "work rejected"}
            response = {"result": result, "data": data}
            if args.get("lose_result"):
                self.trace("unreported_work_finished", **entry)
                self.current = None
                return
            self.logger.record_command_result(
                entry["request_id"],
                response,
                author="participant",
                outcome="succeeded" if result == "success" else "failed",
                context=args.get("context") if command == "execute" else None,
            )
            self.trace("work_finished", **entry, response=response)
            self.current = None
            for writer in list(self.clients):
                try:
                    await self.send(
                        writer,
                        {
                            "protocol_version": 2,
                            "message_type": "response",
                            "request_id": entry["request_id"],
                            **response,
                        },
                    )
                except (OSError, KeyError):
                    continue

    async def ticks(self) -> None:
        while not self.finished.is_set():
            self.fault("idle")
            if not self.frozen and self.context["settings"].get("auto_increment", True):
                self.counter += 1
                await self.publish_counter()
            await asyncio.sleep(0.25)

    def action_marker(self, name: str) -> None:
        if not self.context["settings"].get("command_proxy"):
            return
        row = {
            "action": name,
            "pid": os.getpid(),
            "at": time.monotonic(),
            **self.context["context"],
        }
        with (self.controls / "actions.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row) + "\n")
        write_json(
            self.controls / f"action-{name}.json",
            {"pid": os.getpid(), "context": self.context},
        )

    async def run(self) -> None:
        self.action_marker("start")
        self.logger.open()
        self.token = str(uuid4())
        token = self.directory / "service.token"
        token.write_text(self.token, encoding="utf-8")
        endpoint = Path(self.context["endpoint_path"])
        server = await asyncio.start_server(self.client, "127.0.0.1", 0)
        write_json(
            endpoint,
            {
                "protocol_version": 2,
                **self.identity,
                "process": process_identity(os.getpid()),
                "endpoint": {
                    "host": "127.0.0.1",
                    "port": server.sockets[0].getsockname()[1],
                    "token_file": str(token),
                },
            },
        )
        write_json(self.directory / "received-context.json", self.context)
        self.trace("started", process=process_identity(os.getpid()))
        ticking = asyncio.create_task(self.ticks())
        try:
            await self.finished.wait()
        finally:
            server.close()
            for task in [ticking, *self.tasks]:
                task.cancel()
            await asyncio.gather(ticking, *self.tasks, return_exceptions=True)
            for writer in list(self.clients):
                # A deliberately unread test frame must not prevent process exit.
                writer.transport.abort()
            await server.wait_closed()
            self.logger.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--emp-context", type=Path, required=True)
    parser.add_argument("--action", choices=("start", "stop"), default="start")
    parser.add_argument("--child", action="store_true")
    options = parser.parse_args()
    context = read_json(options.emp_context)
    if context["settings"].get("launch_child") and not options.child:
        with subprocess.Popen(
            [
                sys.executable,
                "-B",
                __file__,
                "--emp-context",
                str(options.emp_context),
                "--child",
            ],
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        ) as child:
            result = child.wait()
            if context["settings"].get("launcher_cleanup"):
                controls = Path(context["settings"]["controls"])
                write_json(
                    controls / "launcher-cleanup-started.json",
                    {"process": process_identity(os.getpid())},
                )
                while (controls / "hold-launcher-cleanup").exists():
                    time.sleep(0.025)
                (
                    Path(context["module_data_directory"]) / "launcher-cleanup.txt"
                ).write_text("old launcher cleanup", encoding="utf-8")
                (controls / "launcher-cleanup-finished").touch()
            raise SystemExit(result)
    asyncio.run(PythonService(context).run())


if __name__ == "__main__":
    main()
