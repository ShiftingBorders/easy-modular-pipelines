"""Command-backed service with the same protocol as every other service."""

import argparse
import asyncio
import subprocess
import sys
from pathlib import Path

from core.logger import OperationLogger
from core.logger_utils.events import LoggingError
from core.runner_utils.participant_server import ParticipantServer
from core.runner_utils.runtimeio import capture_stream, read_json, write_json


class CommandProxy:
    def __init__(self, context: dict) -> None:
        self.context = context
        self.directory = Path(context["module_data_directory"])
        self.logger = OperationLogger(Path(context["logging_config_path"]))
        self.stopped = asyncio.Event()
        self.startup = None
        self.frozen = False
        self.server = ParticipantServer(
            Path(context["endpoint_path"]),
            context["context"],
            self.logger,
            self.handle,
            control_timeout_seconds=context["control_timeout_seconds"],
        )

    async def action(self, name: str) -> None:
        command = [
            sys.executable,
            "-B",
            str(Path(__file__).with_name("action.py")),
            name,
            "--runtime-directory",
            str(self.directory),
        ]
        try:
            self.logger.record_event(
                "control.intent", {"action": name, "argv": command}
            )
        except LoggingError as error:
            if name != "stop":
                raise
            write_json(self.directory / "stop.emergency.json", {"error": str(error)})
        task = asyncio.create_task(
            asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        )
        process = None
        streams = []
        try:
            process = await asyncio.shield(task)
            if name == "stop":
                # This finite demo emits little output. A broken journal must not
                # interrupt the external cleanup command before it has finished.
                stdout, stderr = await process.communicate()
                output = {
                    "stdout": stdout.decode("utf-8", errors="replace"),
                    "stderr": stderr.decode("utf-8", errors="replace"),
                }
                try:
                    for stream, text in output.items():
                        if text:
                            self.logger.record_event(
                                "command.output", {"stream": stream, "text": text}
                            )
                except LoggingError as error:
                    write_json(
                        self.directory / "stop.emergency.json",
                        {"error": str(error), "output": output},
                    )
            else:
                streams = [
                    asyncio.create_task(
                        capture_stream(
                            stream, name, self.logger, self.context["context"]
                        )
                    )
                    for stream, name in (
                        (process.stdout, "stdout"),
                        (process.stderr, "stderr"),
                    )
                ]
                await asyncio.gather(process.wait(), *streams)
        except BaseException:
            process = process or await task
            if process.returncode is None:
                process.kill()
            if streams:
                await asyncio.gather(process.wait(), *streams, return_exceptions=True)
            else:
                await process.communicate()
            raise
        if process.returncode != 0:
            raise RuntimeError(f"{name} command failed: exit {process.returncode}.")

    async def handle(self, request: dict) -> dict:
        command = request["command"]
        if command == "heartbeat":
            # Completion of the finite start command precedes full readiness.
            await asyncio.shield(self.startup)
            ready = (
                self.directory / "ready.txt"
            ).is_file() and not self.stopped.is_set()
            return {"result": "success" if ready else "fail", "data": {"ready": ready}}
        if command == "shutdown":
            if not self.startup.done():
                self.startup.cancel()
                await asyncio.gather(self.startup, return_exceptions=True)
            await self.action("stop")
            if (self.directory / "ready.txt").exists():
                return {"result": "fail", "data": {"reason": "resource_not_stopped"}}
            self.stopped.set()
            return {"result": "success", "data": {"stopped": True}}
        if command == "execute":
            await asyncio.shield(self.startup)
            if self.frozen:
                return {"result": "fail", "data": {"reason": "writes_frozen"}}
            return {"result": "success", "data": request["args"]["input_data"]}
        if command == "freeze_writes":
            self.frozen = True
        elif command == "unfreeze_writes":
            self.frozen = False
        elif command == "save_state":
            return {"result": "success", "data": {"state_path": None}}
        elif command == "interrupt":
            return {"result": "success", "data": {"stopped": True}}
        else:
            return {"result": "fail", "data": {"reason": "unsupported_command"}}
        return {"result": "success", "data": {}}

    async def run(self) -> None:
        self.logger.open()
        try:
            await self.server.start()
            self.startup = asyncio.create_task(self.action("start"))
            await self.stopped.wait()
        finally:
            if self.startup is not None and not self.startup.done():
                self.startup.cancel()
                await asyncio.gather(self.startup, return_exceptions=True)
            await self.server.close()
            self.logger.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--emp-context", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(CommandProxy(read_json(args.emp_context)).run())


if __name__ == "__main__":
    main()
