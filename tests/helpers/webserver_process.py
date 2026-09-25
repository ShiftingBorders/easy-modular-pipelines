"""Owned Uvicorn process for the approved HTTP/CLI test plan."""

import argparse
import asyncio
import os
import signal
import socket
from functools import partial
from multiprocessing.reduction import ForkingPickler
from pathlib import Path
from unittest.mock import patch

import uvicorn

import webserver
from core import serverruntime
from core.runner_utils.runtimeio import process_identity, write_json
from core.serverruntime import load_server_settings
from tests.helpers.server_faults import BlockedMessage, fault_controller, write_frame


async def serve(config, control, fault_mode):
    settings = load_server_settings(config)
    settings.fixture_control = str(control)
    settings.fixture_mode = fault_mode
    webserver.app.state.settings = settings
    write_json(control / "owner.json", {"process": process_identity(os.getpid())})
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        server = uvicorn.Server(
            uvicorn.Config(
                webserver.app,
                host="127.0.0.1",
                port=port,
                log_level="warning",
                timeout_graceful_shutdown=settings.shutdown_timeout + 5,
            )
        )
        webserver.app.state.stop_http = partial(setattr, server, "should_exit", True)

        async def observe():
            published = False
            while True:
                if server.started and not published:
                    runtime = webserver.app.state.runtime
                    write_json(
                        control / "ready.json",
                        {
                            "url": f"http://127.0.0.1:{port}/api",
                            "process": process_identity(os.getpid()),
                            "controller": runtime.health()["controller"],
                        },
                    )
                    published = True
                if (control / "stop").exists():
                    server.should_exit = True
                if (control / "interrupt").exists():
                    (control / "interrupt").unlink()
                    signal.raise_signal(signal.SIGINT)
                if published and (control / "truncate-request").exists():
                    write_frame(webserver.app.state.runtime._requests, b"\x80\x05\x95")
                    (control / "request-corrupted").touch()
                    (control / "truncate-request").unlink()
                if published and (control / "block-request").exists():
                    write_frame(
                        webserver.app.state.runtime._requests,
                        ForkingPickler.dumps(BlockedMessage(control / "request-read")),
                    )
                    while not (control / "request-read.entered").exists():
                        await asyncio.sleep(0.025)
                    os._exit(26)
                await asyncio.sleep(0.025)

        watcher = asyncio.create_task(observe())
        try:
            await server.serve(sockets=[listener])
            if not server.started:
                raise RuntimeError("HTTP application did not start.")
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--control", type=Path, required=True)
    parser.add_argument("--fault-mode", default="normal")
    options = parser.parse_args()
    if options.fault_mode == "normal":
        asyncio.run(serve(options.config, options.control, options.fault_mode))
    else:
        with patch.object(serverruntime, "controller_process", new=fault_controller):
            asyncio.run(serve(options.config, options.control, options.fault_mode))


if __name__ == "__main__":
    main()
