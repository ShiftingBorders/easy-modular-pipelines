"""Real periodic service for approved four-step weather DAG scenarios."""

import argparse
import asyncio
import random
import time
from pathlib import Path

from core.logger import OperationLogger
from core.runner_utils.participant_server import ParticipantServer
from core.runner_utils.runtimeio import read_json, write_json


class WeatherService:
    def __init__(self, context):
        self.context = context
        self.root = Path(context["experiment_directory"])
        self.data = Path(context["module_data_directory"])
        self.logger = OperationLogger(Path(context["logging_config_path"]))
        self.server = ParticipantServer(
            Path(context["endpoint_path"]), context["context"], self.logger, self.handle
        )
        self.stop = asyncio.Event()
        self.frozen = False
        self.forecast = None
        self.calls = 0
        self.random = random.Random(42)

    def generate(self):
        self.forecast = {
            "city": "Новосибирск",
            "temperature_c": self.random.randint(-15, 25),
            "condition": self.random.choice(["ясно", "облачно", "снег"]),
            "sequence": 1 if self.forecast is None else self.forecast["sequence"] + 1,
            "generated_monotonic": time.monotonic(),
        }
        write_json(self.data / "forecast.json", self.forecast)
        self.logger.record_event("weather.generated", self.forecast)

    async def tick(self):
        while True:
            await asyncio.sleep(30)
            if not self.frozen:
                self.generate()

    async def handle(self, request):
        command, args = request["command"], request["args"]
        if command == "heartbeat":
            return {"result": "success", "data": {"ready": self.forecast is not None}}
        if command == "execute":
            self.calls += 1
            settings = args["settings"]
            self.logger.record_event(
                "weather.request",
                {
                    "number": self.calls,
                    "settings": settings,
                    "request_id": request["request_id"],
                },
            )
            if settings.get("fail_first") and self.calls == 1:
                return {"result": "fail", "data": {"reason": "temporary_failure"}}
            if settings.get("delay_first") and self.calls == 1:
                await asyncio.sleep(settings["delay_first"])
            return {"result": "success", "data": dict(self.forecast)}
        if command == "shutdown":
            self.stop.set()
        elif command == "interrupt":
            pass
        elif command == "freeze_writes":
            self.frozen = True
        elif command == "unfreeze_writes":
            self.frozen = False
        elif command == "save_state":
            path = Path(args["output_directory"]) / "weather.json"
            write_json(path, {"forecast": self.forecast, "calls": self.calls})
            return {
                "result": "success",
                "data": {"state_path": path.relative_to(self.root).as_posix()},
            }
        elif command == "load_state":
            saved = read_json(self.root / args["state_path"])
            self.forecast, self.calls = saved["forecast"], saved["calls"]
            write_json(self.data / "forecast.json", self.forecast)
        else:
            return {"result": "fail", "data": {"reason": "unsupported"}}
        return {"result": "success", "data": {}}

    async def run(self):
        self.logger.open()
        ticking = None
        try:
            self.generate()
            await self.server.start()
            ticking = asyncio.create_task(self.tick())
            await self.stop.wait()
        finally:
            if ticking is not None:
                ticking.cancel()
                await asyncio.gather(ticking, return_exceptions=True)
            await self.server.close()
            self.logger.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--emp-context", type=Path, required=True)
    asyncio.run(WeatherService(read_json(parser.parse_args().emp_context)).run())
