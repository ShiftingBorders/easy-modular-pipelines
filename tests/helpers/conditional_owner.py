"""Crash a real runner only at approved conditional-transition test boundaries."""

import argparse
import asyncio
import os
from pathlib import Path
from unittest.mock import patch

from core.experiments.runner import ExperimentRunner
from core.experiments.stages import StageRunner
from core.modules.manager import ModuleManager
from core.primitives.json_files import write_json
from core.primitives.processes import process_identity
from core.storage.hash_db import HashDB
from core.storage.seaweed_client import SeaweedDB


async def run_owner(options) -> None:
    hashes = HashDB(options.root / "hashes.json")
    archives = SeaweedDB("http://127.0.0.1:1")
    manager = ModuleManager(
        options.root / "modules", hashes, archives, options.root / "owner-work"
    )
    runner = ExperimentRunner(options.root, manager)
    original_accept = StageRunner._accept
    original_save = ExperimentRunner._save_state
    original_attempt_save = StageRunner._save_state

    def crash():
        write_json(
            options.root / "owner-fault.json",
            {
                "phase": options.boundary,
                "process": process_identity(os.getpid()),
            },
        )
        os._exit(23)

    def accept(stages, state, attempt, response, outcome):
        result = original_accept(stages, state, attempt, response, outcome)
        if (
            options.boundary == "accepted"
            and attempt.stage_id == options.condition
            and result["result"] == "success"
        ):
            crash()
        return result

    def save(owner):
        original_save(owner)
        transfer = owner._state.pending_input
        if (
            options.boundary == "transition"
            and transfer is not None
            and transfer.source_stage_id == options.condition
        ):
            crash()

    def save_attempt(stages, state, **kwargs):
        original_attempt_save(stages, state, **kwargs)
        attempt = state.active_attempt
        if (
            options.boundary == "target_started"
            and attempt is not None
            and attempt.stage_id == options.target
            and attempt.process_identity is not None
        ):
            crash()

    try:
        await runner.recover(options.experiment)
        write_json(
            options.root / "owner-ready.json",
            {"process": process_identity(os.getpid())},
        )
        with (
            patch.object(StageRunner, "_accept", accept),
            patch.object(ExperimentRunner, "_save_state", save),
            patch.object(StageRunner, "_save_state", save_attempt),
        ):
            await runner.resume()
            await asyncio.wait_for(asyncio.shield(runner._task), 60)
        raise RuntimeError("The selected conditional crash boundary was not reached.")
    finally:
        try:
            await runner.stop()
        finally:
            await runner.close()
            archives.close()
            hashes.close_connection()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--condition", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument(
        "--boundary",
        choices=("accepted", "transition", "target_started"),
        required=True,
    )
    asyncio.run(run_owner(parser.parse_args()))


if __name__ == "__main__":
    main()
