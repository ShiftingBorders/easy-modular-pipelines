"""Approved E6; explicit only: uv run python -m unittest tests.integration_cli -v."""

import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from core.hashdb import HashDB
from tests.helpers.dag import (
    REPOSITORY,
    TEMP_ROOT,
    process_running,
    terminate_owned,
    wait_until,
)


class RealSeaweedCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_demo_registers_runs_two_cycles_and_stops_owned_storage(self):
        """E3/E4/E6: full CLI and real Filer; unavailable binary is an environment error."""
        binary = (
            REPOSITORY / "core/seaweedfs" / ("weed.exe" if os.name == "nt" else "weed")
        )
        self.assertTrue(
            binary.is_file(),
            f"Explicit integration run requires SeaweedFS binary: {binary}",
        )
        TEMP_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as temporary:
            ownership = Path(temporary) / "ownership.json"
            process = None
            output = None
            try:
                async with asyncio.timeout(120):
                    process = await asyncio.create_subprocess_exec(
                        "uv",
                        "run",
                        "--project",
                        str(REPOSITORY),
                        "--no-sync",
                        "python",
                        "-B",
                        "-m",
                        "tests.helpers.dag_cli",
                        "--ownership",
                        str(ownership),
                        "--audit-seaweed",
                        "--demo",
                        "--auto",
                        cwd=REPOSITORY,
                        stdin=asyncio.subprocess.DEVNULL,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    output = asyncio.create_task(process.communicate())
                    stdout, stderr = await asyncio.shield(output)
                    self.assertEqual(
                        process.returncode, 0, stderr.decode("utf-8", errors="replace")
                    )
                    text = stdout.decode("utf-8")
                    final = json.JSONDecoder().raw_decode(
                        text.split("Final state: ", 1)[1]
                    )[0]
                    self.assertEqual(final["data"]["phase"], "completed")
                    self.assertEqual(final["data"]["cycle_number"], 2)
                    self.assertIsNone(final["data"]["error"])
                    observed = json.loads(ownership.read_text())
                    self.assertTrue(observed["package_registered"])
                    root = Path(observed["project_root"])
                    database = HashDB(root / "hashes.json")
                    try:
                        self.assertEqual(
                            len(database.get_module_hash("counter", "1.0")), 64
                        )
                    finally:
                        database.close_connection()
                    mapping = json.loads((root / "experiments.json").read_text())
                    experiment = root / "experiments" / mapping[final["experiment_id"]]
                    results = list(experiment.glob("shared_artifacts/**/counter.json"))
                    self.assertEqual(len(results), 2)
                    for artifact in results:
                        self.assertEqual(json.loads(artifact.read_text())["ticks"], 5)
                    relative = Path(final["data"]["result"]["artifact"])
                    self.assertFalse(relative.is_absolute())
                    self.assertTrue((experiment / relative).is_file())
                    self.assertFalse(process_running(observed["cli"]["pid"]))
                    self.assertFalse(process_running(observed["seaweed"]["pid"]))
            finally:
                if ownership.exists():
                    observed = json.loads(ownership.read_text())
                    for name in ("cli", "seaweed"):
                        if name in observed:
                            terminate_owned(observed[name])
                            await wait_until(
                                lambda pid=observed[name]["pid"]: (
                                    not process_running(pid)
                                ),
                                timeout=5,
                            )
                    if "project_root" in observed:
                        root = Path(observed["project_root"]).resolve()
                        allowed = (REPOSITORY / ".artifacts/dag-demo").resolve()
                        if (
                            root.parent != allowed
                            or root.is_symlink()
                            or root.is_junction()
                        ):
                            raise ValueError(
                                "Integration cleanup target is outside the demo root."
                            )
                        for file in root.glob("experiments/**/process.json"):
                            record = json.loads(file.read_text())
                            for name in ("stage", "executor"):
                                if record.get(name):
                                    terminate_owned(record[name])
                                    await wait_until(
                                        lambda pid=record[name]["pid"]: (
                                            not process_running(pid)
                                        ),
                                        timeout=5,
                                    )
                        shutil.rmtree(root)
                if process is not None and process.returncode is None:
                    await asyncio.wait_for(process.wait(), 5)
                if output is not None:
                    await output
