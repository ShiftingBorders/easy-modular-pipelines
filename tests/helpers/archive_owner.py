"""Disposable native owner for archive liveness and interrupted-publication tests."""

import argparse
import asyncio
import os
import time
from pathlib import Path
from unittest.mock import patch

from core.experiments.archiver import ExperimentArchiver
from core.experiments.assembler import find_experiment
from core.experiments.state import RunnerStateStore
from core.modules.manager import ModuleManager
from core.primitives.json_files import write_json
from core.primitives.processes import process_identity
from core.storage.hash_db import HashDB
from core.storage.seaweed_client import SeaweedDB


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ownership", type=Path, required=True)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--filer-url", required=True)
    parser.add_argument(
        "--action", choices=("hold", "create", "install"), required=True
    )
    parser.add_argument("--phase", default="none")
    options = parser.parse_args()
    write_json(options.ownership, {"process": process_identity(os.getpid())})
    if options.action == "hold":
        while not options.ownership.with_suffix(".release").exists():
            time.sleep(0.025)
        return
    hashes = HashDB(options.project_root / "hashes.json")
    storage = SeaweedDB(options.filer_url, timeout=10)
    manager = ModuleManager(
        options.project_root / "modules", hashes, storage, options.project_root / "work"
    )
    archiver = ExperimentArchiver(
        options.project_root, manager, config_path=options.config
    )
    original_link, original_rename = os.link, Path.rename

    def crash(phase):
        if phase == options.phase:
            write_json(options.ownership.with_suffix(".phase.json"), {"phase": phase})
            os._exit(23)

    def link(source, target, *args, **kwargs):
        if Path(target) == options.archive:
            crash("before_archive")
        result = original_link(source, target, *args, **kwargs)
        if Path(target) == options.archive:
            crash("after_archive")
        return result

    def rename(source, target):
        kind = "bundle" if Path(target) == options.destination else "module"
        crash(f"before_{kind}")
        result = original_rename(source, target)
        crash(f"after_{kind}")
        return result

    try:
        with patch("os.link", new=link), patch.object(Path, "rename", new=rename):
            if options.action == "create":
                state = RunnerStateStore().load(
                    find_experiment(options.project_root, "archive-source")
                )
                asyncio.run(archiver.create(state, options.archive))
            else:
                asyncio.run(archiver.install(options.archive, options.destination))
    finally:
        storage.close()
        hashes.close_connection()


if __name__ == "__main__":
    main()
