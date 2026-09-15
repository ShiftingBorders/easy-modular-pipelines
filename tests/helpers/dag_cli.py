"""Run the real CLI with process-ownership records for approved E cleanup checks."""

import argparse
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import runner
from core.runner_utils.runtimeio import process_identity
from core.seaweed import SeaweedDB


def main() -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--ownership", type=Path, required=True)
    parser.add_argument("--audit-seaweed", action="store_true")
    options, arguments = parser.parse_known_args()
    records = {"cli": process_identity(os.getpid())}
    options.ownership.write_text(json.dumps(records), encoding="utf-8")
    original_start, original_stop = (
        runner.SeaweedProcess.start,
        runner.SeaweedProcess.stop,
    )

    def start(service):
        original_start(service)
        records["seaweed"] = process_identity(service._process.pid)
        records["project_root"] = str(service.volume_path.parent)
        records["filer_url"] = service.filer_url
        options.ownership.write_text(json.dumps(records), encoding="utf-8")

    def stop(service):
        if service._process is not None and service.filer_url:
            database = SeaweedDB(service.filer_url)
            try:
                records["package_registered"] = database.check_module_stored(
                    "counter", "1.0"
                )
            finally:
                database.close()
                options.ownership.write_text(json.dumps(records), encoding="utf-8")
        original_stop(service)

    sys.argv = ["runner.py", *arguments]
    if options.audit_seaweed:
        with (
            patch.object(runner.SeaweedProcess, "start", start),
            patch.object(runner.SeaweedProcess, "stop", stop),
        ):
            runner.main()
    else:
        runner.main()


if __name__ == "__main__":
    main()
