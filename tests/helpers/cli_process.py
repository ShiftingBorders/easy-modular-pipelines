"""Run only the real HTTP CLI, recording ownership for isolated interruption tests."""

import _thread
import argparse
import os
import sys
import threading
import time
from pathlib import Path

import cli
from core.runner_utils.runtimeio import process_identity, write_json


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--ownership", type=Path, required=True)
    options, arguments = parser.parse_known_args()
    write_json(options.ownership, {"process": process_identity(os.getpid())})

    def interrupt_on_request():
        trigger = options.ownership.with_suffix(".interrupt")
        while True:
            if trigger.exists():
                trigger.unlink()
                _thread.interrupt_main()
            time.sleep(0.025)

    threading.Thread(target=interrupt_on_request, daemon=True).start()
    sys.argv = ["cli.py", *arguments]
    cli.main()


if __name__ == "__main__":
    main()
