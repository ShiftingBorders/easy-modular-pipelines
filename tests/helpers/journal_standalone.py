"""Short-lived application proving the standalone writer outlives its caller."""

import os
import sys
from pathlib import Path

from core.journal.logger import OperationLogger
from tests.helpers.logging_process import announce


def main() -> None:
    _mode, configuration = sys.argv[1:]
    announce({"pid": os.getpid()})
    with OperationLogger(Path(configuration)) as logger:
        identifier = logger.record_event("single_writer.independent")
        status = logger._store.get_status()
        announce({"event_id": identifier, "writer": status["process"]})


if __name__ == "__main__":
    main()
