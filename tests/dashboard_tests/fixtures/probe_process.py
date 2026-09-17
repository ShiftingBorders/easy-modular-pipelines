"""Child-process fixture for the approved timeout, crash and shutdown scenarios."""

import json
import os
import sys
import time
from pathlib import Path

mode, marker = sys.argv[1:]
Path(marker).write_text(str(os.getpid()), encoding="utf-8")
if mode == "crash":
    os._exit(7)
if mode == "hang":
    time.sleep(60)
else:
    print(
        json.dumps(
            {"status": "reply", "reason": None, "rtt_ms": 1, "address": "127.0.0.1"}
        ),
        flush=True,
    )
