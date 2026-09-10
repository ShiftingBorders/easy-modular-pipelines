from enum import Enum


class SeaweedState(Enum): 
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"

DEFAULT_LAUNCH_ARGS = {
            "ip": "127.0.0.1",
            "ip.bind": "127.0.0.1",
            "master.port": 9333,
            "volume.port": 8080,
            "filer": "true",
            "filer.port": 8888,
            "master.defaultReplication": "000",
            "master.telemetry": "false",
        }
DEFAULT_PROCESS_ARGS = {
            "start_stop_sec": 60,
            "max_archive_gb": 5,
        }