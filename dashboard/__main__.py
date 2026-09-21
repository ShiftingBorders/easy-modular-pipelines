"""Launch only the dashboard, leaving the system runtime independently owned."""

import argparse
import json
import multiprocessing
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path

from dashboard.config import load_settings
from dashboard.journals import LocalJournals, cache_experiment


def precache(settings: dict) -> int:
    """Build a fixed registry snapshot to finite source boundaries, then exit."""
    if settings["project_root"] is None:
        raise ValueError(
            "Precache requires project_root in the configuration or --project-root."
        )
    journals = LocalJournals(settings)
    try:
        registry = journals.registry()
    finally:
        journals.close()
    pending = deque((identifier, None) for identifier in registry)
    running, progress = {}, {}
    completed, failed = 0, 0
    workers = settings["cache_workers"]
    with ProcessPoolExecutor(
        max_workers=workers, mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        while pending or running:
            while pending and len(running) < workers:
                identifier, target = pending.popleft()
                future = pool.submit(cache_experiment, settings, identifier, target)
                running[future] = (identifier, target)
            finished, _ = wait(running, return_when=FIRST_COMPLETED)
            for future in finished:
                identifier, target = running.pop(future)
                result = future.result()
                error = result.get("error")
                if error and error["code"] == "cache_busy":
                    pending.append((identifier, target))
                    time.sleep(0.1)
                    continue
                if error:
                    failed += 1
                    print(json.dumps(result, ensure_ascii=False), flush=True)
                    continue
                marker = (result["cached_through"], result["complete"])
                if progress.get(identifier) != marker:
                    print(json.dumps(result, ensure_ascii=False), flush=True)
                    progress[identifier] = marker
                if result["complete"]:
                    completed += 1
                else:
                    pending.append((identifier, result["target_boundary"]))
    print(
        json.dumps({"mode": "precache", "completed": completed, "failed": failed}),
        flush=True,
    )
    return int(failed > 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="EMP observability dashboard")
    parser.add_argument(
        "--config", type=Path, default=Path(__file__).with_name("settings.json")
    )
    parser.add_argument("--host", help="Override the dashboard listening address.")
    parser.add_argument(
        "--project-root",
        type=Path,
        help="Local project containing experiments.json and journals.",
    )
    parser.add_argument(
        "--system-api-url",
        help="Base URL of the system server, for example http://127.0.0.1:8000/api/.",
    )
    parser.add_argument(
        "--port", type=int, help="Override the dashboard listening port."
    )
    parser.add_argument(
        "--mode",
        choices=("serve", "precache"),
        default="serve",
        help="Serve the UI, or build journal caches and exit without HTTP.",
    )
    parser.add_argument(
        "--cache-workers",
        type=int,
        help="Number of independent cache processes (1-32; default 2).",
    )
    arguments = parser.parse_args()
    overrides = {}
    if arguments.project_root is not None:
        overrides["project_root"] = str(arguments.project_root.resolve())
    if arguments.system_api_url is not None:
        overrides["system_api_url"] = arguments.system_api_url
    if arguments.cache_workers is not None:
        overrides["cache_workers"] = arguments.cache_workers
    if arguments.mode == "precache":
        try:
            settings = load_settings(arguments.config.resolve(), overrides)
            result = precache(settings)
        except KeyboardInterrupt:
            parser.exit(
                130, "Precache interrupted; committed checkpoints are retained.\n"
            )
        except (OSError, TypeError, ValueError, RuntimeError) as error:
            parser.exit(2, f"Precache failed: {error}\n")
        parser.exit(result)
    from dashboard.application import create_app

    try:
        import uvicorn
    except ImportError:
        parser.exit(2, "Use: uv run --with uvicorn python -B -m dashboard\n")
    app = create_app(arguments.config.resolve(), overrides=overrides)
    settings = app.state.settings
    if arguments.port is not None and not 1 <= arguments.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    uvicorn.run(
        app,
        host=arguments.host or settings["host"],
        port=arguments.port or settings["port"],
        workers=1,
        access_log=False,
    )


if __name__ == "__main__":
    main()
