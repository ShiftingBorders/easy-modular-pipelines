"""Launch only the dashboard, leaving the system runtime independently owned."""

import argparse
from pathlib import Path

from dashboard.application import create_app


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
    arguments = parser.parse_args()
    try:
        import uvicorn
    except ImportError:
        parser.exit(2, "Use: uv run --with uvicorn python -B -m dashboard\n")
    overrides = {}
    if arguments.project_root is not None:
        overrides["project_root"] = str(arguments.project_root.resolve())
    if arguments.system_api_url is not None:
        overrides["system_api_url"] = arguments.system_api_url
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
