"""Finite example commands which prepare and remove an owned readiness marker."""

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop"))
    parser.add_argument("--runtime-directory", type=Path, required=True)
    args = parser.parse_args()
    if not args.runtime_directory.is_absolute():
        raise ValueError("The runtime directory must be absolute.")
    marker = args.runtime_directory / "ready.txt"
    if args.action == "start":
        args.runtime_directory.mkdir(parents=True, exist_ok=True)
        marker.write_text("ready", encoding="utf-8")
    else:
        marker.unlink(missing_ok=True)
    print(f"{args.action} completed", flush=True)


if __name__ == "__main__":
    main()
