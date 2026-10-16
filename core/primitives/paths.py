"""Locations shipped with the framework, independent of the caller's cwd."""

from pathlib import Path


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def seaweed_executable(name: str) -> Path:
    return repository_root() / "core" / "storage" / "seaweedfs" / name
