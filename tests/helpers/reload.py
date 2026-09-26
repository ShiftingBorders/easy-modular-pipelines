"""Fixtures and journal observations for the approved reload protocols RT01-RT20."""

import copy
import json
import shutil

import yaml

from tests.helpers.snapshots import SnapshotWorkspace


def workspace(*, services=False, cycles=2):
    result = SnapshotWorkspace(services=services, cycles=cycles)
    result.stages.append(result.files.stage(settings={"label": "D"}))
    result.template["stages"] = result.stages
    result.template["keep_attempts"] = 5
    result.extra_stage = result.files.stage(settings={"label": "X"})
    return result


def version(work, definition, number, *, name=None):
    """Install a registered fixture version before launching the experiment."""
    result = copy.deepcopy(definition)
    reference = result["module"]
    name = name or reference["name"]
    source = work.root / "modules" / reference["name"] / reference["version"]
    target = work.root / "modules" / name / number
    shutil.copytree(source, target)
    manifest = yaml.safe_load((target / "module.yaml").read_text(encoding="utf-8"))
    manifest.update(name=name, version=number)
    (target / "module.yaml").write_text(yaml.safe_dump(manifest), encoding="utf-8")
    digest = work.manager.module_hash(name, target_folder=target)
    work.files.hashes.add_module_hash(name, number, digest)
    result["module"] = {"name": name, "version": number, "hash": digest}
    return result


def events(runner, kind=None):
    result = []
    checkpoint = boundary = None
    while True:
        page = runner._journal.client.read_events(checkpoint, limit=1000)
        if boundary is None:
            boundary = page["boundary"]["cursor"]
        result.extend(
            row["event"]
            for row in page["events"]
            if row["cursor"] <= boundary
            and (kind is None or row["event"]["event_type"] == kind)
        )
        checkpoint = page["checkpoint"]
        if not page["has_more"] or checkpoint["cursor"] >= boundary:
            return result


def trace(runner):
    path = runner._state.experiment_directory / "shared_data/trace.jsonl"
    return (
        []
        if not path.exists()
        else [
            json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
        ]
    )


def candidate(work):
    return copy.deepcopy(work.runner._state.template)
