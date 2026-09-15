"""Assembly and validation for the initial stage-only experiment runtime."""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
from pathlib import Path
from uuid import UUID, uuid4

import yaml

from core.logger_utils.events import copy_json_object, require_number, require_text
from core.modulemanager import ModuleManager
from core.runner_utils.state import JsonObject, RunnerState


def find_experiment(project_root: Path, experiment_id: str) -> Path:
    root = Path(project_root)
    if not root.is_absolute():
        raise ValueError("project_root must be absolute.")
    require_text(experiment_id, "experiment_id")
    registry_path = root / "experiments.json"
    if not registry_path.exists():
        raise FileNotFoundError(f"Unknown experiment: {experiment_id}")
    registry = copy_json_object(
        json.loads(registry_path.read_text(encoding="utf-8")), "registry"
    )
    folder = registry.get(experiment_id)
    if (
        not isinstance(folder, str)
        or Path(folder).name != folder
        or folder in (".", "..")
    ):
        raise FileNotFoundError(f"Unknown or invalid experiment: {experiment_id}")
    directory = (root / "experiments" / folder).resolve()
    if not directory.is_relative_to((root / "experiments").resolve()):
        raise ValueError("Experiment directory escapes the project.")
    if not directory.is_dir():
        raise FileNotFoundError(f"Experiment directory is missing: {directory}")
    return directory


class ExperimentAssembler:
    """Prepare checked local files; advanced rebuilds remain explicitly unavailable."""

    def __init__(self, project_root: Path, module_manager: ModuleManager) -> None:
        self._project_root = Path(project_root)
        if not self._project_root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._project_root = self._project_root.resolve()
        self._module_manager = module_manager

    def load_template(self, template_path: Path) -> tuple[str, JsonObject]:
        path = Path(template_path)
        if not path.is_absolute():
            raise ValueError("template_path must be absolute.")
        text = path.read_text(encoding="utf-8")
        template = copy_json_object(yaml.safe_load(text), "experiment template")
        fields = {
            "schema_version",
            "name",
            "cycles",
            "keep_attempts",
            "start_timeout",
            "runner_timeout_margin_seconds",
            "stages",
            "services",
            "resources",
            "unknown_state",
            "snapshots",
            "storage",
            "logging",
        }
        if template.keys() != fields:
            raise ValueError(
                f"Template fields: missing={fields - template.keys()}, unknown={template.keys() - fields}."
            )
        if (
            type(template["schema_version"]) is not int
            or template["schema_version"] != 1
        ):
            raise ValueError("Only template schema_version 1 is supported.")
        require_text(template["name"], "name")
        for key in ("cycles", "keep_attempts"):
            if type(template[key]) is not int or template[key] < 1:
                raise ValueError(f"{key} must be a positive integer.")
        for key in ("start_timeout", "runner_timeout_margin_seconds"):
            require_number(template[key], key)
        if template["start_timeout"] <= 0:
            raise ValueError("start_timeout must be positive.")
        if type(template["services"]) is not list or template["services"]:
            raise NotImplementedError(
                "The initial runtime supports stages without services."
            )
        snapshots = copy_json_object(template["snapshots"], "snapshots")
        if snapshots.keys() != {"mode", "keep"}:
            raise ValueError("snapshots requires mode and keep.")
        if snapshots["mode"] != "off":
            raise NotImplementedError(
                "Snapshots are not available in the initial runtime."
            )
        if type(snapshots["keep"]) is not int or snapshots["keep"] < 1:
            raise ValueError("snapshots.keep must be a positive integer.")
        storage = copy_json_object(template["storage"], "storage")
        if storage.keys() != {"min_snapshot_free_bytes"}:
            raise ValueError("storage requires min_snapshot_free_bytes.")
        if (
            type(storage["min_snapshot_free_bytes"]) is not int
            or storage["min_snapshot_free_bytes"] < 0
        ):
            raise ValueError("min_snapshot_free_bytes must be nonnegative.")
        logging = copy_json_object(template["logging"], "logging")
        if logging.keys() != {
            "busy_timeout_seconds",
            "max_event_bytes",
            "min_free_bytes",
            "filtered_refresh_interval_seconds",
        }:
            raise ValueError("All four configurable logging fields must be explicit.")
        for key in ("busy_timeout_seconds", "filtered_refresh_interval_seconds"):
            require_number(logging[key], key)
            if logging[key] <= 0:
                raise ValueError(f"logging.{key} must be positive.")
        if logging["busy_timeout_seconds"] > 60:
            raise ValueError("logging.busy_timeout_seconds must not exceed 60.")
        for key in ("max_event_bytes", "min_free_bytes"):
            value = logging[key]
            if key == "max_event_bytes" and value is None:
                continue
            if type(value) is not int or value < (1 if key == "max_event_bytes" else 0):
                raise ValueError(f"Invalid logging.{key}.")
        unknown = copy_json_object(template["unknown_state"], "unknown_state")
        if unknown.keys() != {
            "timeout_seconds",
            "on_timeout",
            "recovery_limit",
            "on_recovery_limit",
        }:
            raise ValueError("All unknown_state fields must be explicit.")
        require_number(unknown["timeout_seconds"], "unknown_state.timeout_seconds")
        if unknown["timeout_seconds"] <= 0:
            raise ValueError("unknown_state.timeout_seconds must be positive.")
        if unknown["on_timeout"] not in ("stop", "pause", "rerun", "skip"):
            raise ValueError("Invalid unknown_state.on_timeout.")
        if type(unknown["recovery_limit"]) is not int or unknown["recovery_limit"] < 0:
            raise ValueError("unknown_state.recovery_limit must be nonnegative.")
        if unknown["on_recovery_limit"] not in ("stop", "pause"):
            raise ValueError("Invalid unknown_state.on_recovery_limit.")
        stages = template["stages"]
        if type(stages) is not list or not stages:
            raise ValueError("stages must be a nonempty array.")
        seen = set()
        for stage in stages:
            stage = copy_json_object(stage, "stage")
            if stage.keys() - {
                "stage_id",
                "module",
                "settings",
                "timeout_seconds",
                "errors",
            }:
                raise ValueError("Unknown stage fields.")
            if not {"module", "settings", "timeout_seconds", "errors"} <= stage.keys():
                raise ValueError(
                    "A stage requires module, settings, timeout_seconds, and errors."
                )
            if "stage_id" in stage:
                stage_id = str(UUID(require_text(stage["stage_id"], "stage_id")))
                if stage_id in seen:
                    raise ValueError("Duplicate stage_id.")
                seen.add(stage_id)
            module = copy_json_object(stage["module"], "module")
            if module.keys() != {"name", "version", "hash"}:
                raise ValueError("module requires name/version/hash.")
            for key in ("name", "version"):
                value = require_text(module[key], f"module.{key}")
                if value in (".", "..") or any(c in value for c in '/\\:*?"<>|'):
                    raise ValueError(f"Unsafe module {key}.")
            digest = require_text(module["hash"], "module.hash")
            if len(digest) != 64 or any(
                c not in "0123456789abcdefABCDEF" for c in digest
            ):
                raise ValueError("module.hash must be SHA-256.")
            copy_json_object(stage["settings"], "stage settings")
            if stage["timeout_seconds"] is not None:
                require_number(stage["timeout_seconds"], "timeout_seconds")
                if stage["timeout_seconds"] <= 0:
                    raise ValueError("timeout_seconds must be positive or null.")
            errors = copy_json_object(stage["errors"], "errors")
            if errors.keys() != {"retries", "retry_delay_seconds", "on_exhausted"}:
                raise ValueError("All stage error policy fields must be explicit.")
            if type(errors["retries"]) is not int or errors["retries"] < 0:
                raise ValueError("errors.retries must be nonnegative.")
            require_number(errors["retry_delay_seconds"], "retry_delay_seconds")
            if errors["on_exhausted"] not in ("stop", "pause", "skip"):
                raise ValueError("Invalid errors.on_exhausted.")
        if type(template["resources"]) is not list:
            raise TypeError("resources must be an array.")
        resource_names = set()
        for resource in template["resources"]:
            if type(resource) is not dict or resource.keys() != {
                "name",
                "path",
                "hash",
            }:
                raise ValueError("A resource requires name/path/hash.")
            name = require_text(resource["name"], "resource.name")
            if (
                name in resource_names
                or name in (".", "..")
                or any(c in name for c in '/\\:*?"<>|')
            ):
                raise ValueError("Resource names must be unique safe path components.")
            resource_names.add(name)
            require_text(resource["path"], "resource.path")
            configured = Path(resource["path"])
            if not configured.is_absolute():
                if configured.drive or configured.root:
                    raise ValueError("Ambiguous resource path.")
                configured = path.parent / configured
            # Store resolved paths in the normalized document; preserve original YAML.
            resource["path"] = str(configured)
            digest = resource["hash"]
            if digest is not None and (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(c not in "0123456789abcdefABCDEF" for c in digest)
            ):
                raise ValueError("resource.hash must be SHA-256 or null.")
        return text, template

    def read_module(self, module_directory: Path) -> JsonObject:
        directory = Path(module_directory)
        if not directory.is_absolute():
            raise ValueError("module_directory must be absolute.")
        module = copy_json_object(
            yaml.safe_load((directory / "module.yaml").read_text(encoding="utf-8")),
            "module",
        )
        required = {
            "schema_version",
            "name",
            "version",
            "role",
            "implementation",
            "commands",
            "defaults",
        }
        if (
            module.keys() != required
            or type(module["schema_version"]) is not int
            or module["schema_version"] != 1
        ):
            raise ValueError("Invalid module schema.")
        if module["role"] != "stage" or module["implementation"] not in (
            "full",
            "action",
        ):
            raise NotImplementedError(
                "The initial runtime executes full/action stages."
            )
        commands = copy_json_object(module["commands"], "commands")
        expected = (
            {"start", "stop"} if module["implementation"] == "action" else {"start"}
        )
        if commands.keys() != expected:
            raise ValueError("Module commands do not match its implementation.")
        for argv in commands.values():
            if not isinstance(argv, list) or not argv:
                raise ValueError("Each module command must be a nonempty argv array.")
            for argument in argv:
                require_text(argument, "command argument")
        copy_json_object(module["defaults"], "module defaults")
        return module

    async def assemble(self, template_path: Path, experiment_id: str) -> RunnerState:
        require_text(experiment_id, "experiment_id")
        _, template = self.load_template(template_path)
        registry_path = self._project_root / "experiments.json"
        registry = {}
        if registry_path.exists():
            registry = copy_json_object(
                json.loads(registry_path.read_text(encoding="utf-8")), "registry"
            )
        if experiment_id in registry:
            raise FileExistsError(f"Experiment ID already exists: {experiment_id}")
        folder = str(uuid4())
        directory = self._project_root / "experiments" / folder
        directory.mkdir(parents=True, exist_ok=False)
        for stage in template["stages"]:
            stage.setdefault("stage_id", str(uuid4()))
        template_yaml = yaml.safe_dump(template, allow_unicode=True, sort_keys=False)
        state = RunnerState(
            experiment_id,
            directory,
            str(uuid4()),
            directory / "experiment.yaml",
            str(uuid4()),
            template_yaml,
            template,
            "paused",
        )
        copy_task = None
        temporary = None
        try:
            for name in (
                "modules",
                "module_data",
                "shared_settings",
                "shared_data/resources",
                "shared_artifacts",
                "journals",
                "runner/logging",
            ):
                (directory / name).mkdir(parents=True, exist_ok=True)
            copied = set()
            for stage in template["stages"]:
                module = stage["module"]
                key = (module["name"], module["version"])
                if key in copied:
                    continue
                source = self._project_root / "modules" / key[0] / key[1]
                target = directory / "modules" / key[0] / key[1]
                if (
                    source.is_symlink()
                    or source.is_junction()
                    or any(
                        item.is_symlink() or item.is_junction()
                        for item in source.rglob("*")
                    )
                ):
                    raise ValueError("Module code must not contain filesystem links.")
                definition = self.read_module(source)
                if (definition["name"], definition["version"]) != key:
                    raise ValueError("module.yaml identity differs from template.")
                copy_task = asyncio.create_task(
                    asyncio.to_thread(shutil.copytree, source, target)
                )
                await asyncio.shield(copy_task)
                self.check_module(state, stage)
                copied.add(key)
            for resource in template["resources"]:
                source = Path(resource["path"])
                target = directory / "shared_data" / "resources" / resource["name"]
                if source.is_dir():
                    copy_task = asyncio.create_task(
                        asyncio.to_thread(shutil.copytree, source, target)
                    )
                else:
                    copy_task = asyncio.create_task(
                        asyncio.to_thread(shutil.copy2, source, target)
                    )
                await asyncio.shield(copy_task)
            state.template_path.write_text(template_yaml, encoding="utf-8")
            registry[experiment_id] = folder
            temporary = registry_path.with_name(f".experiments-{uuid4()}.json")
            temporary.write_text(
                json.dumps(registry, ensure_ascii=False), encoding="utf-8"
            )
            temporary.replace(registry_path)
        except BaseException as error:
            # Cancelling an await does not stop the copying thread. Reap it before
            # removing its destination, so it cannot recreate a discarded build.
            if copy_task is not None:
                await asyncio.gather(copy_task, return_exceptions=True)
            # Only the freshly allocated, unregistered instance belongs to this operation.
            try:
                if directory.resolve().is_relative_to(
                    (self._project_root / "experiments").resolve()
                ):
                    await asyncio.to_thread(shutil.rmtree, directory)
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
            except OSError as cleanup_error:
                error.add_note(f"Build cleanup also failed: {cleanup_error}")
            raise
        return state

    async def rebuild(
        self, state: RunnerState, template_yaml: str, template: JsonObject
    ) -> None:
        raise NotImplementedError(
            "Rebuilding requires the snapshot phase of the runtime."
        )

    def check_modules(self, state: RunnerState) -> None:
        for definition in state.template["stages"]:
            self.check_module(state, definition)

    def check_module(self, state: RunnerState, definition: JsonObject) -> None:
        module = definition["module"]
        directory = (
            state.experiment_directory / "modules" / module["name"] / module["version"]
        )
        actual = self._module_manager.module_hash(
            module["name"], target_folder=directory
        )
        registered = self._module_manager.hash_db.get_module_hash(
            module["name"], module["version"]
        )
        if (
            not registered
            or actual.lower() != registered.lower()
            or actual.lower() != module["hash"].lower()
        ):
            raise ValueError(
                f"Module integrity check failed: {module['name']} / {module['version']}"
            )

    def check_resources(self, state: RunnerState) -> None:
        for resource in state.template["resources"]:
            if resource["hash"] is None:
                continue
            path = (
                state.experiment_directory
                / "shared_data"
                / "resources"
                / resource["name"]
            )
            if path.is_dir():
                actual = self._module_manager.module_hash(
                    resource["name"], target_folder=path
                )
            else:
                with path.open("rb") as stream:
                    actual = hashlib.file_digest(stream, "sha256").hexdigest()
            if actual.lower() != resource["hash"].lower():
                raise ValueError(f"Resource integrity check failed: {resource['name']}")
