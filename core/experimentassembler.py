"""Assembly and validation of stage and service experiment definitions."""

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
from core.modulemanifest import read_module_manifest
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
    """Prepare checked local files for initial assembly and protected rebuilds."""

    def __init__(self, project_root: Path, module_manager: ModuleManager) -> None:
        self._project_root = Path(project_root)
        if not self._project_root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._project_root = self._project_root.resolve()
        self._module_manager = module_manager

    def load_template(
        self, template_path: Path, *, template_yaml: str | None = None
    ) -> tuple[str, JsonObject]:
        path = Path(template_path)
        if not path.is_absolute():
            raise ValueError("template_path must be absolute.")
        text = (
            path.read_text(encoding="utf-8")
            if template_yaml is None
            else require_text(template_yaml, "template YAML")
        )
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
            or template["schema_version"] != 2
        ):
            raise ValueError("Only template schema_version 2 is supported.")
        require_text(template["name"], "name")
        for key in ("cycles", "keep_attempts"):
            value = template[key]
            if type(value) is not int or value < 1:
                raise ValueError(f"{key} must be a positive integer.")
        start_timeout = require_number(template["start_timeout"], "start_timeout")
        require_number(
            template["runner_timeout_margin_seconds"], "runner_timeout_margin_seconds"
        )
        if start_timeout <= 0:
            raise ValueError("start_timeout must be positive.")
        services = template["services"]
        if type(services) is not list:
            raise TypeError("services must be an array.")
        snapshots = copy_json_object(template["snapshots"], "snapshots")
        if snapshots.keys() != {"mode", "keep"}:
            raise ValueError("snapshots requires mode and keep.")
        if snapshots["mode"] not in ("off", "after_stage", "after_epoch"):
            raise ValueError("snapshots.mode must be off, after_stage, or after_epoch.")
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
            number = require_number(logging[key], key)
            if number <= 0:
                raise ValueError(f"logging.{key} must be positive.")
            if key == "busy_timeout_seconds" and number > 60:
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
        if (
            require_number(unknown["timeout_seconds"], "unknown_state.timeout_seconds")
            <= 0
        ):
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
        definitions = [("stage", item) for item in stages]
        definitions.extend(("service", item) for item in services)
        socket_fields = {
            "heartbeat",
            "command_timeout_seconds",
            "on_command_timeout",
            "state_required",
            "errors",
        }
        for role, definition in definitions:
            if type(definition) is not dict:
                raise TypeError(f"{role} must be a JSON object.")
            identity_key = f"{role}_id"
            required = {"module", "settings"}
            if role == "stage":
                required.update({"timeout_seconds", "errors"})
                if "service_id" in definition:
                    required.remove("module")
                    required.add("service_id")
                    definition["service_id"] = str(
                        UUID(require_text(definition["service_id"], "service_id"))
                    )
            else:
                required.update(socket_fields)
            if definition.keys() - {identity_key} != required:
                raise ValueError(
                    f"Invalid {role} fields; required: {sorted(required)}."
                )
            if identity_key in definition:
                identifier = str(
                    UUID(require_text(definition[identity_key], identity_key))
                )
                if identifier in seen:
                    raise ValueError("Duplicate stage/service definition ID.")
                seen.add(identifier)
                definition[identity_key] = identifier
            if "module" in definition:
                module = copy_json_object(definition["module"], "module")
                if module.keys() != {"name", "version", "hash"}:
                    raise ValueError("module requires name/version/hash.")
                for key in ("name", "version"):
                    value = require_text(module[key], f"module.{key}").strip()
                    if value in (".", "..") or any(c in value for c in '/\\:*?"<>|'):
                        raise ValueError(f"Unsafe module {key}.")
                    module[key] = value
                digest = require_text(module["hash"], "module.hash")
                if len(digest) != 64 or any(
                    c not in "0123456789abcdefABCDEF" for c in digest
                ):
                    raise ValueError("module.hash must be SHA-256.")
                definition["module"] = module
            copy_json_object(definition["settings"], f"{role} settings")
            if (
                role == "stage"
                and definition["timeout_seconds"] is not None
                and require_number(definition["timeout_seconds"], "timeout_seconds")
                <= 0
            ):
                raise ValueError("timeout_seconds must be positive or null.")
            if role == "service":
                self.validate_service_definition(definition)
            self.validate_errors(definition["errors"])
        service_ids = {item["service_id"] for item in services if "service_id" in item}
        for definition in stages:
            if (
                "service_id" in definition
                and definition["service_id"] not in service_ids
            ):
                raise ValueError(
                    "A DAG service reference requires an explicit service_id in services."
                )
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
            configured = Path(require_text(resource["path"], "resource.path"))
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

    async def validate_template(self, template_path: Path) -> JsonObject:
        """Check structure and registered module references without assembling a run."""
        try:
            # File reads and YAML parsing must not stall the active experiment.
            # Keep module inspection below on the thread that owns HashDB.
            _, template = await asyncio.to_thread(self.load_template, template_path)
        except yaml.YAMLError as error:
            raise ValueError(f"Invalid template YAML: {error}") from error
        warnings = [
            f"services[{index}] has no service_id; one will be generated during assembly."
            for index, service in enumerate(template["services"])
            if "service_id" not in service
        ]
        references = {}
        for role in ("stage", "service"):
            for definition in template[f"{role}s"]:
                reference = self.module_reference(template, definition)
                key = (reference["name"], reference["version"])
                if key not in references:
                    references[key] = await self._module_manager.inspect_module(*key)
                registration = references[key]
                if registration["module"]["hash"].lower() != reference["hash"].lower():
                    raise ValueError(f"Registered hash differs from template: {key}.")
                if not registration["archive_available"]:
                    raise FileNotFoundError(
                        f"Registered module archive is missing: {key}."
                    )
        return {
            "valid": True,
            "warnings": warnings,
            "name": template["name"],
            "template_path": str(template_path),
            "modules": [entry["module"] for entry in references.values()],
            "scope": "structure_and_registered_references",
            "archive_integrity_checked": False,
        }

    def module_reference(
        self, template: JsonObject, definition: JsonObject
    ) -> JsonObject:
        if "module" in definition:
            return copy_json_object(definition["module"], "module")
        service_id = definition.get("service_id")
        for service in template["services"]:
            if service_id is not None and service.get("service_id") == service_id:
                return copy_json_object(service["module"], "service module")
        raise ValueError(f"Unknown service reference: {service_id}")

    def validate_errors(self, errors: JsonObject) -> None:
        errors = copy_json_object(errors, "errors")
        if errors.keys() != {"retries", "retry_delay_seconds", "on_exhausted"}:
            raise ValueError("All error policy fields must be explicit.")
        if type(errors["retries"]) is not int or errors["retries"] < 0:
            raise ValueError("errors.retries must be nonnegative.")
        require_number(errors["retry_delay_seconds"], "retry_delay_seconds")
        if errors["on_exhausted"] not in ("stop", "pause", "skip"):
            raise ValueError("Invalid errors.on_exhausted.")

    def validate_service_definition(self, definition: JsonObject) -> None:
        required = {
            "module",
            "settings",
            "heartbeat",
            "command_timeout_seconds",
            "on_command_timeout",
            "state_required",
            "errors",
        }
        if definition.keys() - {"service_id"} != required:
            raise ValueError(
                "A service requires explicit settings, heartbeat and policies."
            )
        copy_json_object(definition["settings"], "settings")
        heartbeat = copy_json_object(definition["heartbeat"], "heartbeat")
        if heartbeat.keys() != {"interval_seconds", "grace_seconds"}:
            raise ValueError("heartbeat requires interval_seconds and grace_seconds.")
        for name, value in heartbeat.items():
            if require_number(value, name) <= 0:
                raise ValueError(f"{name} must be positive.")
        if (
            require_number(definition["command_timeout_seconds"], "command timeout")
            <= 0
        ):
            raise ValueError("command_timeout_seconds must be positive.")
        if definition["on_command_timeout"] not in ("pause", "restart", "stop"):
            raise ValueError("Invalid on_command_timeout.")
        if type(definition["state_required"]) is not bool:
            raise TypeError("state_required must be a boolean.")
        self.validate_errors(definition["errors"])

    def read_module(self, module_directory: Path) -> JsonObject:
        return read_module_manifest(module_directory)

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
        # load_template has validated every definition; preserve the same objects
        # so generated IDs also appear in the template written to the experiment.
        definitions: list[tuple[str, JsonObject]] = []
        for role in ("stage", "service"):
            entries = template[f"{role}s"]
            if not isinstance(entries, list):
                raise TypeError(f"{role}s must be an array.")
            for definition in entries:
                if not isinstance(definition, dict):
                    raise TypeError(f"{role} must be a JSON object.")
                definition.setdefault(f"{role}_id", str(uuid4()))
                definitions.append((role, definition))
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
            copied = {}
            for role, item in definitions:
                if role == "stage" and "service_id" in item:
                    continue
                module = copy_json_object(item["module"], "module")
                key = (
                    require_text(module["name"], "module.name"),
                    require_text(module["version"], "module.version"),
                )
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
                definition = copied.get(key) or self.read_module(source)
                if (definition["name"], definition["version"]) != key:
                    raise ValueError("module.yaml identity differs from template.")
                if definition["role"] != role:
                    raise ValueError(f"Module {key} does not have role={role}.")
                if key not in copied:
                    copy_task = asyncio.create_task(
                        asyncio.to_thread(shutil.copytree, source, target)
                    )
                    await asyncio.shield(copy_task)
                    copied[key] = definition
                # Every reference must match, including repeated uses of shared code.
                self.check_module(state, item)
            resources = template["resources"]
            if not isinstance(resources, list):
                raise TypeError("resources must be an array.")
            for resource in resources:
                if not isinstance(resource, dict):
                    raise TypeError("resource must be a JSON object.")
                source = Path(require_text(resource["path"], "resource.path"))
                resource_name = require_text(resource["name"], "resource.name")
                target = directory / "shared_data" / "resources" / resource_name
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
        self,
        state: RunnerState,
        template_yaml: str,
        template: JsonObject,
        *,
        workspace: Path,
        prepare_only: bool = False,
    ) -> None:
        """Prepare checked additions, then publish without replacing immutable code."""
        root = state.experiment_directory.resolve()
        workspace = Path(workspace)
        if (
            not workspace.is_absolute()
            or not workspace.resolve().is_relative_to(root / "runner/rebuilds")
            or workspace.is_symlink()
            or workspace.is_junction()
        ):
            raise ValueError("Rebuild workspace must be inside runner/rebuilds.")
        candidate = RunnerState(
            state.experiment_id,
            root,
            state.run_id,
            state.template_path,
            state.template_revision_id,
            template_yaml,
            template,
            "paused",
        )
        staged = RunnerState(
            state.experiment_id,
            workspace,
            state.run_id,
            workspace / "experiment.yaml",
            state.template_revision_id,
            template_yaml,
            template,
            "paused",
        )
        if prepare_only:
            workspace.mkdir(parents=True, exist_ok=False)
        elif state.pending_rebuild is None:
            raise RuntimeError("Publication requires a recorded rebuild intent.")
        seen = set()
        for role in ("stage", "service"):
            for definition in template[f"{role}s"]:
                if role == "stage" and "service_id" in definition:
                    continue
                reference = definition["module"]
                key = (reference["name"], reference["version"])
                target = root / "modules" / key[0] / key[1]
                prepared = workspace / "modules" / key[0] / key[1]
                if prepare_only:
                    source = (
                        target
                        if target.exists()
                        else (self._project_root / "modules" / key[0] / key[1])
                    )
                    if (
                        not source.resolve().is_relative_to(
                            root if target.exists() else self._project_root
                        )
                        or source.is_symlink()
                        or source.is_junction()
                        or any(
                            p.is_symlink() or p.is_junction() for p in source.rglob("*")
                        )
                    ):
                        raise ValueError(
                            "Module code must not contain filesystem links."
                        )
                    manifest = self.read_module(source)
                    if (manifest["name"], manifest["version"], manifest["role"]) != (
                        *key,
                        role,
                    ):
                        raise ValueError(
                            f"Module identity/role differs from template: {key}."
                        )
                    if not target.exists() and key not in seen:
                        prepared.parent.mkdir(parents=True, exist_ok=True)
                        copying = asyncio.create_task(
                            asyncio.to_thread(shutil.copytree, source, prepared)
                        )
                        try:
                            await asyncio.shield(copying)
                        finally:
                            await asyncio.gather(copying, return_exceptions=True)
                    self.check_module(
                        candidate if target.exists() else staged, definition
                    )
                else:
                    if not target.exists():
                        self.check_module(staged, definition)
                        if not target.parent.resolve().is_relative_to(root):
                            raise ValueError(
                                "Module destination escapes the experiment."
                            )
                        target.parent.mkdir(parents=True, exist_ok=True)
                        prepared.replace(target)
                    self.check_module(candidate, definition)
                seen.add(key)
        checking_resources = asyncio.create_task(
            asyncio.to_thread(self.check_resources, candidate)
        )
        try:
            await asyncio.shield(checking_resources)
        finally:
            # Cancellation must not leave a reader running during restoration.
            await asyncio.gather(checking_resources, return_exceptions=True)
        if prepare_only:
            (workspace / "experiment.yaml").write_text(template_yaml, encoding="utf-8")
        else:
            # The durable pending_rebuild record covers the multi-file publication.
            (workspace / "experiment.yaml").replace(state.template_path)

    def check_modules(self, state: RunnerState) -> None:
        for role in ("stage", "service"):
            definitions = state.template[f"{role}s"]
            if not isinstance(definitions, list):
                raise TypeError(f"{role}s must be an array.")
            for definition in definitions:
                if not isinstance(definition, dict):
                    raise TypeError(f"{role} must be a JSON object.")
                self.check_module(state, definition)

    def check_module(self, state: RunnerState, definition: JsonObject) -> None:
        module = self.module_reference(state.template, definition)
        name = require_text(module["name"], "module.name")
        version = require_text(module["version"], "module.version")
        expected_hash = require_text(module["hash"], "module.hash")
        for key, component in (("name", name), ("version", version)):
            if component in (".", "..") or any(
                character in component for character in '/\\:*?"<>|'
            ):
                raise ValueError(f"Unsafe module {key}.")
        directory = state.experiment_directory / "modules" / name / version
        if not directory.resolve().is_relative_to(state.experiment_directory.resolve()):
            raise ValueError("Module code escapes the experiment.")
        actual = self._module_manager.module_hash(name, target_folder=directory)
        registered = self._module_manager.hash_db.get_module_hash(name, version)
        if (
            not registered
            or actual.lower() != registered.lower()
            or actual.lower() != expected_hash.lower()
        ):
            raise ValueError(f"Module integrity check failed: {name} / {version}")

    def check_resources(self, state: RunnerState) -> None:
        resources = state.template["resources"]
        if not isinstance(resources, list):
            raise TypeError("resources must be an array.")
        for resource in resources:
            if not isinstance(resource, dict):
                raise TypeError("resource must be a JSON object.")
            if resource["hash"] is None:
                continue
            name = require_text(resource["name"], "resource.name")
            expected_hash = require_text(resource["hash"], "resource.hash")
            path = state.experiment_directory / "shared_data" / "resources" / name
            if path.is_dir():
                actual = self._module_manager.module_hash(name, target_folder=path)
            else:
                with path.open("rb") as stream:
                    actual = hashlib.file_digest(stream, "sha256").hexdigest()
            if actual.lower() != expected_hash.lower():
                raise ValueError(f"Resource integrity check failed: {name}")
