"""Assembly and validation of stage and service experiment definitions."""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path
from threading import Event
from uuid import uuid4

import yaml

from core.experiments.state import RunnerState
from core.experiments.template_validation import (
    _assign_definition_ids,
    _resolve_template_paths,
    _template_document,
)
from core.models.experiment_registry import RegistryEntry
from core.models.experiment_template import (
    ErrorPolicy,
    ExperimentTemplate,
    ModuleReference,
    ResourceDefinition,
    ServiceCallDefinition,
    ServiceDefinition,
    StageDefinition,
)
from core.models.module_manifest import ModuleManifest
from core.modules.hashing import _hash_file
from core.modules.manager import ModuleManager
from core.modules.manifest import _read_module_manifest, read_module_manifest
from core.primitives.json_values import (
    JsonObject,
    copy_json_object,
    require_text,
)
from core.primitives.tasks import _await_read_task


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
    try:
        folder = RegistryEntry.model_validate(
            {"folder": registry.get(experiment_id)}
        ).folder
    except (ValueError, TypeError) as error:
        raise FileNotFoundError(
            f"Unknown or invalid experiment: {experiment_id}"
        ) from error
    directory = (root / "experiments" / folder).resolve()
    if not directory.is_relative_to((root / "experiments").resolve()):
        raise ValueError("Experiment directory escapes the project.")
    if not directory.is_dir():
        raise FileNotFoundError(f"Experiment directory is missing: {directory}")
    return directory


class ExperimentAssembler:
    """Prepare checked local files for initial assembly and protected rebuilds."""

    def __init__(self, project_root: Path, module_manager: ModuleManager) -> None:
        """Bind the caller-owned module manager and resolve an absolute project root.

        Args:
            project_root: Absolute root of the caller's project.
            module_manager: Caller-owned module manager providing hash registration
                and archive storage.
        """
        self._project_root = Path(project_root)
        if not self._project_root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._project_root = self._project_root.resolve()
        self._module_manager = module_manager

    def load_template(
        self,
        template_path: Path,
        *,
        template_yaml: str | None = None,
    ) -> tuple[str, JsonObject]:
        """Load template YAML and return its normalized public JSON representation.

        Args:
            template_path: Absolute template path, also the base for relative resource paths.
            template_yaml: Optional YAML text replacing file reading.

        Returns:
            Original YAML text and validated template document with resolved paths.
        """
        text, template = self._load_template(template_path, template_yaml=template_yaml)
        return text, _template_document(template, Path(template_path))

    def _load_template(
        self,
        template_path: Path,
        *,
        template_yaml: str | None = None,
    ) -> tuple[str, ExperimentTemplate]:
        """Return YAML text and a validated template without resolving resource paths.

        Args:
            template_path: Absolute template path; relative resources resolve from
                its parent directory.
            template_yaml: Applied or candidate template YAML retained for audit and
                publication.

        Returns:
            YAML text and a validated template without resolving resource paths.
        """
        path = Path(template_path)
        if not path.is_absolute():
            raise ValueError("template_path must be absolute.")
        text = (
            path.read_text(encoding="utf-8")
            if template_yaml is None
            else require_text(template_yaml, "template YAML")
        )
        return text, ExperimentTemplate.model_validate(yaml.safe_load(text))

    async def validate_template(self, template_path: Path) -> JsonObject:
        """Check structure and registered module references without assembling a run.

        Args:
            template_path: Absolute template path; relative resources resolve from
                its parent directory.

        Returns:
            Validation metadata, normalized module references, and warnings; this
            does not create or run an experiment.

        Raises:
            ValueError: YAML, template structure, or registered module hash is
                invalid.
            FileNotFoundError: A referenced module archive or template is
                unavailable.
        """
        try:
            # File reads and YAML parsing must not stall the active experiment.
            # Keep module inspection below on the thread that owns HashDB.
            _, document = await asyncio.to_thread(self.load_template, template_path)
            # The compatible loader returns JSON and may be overridden by callers.
            template = await asyncio.to_thread(
                ExperimentTemplate.model_validate, document
            )
        except yaml.YAMLError as error:
            raise ValueError(f"Invalid template YAML: {error}") from error
        warnings = [
            f"services[{index}] has no service_id; one will be generated during assembly."
            for index, service in enumerate(template.services)
            if service.service_id is None
        ]
        references = {}
        for definition in (*template.stages, *template.services):
            reference = template.module_reference(definition)
            key = (reference.name, reference.version)
            if key not in references:
                references[key] = await self._module_manager.inspect_module(*key)
            registration = references[key]
            if registration["module"]["hash"].lower() != reference.hash.lower():
                raise ValueError(f"Registered hash differs from template: {key}.")
            if not registration["archive_available"]:
                raise FileNotFoundError(f"Registered module archive is missing: {key}.")
        return {
            "valid": True,
            "warnings": warnings,
            "name": template.name,
            "template_path": str(template_path),
            "modules": [entry["module"] for entry in references.values()],
            "scope": "structure_and_registered_references",
            "archive_integrity_checked": False,
        }

    def module_reference(
        self,
        template: ExperimentTemplate | JsonObject,
        definition: StageDefinition
        | ServiceCallDefinition
        | ServiceDefinition
        | JsonObject,
    ) -> JsonObject:
        """Return a detached module reference for a stage, service, or service call.

        Args:
            template: Template containing service declarations.
            definition: Definition with a direct module or a service reference.

        Returns:
            Module name, version, and content hash.

        Raises:
            ValueError: A service reference cannot be resolved.
        """
        if isinstance(template, ExperimentTemplate) and isinstance(
            definition, (StageDefinition, ServiceCallDefinition, ServiceDefinition)
        ):
            return self._module_reference(template, definition).model_dump(
                exclude_unset=True
            )
        if not isinstance(definition, dict):
            definition = definition.model_dump(exclude_unset=True)
        if "module" in definition:
            return copy_json_object(definition["module"], "module")
        service_id = definition.get("service_id")
        if isinstance(template, ExperimentTemplate):
            for service in template.services:
                if service_id is not None and service.service_id == service_id:
                    return service.module.model_dump(exclude_unset=True)
            raise ValueError(f"Unknown service reference: {service_id}")
        for service in template["services"]:
            if service_id is not None and service.get("service_id") == service_id:
                return copy_json_object(service["module"], "service module")
        raise ValueError(f"Unknown service reference: {service_id}")

    def validate_errors(self, errors: JsonObject) -> None:
        """Validate a detached retry/exhaustion policy, raising on invalid fields.

        Args:
            errors: Detached retry/exhaustion policy to validate before use.
        """
        ErrorPolicy.model_validate(copy_json_object(errors, "errors"))

    def validate_service_definition(self, definition: JsonObject) -> None:
        """Validate a detached service definition before runtime preparation.

        Args:
            definition: Validated stage/service definition with assigned stable
                identity.
        """
        ServiceDefinition.model_validate(
            copy_json_object(definition, "service definition")
        )

    def _module_reference(
        self,
        template: ExperimentTemplate,
        definition: StageDefinition | ServiceCallDefinition | ServiceDefinition,
    ) -> ModuleReference:
        """Resolve a typed definition's module, rejecting an unknown service ID.

        Args:
            template: Validated experiment template supplying definitions and
                policies.
            definition: Validated stage/service definition with assigned stable
                identity.

        Returns:
            Direct module reference or the declared service's reference; an unknown
            service ID raises ValueError.
        """
        if not isinstance(definition, ServiceCallDefinition):
            return definition.module
        for service in template.services:
            if service.service_id == definition.service_id:
                return service.module
        raise ValueError(f"Unknown service reference: {definition.service_id}")

    def read_module(self, module_directory: Path) -> JsonObject:
        """Read and validate module.yaml from the supplied module code directory.

        Args:
            module_directory: Module code directory containing module.yaml.

        Returns:
            Validated manifest JSON from the selected module directory.
        """
        return read_module_manifest(module_directory)

    async def assemble(self, template_path: Path, experiment_id: str) -> RunnerState:
        """Validate a template and assemble/register a new experiment directory.

        Args:
            template_path: Absolute YAML path; relative resources use its parent directory.
            experiment_id: New nonempty experiment identifier.

        Returns:
            Paused initial runner state with assigned definition IDs and local inputs.

        Raises:
            FileExistsError: The experiment ID is already registered.
            ValueError: Template, module identity, or copied module integrity is invalid.
        """
        require_text(experiment_id, "experiment_id")
        _, validated = await asyncio.to_thread(self._load_template, template_path)
        return await self._assemble(Path(template_path), experiment_id, validated)

    async def _assemble(
        self,
        template_path: Path,
        experiment_id: str,
        validated: ExperimentTemplate,
    ) -> RunnerState:
        """Allocate an experiment, copy inputs, and publish its registry entry after success.

        Creates a fresh unregistered directory, copies inputs, and publishes the
        registry entry last. Failure cleans only that newly allocated directory
        after copying workers have finished.

        Args:
            template_path: Absolute template path; relative resources resolve from
                its parent directory.
            experiment_id: Registered experiment identifier used to select saved
                state or history.
            validated: Parsed template before resource-path resolution and generated
                definition IDs.

        Returns:
            Initial paused runner state with assigned definition IDs and registered
            experiment directory.
        """
        template = _resolve_template_paths(validated, template_path)
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
        template = _assign_definition_ids(template)
        template_yaml = yaml.safe_dump(
            template.model_dump(exclude_unset=True), allow_unicode=True, sort_keys=False
        )
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
        try:
            await self._copy_assembly_inputs(state, validated, template_path.parent)
            state.template_path.write_text(template_yaml, encoding="utf-8")
            _publish_registry(registry_path, registry, experiment_id, folder)
        except BaseException as error:
            # Only the freshly allocated, unregistered instance belongs to this operation.
            try:
                if directory.resolve().is_relative_to(
                    (self._project_root / "experiments").resolve()
                ):
                    await asyncio.to_thread(shutil.rmtree, directory)
            except OSError as cleanup_error:
                error.add_note(f"Build cleanup also failed: {cleanup_error}")
            raise
        return state

    async def _copy_assembly_inputs(
        self,
        state: RunnerState,
        template: ExperimentTemplate,
        config_directory: Path,
    ) -> None:
        """Create runtime directories and copy/check modules and template-relative resources.

        Copies each module identity once and checks every definition's conditional
        contract. Resource paths use config_directory. Cancellation waits for the
        copying thread before outer cleanup may remove its destination.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            template: Validated experiment template supplying definitions and
                policies.
            config_directory: Containing template directory used to resolve relative
                resource paths.
        """
        directory = state.experiment_directory
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
        copied: dict[tuple[str, str], ModuleManifest] = {}
        checked: dict[Path, tuple[ModuleManifest, str]] = {}
        copy_task = None
        try:
            for role, entries in (
                ("stage", template.stages),
                ("service", template.services),
            ):
                for item in entries:
                    if isinstance(item, ServiceCallDefinition):
                        continue
                    inputs = asyncio.create_task(asyncio.to_thread(
                        self._module_copy_inputs, item.module, role, directory, dict(copied)
                    ))
                    key, source, target, manifest = await _await_read_task(inputs)
                    if key not in copied:
                        copy_task = asyncio.create_task(
                            asyncio.to_thread(shutil.copytree, source, target)
                        )
                        await asyncio.shield(copy_task)
                        copied[key] = manifest
                    await self._check_module_async(
                        state, item.module, "returns_data" in item.model_fields_set,
                        checked=checked,
                    )
            for resource in template.resources:
                source = Path(resource.path)
                if not source.is_absolute():
                    source = config_directory / source
                target = directory / "shared_data/resources" / resource.name
                operation = shutil.copytree if source.is_dir() else shutil.copy2
                copy_task = asyncio.create_task(
                    asyncio.to_thread(operation, source, target)
                )
                await asyncio.shield(copy_task)
        finally:
            # Cancellation cannot stop a thread; reap copying before the caller removes its folder.
            if copy_task is not None:
                await asyncio.gather(copy_task, return_exceptions=True)

    def _module_copy_inputs(
        self,
        module: ModuleReference,
        role: str,
        directory: Path,
        copied: dict[tuple[str, str], ModuleManifest],
    ) -> tuple[tuple[str, str], Path, Path, ModuleManifest]:
        """Check module links, identity, and role and return its copy paths and manifest.

        Args:
            module: Validated name/version/hash reference used to locate and verify
                module code.
            role: Required stage or service role.
            directory: Root of the new experiment where versioned module code will
                be copied.
            copied: Module identities already copied in this assembly, mapped to
                validated manifests.

        Returns:
            Module name/version key, source directory, target directory, and
            validated manifest.
        """
        key = (module.name, module.version)
        source = self._project_root / "modules" / key[0] / key[1]
        target = directory / "modules" / key[0] / key[1]
        if (
            source.is_symlink()
            or source.is_junction()
            or any(
                item.is_symlink() or item.is_junction() for item in source.rglob("*")
            )
        ):
            raise ValueError("Module code must not contain filesystem links.")
        manifest = copied.get(key) or _read_module_manifest(source)
        if (manifest.name, manifest.version) != key:
            raise ValueError("module.yaml identity differs from template.")
        if manifest.role != role:
            raise ValueError(f"Module {key} does not have role={role}.")
        return key, source, target, manifest

    async def rebuild(
        self,
        state: RunnerState,
        template_yaml: str,
        template: JsonObject,
        *,
        workspace: Path,
        prepare_only: bool = False,
    ) -> None:
        """Prepare checked additions, then publish without replacing immutable code.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            template_yaml: Applied or candidate template YAML retained for audit and
                publication.
            template: Validated experiment template supplying definitions and
                policies.
            workspace: Absolute rebuild directory beneath the experiment's
                runner/rebuilds tree.
            prepare_only: Stage/check candidate files without publishing them when
                True.
        """
        validated = ExperimentTemplate.model_validate(template)
        await self._rebuild(state, template_yaml, validated, workspace, prepare_only)

    async def _rebuild(
        self,
        state: RunnerState,
        template_yaml: str,
        template: ExperimentTemplate,
        workspace: Path,
        prepare_only: bool,
    ) -> None:
        """Stage or publish checked template files under a protected rebuild workspace.

        Args:
            state: Existing experiment state and recorded rebuild intent.
            template_yaml: Candidate template text.
            template: Validated candidate definitions and policies.
            workspace: Absolute directory within the experiment's runner/rebuilds tree.
            prepare_only: Whether to stage inputs instead of publishing them.

        Raises:
            ValueError: The workspace or candidate contents are invalid.
            RuntimeError: Publication has no recorded rebuild intent.
        """
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
        await self._rebuild_modules(
            candidate, staged, template, workspace, prepare_only
        )
        await self._check_resources_async(candidate)
        if prepare_only:
            (workspace / "experiment.yaml").write_text(template_yaml, encoding="utf-8")
        else:
            # The durable pending_rebuild record covers the multi-file publication.
            (workspace / "experiment.yaml").replace(state.template_path)

    async def _rebuild_modules(
        self,
        candidate: RunnerState,
        staged: RunnerState,
        template: ExperimentTemplate,
        workspace: Path,
        prepare_only: bool,
    ) -> None:
        """Prepare or publish each distinct candidate module and check all references.

        Args:
            candidate: Candidate runner state describing the real target experiment.
            staged: Candidate runner state rooted in the temporary rebuild
                workspace.
            template: Validated experiment template supplying definitions and
                policies.
            workspace: Absolute rebuild directory beneath the experiment's
                runner/rebuilds tree.
            prepare_only: Stage/check candidate files without publishing them when
                True.
        """
        root = candidate.experiment_directory
        seen = set()
        checked: dict[Path, tuple[ModuleManifest, str]] = {}
        for role, entries in (
            ("stage", template.stages),
            ("service", template.services),
        ):
            for definition in entries:
                if isinstance(definition, ServiceCallDefinition):
                    continue
                reference = definition.module
                conditional = "returns_data" in definition.model_fields_set
                key = (reference.name, reference.version)
                target = root / "modules" / key[0] / key[1]
                prepared = workspace / "modules" / key[0] / key[1]
                if prepare_only:
                    inspecting = asyncio.create_task(asyncio.to_thread(
                        self._rebuild_module_source, root, target, key, role
                    ))
                    source = await _await_read_task(inspecting)
                    if not target.exists() and key not in seen:
                        prepared.parent.mkdir(parents=True, exist_ok=True)
                        copying = asyncio.create_task(
                            asyncio.to_thread(shutil.copytree, source, prepared)
                        )
                        try:
                            await asyncio.shield(copying)
                        finally:
                            await asyncio.gather(copying, return_exceptions=True)
                    await self._check_module_async(
                        candidate if target.exists() else staged, reference, conditional,
                        checked=checked,
                    )
                else:
                    await self._publish_rebuild_module(
                        root,
                        target,
                        prepared,
                        candidate,
                        staged,
                        reference,
                        conditional,
                        checked,
                    )
                seen.add(key)

    def _rebuild_module_source(
        self, root: Path, target: Path, key: tuple[str, str], role: str
    ) -> Path:
        """Select existing or registered module code and validate confinement/identity/role.

        Args:
            root: Current experiment root used to confine existing module code.
            target: Candidate module's destination directory in the experiment.
            key: Module name/version pair.
            role: Required stage or service role.

        Returns:
            Existing experiment code or registered project code matching the
            required identity and role.

        Raises:
            ValueError: Code escapes its allowed root, contains links, or has a
                different manifest identity/role.
        """
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
            or any(p.is_symlink() or p.is_junction() for p in source.rglob("*"))
        ):
            raise ValueError("Module code must not contain filesystem links.")
        manifest = _read_module_manifest(source)
        if (manifest.name, manifest.version, manifest.role) != (
            *key,
            role,
        ):
            raise ValueError(f"Module identity/role differs from template: {key}.")
        return source

    async def _publish_rebuild_module(
        self,
        root: Path,
        target: Path,
        prepared: Path,
        candidate: RunnerState,
        staged: RunnerState,
        reference: ModuleReference,
        conditional: bool,
        checked: dict[Path, tuple[ModuleManifest, str]],
    ) -> None:
        """Publish a checked staged module when absent and verify the candidate reference.

        Args:
            root: Absolute root of the experiment or payload being processed.
            target: Destination path or target record selected for this operation.
            prepared: Checked staged module directory ready for publication.
            candidate: Candidate runner state describing the real target experiment.
            staged: Candidate runner state rooted in the temporary rebuild
                workspace.
            reference: Expected module name/version/hash reference.
            conditional: Whether the template supplies the conditional returns_data
                field.
            checked: Per-operation cache of checked module paths and manifest/hash
                pairs.
        """
        if not target.exists():
            await self._check_module_async(staged, reference, conditional, checked=checked)
            if not target.parent.resolve().is_relative_to(root):
                raise ValueError("Module destination escapes the experiment.")
            target.parent.mkdir(parents=True, exist_ok=True)
            prepared.replace(target)
            # Rename preserves the verified immutable contents within this operation.
            if prepared in checked:
                checked[target] = checked.pop(prepared)
        await self._check_module_async(candidate, reference, conditional, checked=checked)

    async def _check_modules_async(self, state: RunnerState) -> None:
        """Check all referenced modules asynchronously, reusing hashes within this pass."""
        checked: dict[Path, tuple[ModuleManifest, str]] = {}
        for definition in (*state.template.stages, *state.template.services):
            await self._check_module_async(
                state, state.template.module_reference(definition),
                "returns_data" in definition.model_fields_set, checked=checked,
            )

    def check_modules(self, state: RunnerState) -> None:
        """Verify all applied-template module hashes and conditional-field contracts.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
        """
        self._check_modules(state, state.template)

    def _check_modules(self, state: RunnerState, template: ExperimentTemplate) -> None:
        """Verify the supplied template's modules within the experiment directory.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            template: Validated experiment template supplying definitions and
                policies.
        """
        for definition in (*template.stages, *template.services):
            self._check_module(
                state,
                template.module_reference(definition),
                "returns_data" in definition.model_fields_set,
            )

    def check_module(
        self,
        state: RunnerState,
        definition: StageDefinition
        | ServiceCallDefinition
        | ServiceDefinition
        | JsonObject,
    ) -> None:
        """Verify one definition's installed content and conditional configuration.

        Args:
            state: Experiment state containing local module code and service definitions.
            definition: Stage, service, or service-call model or JSON definition.

        Raises:
            ValueError: References, conditional fields, or content hashes disagree.
        """
        module = ModuleReference.model_validate(
            self.module_reference(state.template, definition)
        )
        fields = (
            definition.keys()
            if isinstance(definition, dict)
            else definition.model_fields_set
        )
        self._check_module(state, module, "returns_data" in fields)

    def _check_module(
        self,
        state: RunnerState,
        module: ModuleReference,
        returns_data: bool,
    ) -> ModuleManifest:
        """Read and hash local module code and return its manifest after integrity checks.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            module: Validated name/version/hash reference used to locate and verify
                module code.
            returns_data: Whether the template field is present, not its boolean
                value.

        Returns:
            Installed manifest after actual/template/registered hashes and
            conditional-field presence agree.
        """
        directory = self._module_directory(state.experiment_directory, module)
        manifest = _read_module_manifest(directory)
        actual = self._module_manager.module_hash(module.name, target_folder=directory)
        self._check_module_integrity(module, returns_data, manifest, actual)
        return manifest

    async def _check_module_async(
        self,
        state: RunnerState,
        module: ModuleReference,
        returns_data: bool,
        *,
        checked: dict[Path, tuple[ModuleManifest, str]] | None = None,
    ) -> ModuleManifest:
        """Read files in a worker; compare with caller-owned HashDB on this thread.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
            module: Validated name/version/hash reference used to locate and verify
                module code.
            returns_data: Whether the template field is present, not its boolean
                value.
            checked: Per-operation cache of checked module paths and manifest/hash
                pairs.

        Returns:
            Manifest after hash/conditional checks. The checked mapping, when
            supplied, caches this operation's verified immutable contents.

        Raises:
            ValueError: The conditional-field contract or
                template/registered/content hash comparison fails.
        """
        directory = self._module_directory(state.experiment_directory, module)
        result = None if checked is None else checked.get(directory)
        if result is None:
            cancelled = Event()
            reading = asyncio.create_task(asyncio.to_thread(
                self._read_module_hash, directory, module.name, cancelled
            ))
            result = await _await_read_task(reading, cancelled)
            if checked is not None:
                checked[directory] = result
        manifest, actual = result
        self._check_module_integrity(module, returns_data, manifest, actual)
        return manifest

    def _module_directory(self, root: Path, module: ModuleReference) -> Path:
        """Return the module directory after confirming it resolves within the experiment.

        Args:
            root: Absolute root of the experiment or payload being processed.
            module: Validated name/version/hash reference used to locate and verify
                module code.

        Returns:
            The module directory after confirming it resolves within the experiment.
        """
        name, version = module.name, module.version
        directory = root / "modules" / name / version
        if not directory.resolve().is_relative_to(root.resolve()):
            raise ValueError("Module code escapes the experiment.")
        return directory

    def _read_module_hash(
        self, directory: Path, name: str, cancelled: Event
    ) -> tuple[ModuleManifest, str]:
        """Read a module manifest and compute its hash with cooperative cancellation.

        Args:
            directory: Absolute operation directory whose contents are inspected or
                prepared.
            name: Operation or module name used by the selected action.
            cancelled: Thread-safe cooperative cancellation event; callers await the
                worker before cleanup.

        Returns:
            Validated module manifest and computed hexadecimal content digest.
        """
        manifest = _read_module_manifest(directory)
        digest = self._module_manager._module_hash(name, directory, cancelled)
        return manifest, digest

    def _check_module_integrity(
        self, module: ModuleReference, returns_data: bool,
        manifest: ModuleManifest, actual: str,
    ) -> None:
        """Check conditional-field presence and agreement of actual/template/registered hashes.

        Args:
            module: Expected name, version, and template hash.
            returns_data: Whether the definition supplies the field, not its boolean value.
            manifest: Manifest read from local module code.
            actual: Computed module content hash.

        Raises:
            ValueError: The conditional contract or any required hash comparison fails.
        """
        name, version, expected_hash = module.name, module.version, module.hash
        conditional = manifest.stage_kind == "conditional"
        if conditional != returns_data:
            raise ValueError(
                "Conditional stages require returns_data in the template; "
                "ordinary stages and services must omit it."
            )
        registered = self._module_manager.hash_db.get_module_hash(name, version)
        if (
            not registered
            or actual.lower() != registered.lower()
            or actual.lower() != expected_hash.lower()
        ):
            raise ValueError(f"Module integrity check failed: {name} / {version}")

    def check_resources(self, state: RunnerState) -> None:
        """Verify copied resource contents for entries that declare a hash.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
        """
        self._check_resources(
            state.experiment_directory,
            state.template.resources,
        )

    def _check_resources(
        self, directory: Path, resources: list[ResourceDefinition],
        cancelled: Event | None = None,
    ) -> None:
        """Hash declared files/directories and reject mismatches, honoring cancellation.

        Args:
            directory: Absolute operation directory whose contents are inspected or
                prepared.
            resources: Validated static resource definitions whose optional hashes
                are checked.
            cancelled: Thread-safe cooperative cancellation event; callers await the
                worker before cleanup.
        """
        cancellation = cancelled if cancelled is not None else Event()
        for resource in resources:
            if resource.hash is None:
                continue
            name, expected_hash = resource.name, resource.hash
            path = directory / "shared_data" / "resources" / name
            if path.is_dir():
                actual = self._module_manager._module_hash(name, path, cancellation)
            else:
                actual = _hash_file(
                    path, self._module_manager.hashing_settings,
                    cancellation,
                ).hex()
            if actual.lower() != expected_hash.lower():
                raise ValueError(f"Resource integrity check failed: {name}")

    async def _check_resources_async(self, state: RunnerState) -> None:
        """Check resource hashes in a worker and await its exit before propagating cancellation.

        Args:
            state: Runner state providing the selected template, cursor, and
                participant ownership.
        """
        cancelled = Event()
        if getattr(self.check_resources, "__func__", None) is not ExperimentAssembler.check_resources:
            # Existing library hooks keep their state-based contract and run in
            # a worker, as before. Their readers must also finish on cancellation.
            reading = asyncio.create_task(asyncio.to_thread(self.check_resources, state))
        else:
            resources = [item.model_copy(deep=True) for item in state.template.resources]
            reading = asyncio.create_task(asyncio.to_thread(
                self._check_resources, state.experiment_directory, resources, cancelled
            ))
        await _await_read_task(reading, cancelled)


def _publish_registry(
    path: Path,
    registry: JsonObject,
    experiment_id: str,
    folder: str,
) -> None:
    """Replacing the registry commits the newly assembled experiment's registration."""
    registry[experiment_id] = folder
    temporary = path.with_name(f".experiments-{uuid4()}.json")
    try:
        temporary.write_text(json.dumps(registry, ensure_ascii=False), encoding="utf-8")
        temporary.replace(path)
    except BaseException as error:
        try:
            temporary.unlink(missing_ok=True)
        except OSError as cleanup_error:
            error.add_note(f"Build cleanup also failed: {cleanup_error}")
        raise
