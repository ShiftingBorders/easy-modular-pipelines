"""High-level orchestration of sequential stages and experiment services."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import UUID, uuid4

import psutil

from core.experimentarchiver import ExperimentArchiver
from core.experimentassembler import ExperimentAssembler, find_experiment
from core.logger import OperationLogger
from core.logger_utils.events import LoggingError, copy_json_object, require_text
from core.modulemanager import ModuleManager
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.launch import ModuleLauncher
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from core.runner_utils.services import ServiceManager
from core.runner_utils.snapshots import ExperimentSnapshots
from core.runner_utils.stages import StageRunner
from core.runner_utils.state import (
    JsonObject,
    ModuleRole,
    RunnerState,
    RunnerStateStore,
    StageAttempt,
    state_from_document,
    state_to_document,
)


class ExperimentRunner:
    def __init__(
        self,
        project_root: Path,
        module_manager: ModuleManager,
        *,
        notify: Callable[[JsonObject], None] | None = None,
        archive_config_path: Path | None = None,
    ) -> None:
        self._project_root = Path(project_root)
        if not self._project_root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._assembler = ExperimentAssembler(self._project_root, module_manager)
        self._archiver = ExperimentArchiver(
            self._project_root, module_manager, config_path=archive_config_path
        )
        self._hash_module = module_manager.module_hash
        self._journal = RunnerJournal()
        self._state_store = RunnerStateStore()
        self._resource_observer: Callable[[JsonObject], None] | None = None
        self._resource_error: str | None = None
        self._suspend_resources: Callable[[], Awaitable[None]] | None = None
        self._resume_resources: Callable[[], None] | None = None
        self._launcher = ModuleLauncher(self._assembler, self._journal)
        self._services = ServiceManager(
            self._launcher,
            self._journal,
            self._state_store,
            notify_resources=self._publish_resources,
        )
        self._stages = StageRunner(
            self._launcher,
            self._journal,
            self._state_store,
            notify_resources=self._publish_resources,
        )
        self._snapshots = ExperimentSnapshots(
            self._project_root,
            self._stages,
            self._services,
            self._journal,
            self._state_store,
            assembler=self._assembler,
            hash_module=self._hash_module,
            notify_resources=self._publish_resources,
        )
        self._maintenance = False
        self._maintenance_task = None
        self._continue_source: Path | None = None
        self._recover_live = False
        self._state: RunnerState | None = None
        self._task = None
        self._stage_task = None
        self._service_task = None
        self._stages.bind_services(self._services)
        self._service_retrying = False
        self._step_future = None
        self._last_attempt = None
        self._last_response = None
        self._last_snapshot = None
        self._error = None
        self._notify = notify
        self._closed = False
        self._stop_requested = False
        self._termination_confirmed = True
        self._wake = asyncio.Event()
        self._ready = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()
        self._pending_advance = False
        self._manual = False
        self._desired_mode = "running"
        self._requested_id = None

    async def run(
        self,
        template_path: Path | None = None,
        experiment_id: str | None = None,
        *,
        continue_run: bool = False,
        delayed_start: bool = False,
    ) -> JsonObject:
        if self._closed:
            raise RuntimeError("Runner is closed.")
        if self._maintenance:
            raise RuntimeError(
                "Wait for the current snapshot or restoration operation."
            )
        if not self._termination_confirmed:
            raise RuntimeError(
                "Previous participant termination is unconfirmed; no new experiment may start."
            )
        if type(delayed_start) is not bool or type(continue_run) is not bool:
            raise TypeError("Run flags must be booleans.")
        if continue_run and (experiment_id is None or template_path is not None):
            raise ValueError(
                "continue requires a source experiment_id and no replacement template."
            )
        if self._task is not None and not self._task.done():
            if continue_run:
                raise RuntimeError(
                    "Stop the selected experiment before continuing another instance."
                )
            if experiment_id != self._requested_id:
                raise RuntimeError(
                    "Stop the active experiment before creating another."
                )
            if self._state is not None and self._state.mode == "paused":
                await self.resume()
            return {"experiment_id": self._requested_id, "signaled": True}
        source = (
            find_experiment(self._project_root, experiment_id) if continue_run else None
        )
        experiment_id = (
            f"{uuid4()}:{experiment_id}"
            if continue_run
            else (str(uuid4()) if experiment_id is None else experiment_id)
        )
        try:
            find_experiment(self._project_root, experiment_id)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError(f"Experiment ID already exists: {experiment_id}")
        if not continue_run and (
            template_path is None or not Path(template_path).is_absolute()
        ):
            raise ValueError("A new experiment requires an absolute template_path.")
        await self._services.close()
        self._services = ServiceManager(
            self._launcher,
            self._journal,
            self._state_store,
            notify_resources=self._publish_resources,
        )
        self._service_task = None
        self._stages.bind_services(self._services)
        self._snapshots = ExperimentSnapshots(
            self._project_root,
            self._stages,
            self._services,
            self._journal,
            self._state_store,
            assembler=self._assembler,
            hash_module=self._hash_module,
            notify_resources=self._publish_resources,
        )
        self._journal.close()
        self._requested_id = experiment_id
        self._template_path = None if template_path is None else Path(template_path)
        self._continue_source = source
        self._recover_live = False
        self._desired_mode = "paused" if delayed_start or continue_run else "running"
        self._state = None
        self._publish_resources()
        self._error = None
        self._last_attempt = None
        self._last_response = None
        self._last_snapshot = None
        self._stop_requested = False
        self._pending_advance = False
        self._manual = False
        self._ready.clear()
        self._idle.clear()
        self._wake.set()
        self._task = asyncio.create_task(
            self._advance_dag(), name=f"dag:{experiment_id}"
        )
        return {"experiment_id": experiment_id, "signaled": True}

    async def _advance_dag(self) -> None:
        wake_task = None
        readiness_task = None
        try:
            if self._state is None and self._continue_source is not None:
                manifest = await asyncio.to_thread(
                    self._snapshots.latest_valid, self._continue_source
                )
                saved = manifest["state"]
                if manifest["experiment_id"] != self._requested_id.split(":", 1)[1]:
                    raise ValueError(
                        "Continuation snapshot belongs to another experiment."
                    )
                if saved["phase"] == "completed" or (
                    saved["pending_advance"]
                    and saved["cycle_number"] == saved["template"]["cycles"]
                    and saved["stage_position"] == len(saved["template"]["stages"])
                ):
                    raise RuntimeError(
                        "The snapshot has no remaining cycles; use experiment rerun."
                    )
                folder = str(uuid4())
                directory = self._project_root / "experiments" / folder
                directory.mkdir(parents=True, exist_ok=False)
                state = RunnerState(
                    self._requested_id,
                    directory,
                    f"{uuid4()}:{saved['run_id']}",
                    directory / "experiment.yaml",
                    saved["template_revision_id"],
                    saved["template_yaml"],
                    saved["template"],
                    "paused",
                )
                state.phase = "restoring"
                self._state = state
                self._state_store.save(state)
                registry = read_json(self._project_root / "experiments.json")
                if self._requested_id in registry:
                    raise FileExistsError("Continuation experiment ID already exists.")
                registry[self._requested_id] = folder
                write_json(self._project_root / "experiments.json", registry)
                self._maintenance = True
                try:
                    if (
                        self._resource_observer is not None
                        and self._suspend_resources is None
                    ):
                        raise RuntimeError(
                            "Resource observer must provide a restoration barrier."
                        )
                    await self._snapshots.restore(
                        state,
                        manifest["snapshot_id"],
                        source_directory=self._continue_source,
                        suspend_resources=self._suspend_resources,
                    )
                finally:
                    self._maintenance = False
                    self._publish_resources()
                    if self._resume_resources is not None:
                        self._resume_resources()
                self._pending_advance = state.pending_advance
                action = "ready"
            elif self._state is None:
                state = await self._assembler.assemble(
                    self._template_path, self._requested_id
                )
                self._state = state
                self._journal.open(state, create=True)
                self._journal.record_template(
                    state, state.template_yaml, state.template, "initial"
                )
                self._assembler.check_modules(state)
                self._assembler.check_resources(state)
                state.mode = self._desired_mode
                state.phase = "starting"
                self._save_state()
                action = await self._services.start_all(state)
            else:
                state = self._state
                self._pending_advance = state.pending_advance
                self._assembler.check_modules(state)
                action = "ready"
                if self._recover_live:
                    action = await self._services.recover(state)
                    barriers = {
                        item.prepared_freeze_id or item.freeze_id
                        for item in state.services.values()
                    } - {None}
                    if barriers and action != "stop":
                        await self._services.unfreeze(state, barriers.pop())
                    self._recover_live = False
            if action == "stop":
                raise RuntimeError(
                    "Service startup or recovery requires experiment stop."
                )
            if action == "pause":
                self._desired_mode = "paused"
            state.mode = self._desired_mode
            state.phase = (
                "stage_running"
                if state.active_attempt is not None
                else ("waiting" if state.mode == "paused" else "starting")
            )
            self._save_state()
            self._ready.set()
            if state.active_attempt is not None:
                self._idle.clear()
                self._stage_task = asyncio.create_task(
                    self._stages.recover(state, wait_services=self._services.wait_ready)
                )
            else:
                self._idle.set()
            wake_task = asyncio.create_task(self._wake.wait())
            if state.template["services"]:
                self._service_task = asyncio.create_task(self._services.monitor(state))

            while not self._stop_requested:
                waiting = [
                    wake_task,
                    self._service_task,
                    self._stage_task,
                    readiness_task,
                ]
                await asyncio.wait(
                    [task for task in waiting if task is not None],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                # Apply service decisions even while a stage runs or the DAG is paused.
                if self._service_task is not None and self._service_task.done():
                    action = self._service_task.result()
                    if action == "stop":
                        raise RuntimeError(
                            "Service supervision requires experiment stop."
                        )
                    state.mode = self._desired_mode = "paused"
                    state.pause_requested = True
                    if self._stage_task is None:
                        state.phase = "waiting"
                    self._save_state()
                    if self._step_future is not None:
                        future, self._step_future = self._step_future, None
                        if not future.done():
                            future.set_exception(
                                RuntimeError("A service requires a pause.")
                            )
                    self._service_task = asyncio.create_task(
                        self._services.monitor(state)
                    )

                if self._stage_task is not None and self._stage_task.done():
                    outcome = self._stage_task.result()
                    self._stage_task = None
                    self._last_attempt = outcome.attempt
                    self._last_response = outcome.result
                    if outcome.attempt.outcome == "unknown":
                        state.mode = self._desired_mode = "paused"
                        state.pause_requested = True
                        state.phase = "waiting"
                        self._idle.clear()
                        self._pending_advance = False
                        self._save_state()
                        if outcome.action == "stop":
                            raise RuntimeError(
                                "Previous stage ownership or termination is unconfirmed."
                            )
                        if self._step_future is not None:
                            future, self._step_future = self._step_future, None
                            if not future.done():
                                future.set_exception(
                                    RuntimeError(
                                        "Stage state is unknown; recover or stop it first."
                                    )
                                )
                        continue
                    if outcome.action == "stop":
                        state.last_result = None
                        state.last_result_id = None
                        state.stage_result_ids.pop(outcome.attempt.stage_id, None)
                        state.stage_result_origins.pop(outcome.attempt.stage_id, None)
                        raise RuntimeError("Stage execution requires experiment stop.")
                    state.active_attempt = None
                    response = outcome.result
                    if response is not None and response["result"] == "success":
                        state.last_result = response["data"]
                        state.last_result_id = outcome.attempt.result_request_id
                        state.stage_result_ids[outcome.attempt.stage_id] = (
                            outcome.attempt.result_request_id
                        )
                        state.stage_result_origins[outcome.attempt.stage_id] = (
                            state.experiment_id
                        )
                    else:
                        state.last_result = None
                        state.last_result_id = None
                        state.stage_result_ids.pop(outcome.attempt.stage_id, None)
                        state.stage_result_origins.pop(outcome.attempt.stage_id, None)
                    self._pending_advance = outcome.action == "advance"
                    if outcome.action == "pause":
                        state.mode = self._desired_mode = "paused"
                    final = (
                        self._pending_advance
                        and state.stage_position == len(state.template["stages"])
                        and state.cycle_number == state.template["cycles"]
                    )
                    state.phase = "waiting"
                    self._save_state()
                    mode = state.template["snapshots"]["mode"]
                    if (
                        self._pending_advance
                        and not final
                        and (
                            mode == "after_stage"
                            or mode == "after_epoch"
                            and state.stage_position == len(state.template["stages"])
                        )
                    ):
                        self._maintenance = True
                        state.phase = "snapshotting"
                        try:
                            await self._snapshots.create(state)
                        finally:
                            self._maintenance = False
                            if state.phase == "snapshotting":
                                state.phase = "waiting"
                        self._save_state()
                    self._idle.set()
                    # The final step also waits for confirmed service shutdown.
                    if self._step_future is not None and not final:
                        future, self._step_future = self._step_future, None
                        if not future.done():
                            if outcome.action == "advance":
                                future.set_result(
                                    {
                                        "attempt_id": outcome.attempt.attempt_id,
                                        "result": response,
                                        "phase": state.phase,
                                    }
                                )
                            else:
                                future.set_exception(
                                    RuntimeError(
                                        "Stage did not complete or skip under its policy."
                                    )
                                )
                    if state.mode == "running" or final:
                        self._wake.set()

                if wake_task.done():
                    self._wake.clear()
                    wake_task = asyncio.create_task(self._wake.wait())
                    final = (
                        self._pending_advance
                        and state.stage_position == len(state.template["stages"])
                        and state.cycle_number == state.template["cycles"]
                    )
                    if (
                        self._stage_task is None
                        and readiness_task is None
                        and (
                            state.mode == "running"
                            or self._step_future is not None
                            or final
                        )
                    ):
                        readiness_task = asyncio.create_task(
                            self._services.wait_ready(state)
                        )

                if readiness_task is None or not readiness_task.done():
                    continue
                action = readiness_task.result()
                readiness_task = None
                if action == "stop":
                    raise RuntimeError("Service readiness requires experiment stop.")
                if action == "pause":
                    state.mode = self._desired_mode = "paused"
                    state.pause_requested = True
                    state.phase = "waiting"
                    self._save_state()
                    if self._step_future is not None:
                        future, self._step_future = self._step_future, None
                        if not future.done():
                            future.set_exception(
                                RuntimeError("Service readiness requires a pause.")
                            )
                    continue
                # Manual recovery must finish before a new stage or final shutdown.
                if self._service_retrying or self._maintenance:
                    continue
                final = (
                    self._pending_advance
                    and state.stage_position == len(state.template["stages"])
                    and state.cycle_number == state.template["cycles"]
                )
                if final:
                    state.pending_advance = self._pending_advance
                    self._maintenance = True
                    try:
                        self._last_snapshot = await self._snapshots.finalize(
                            state, terminal_phase="completed"
                        )
                        if not self._last_snapshot["valid"]:
                            raise RuntimeError(
                                f"Final snapshot is invalid: {self._last_snapshot['error']}"
                            )
                    finally:
                        self._maintenance = False
                    self._termination_confirmed = all(
                        item.stopped for item in state.services.values()
                    )
                    await self._services.close()
                    state.phase = "completed"
                    self._save_state()
                    if self._step_future is not None:
                        future, self._step_future = self._step_future, None
                        if not future.done():
                            future.set_result(
                                {
                                    "attempt_id": None
                                    if self._last_attempt is None
                                    else self._last_attempt.attempt_id,
                                    "result": self._last_response,
                                    "phase": state.phase,
                                }
                            )
                    break
                if state.mode == "paused" and self._step_future is None:
                    continue
                self._idle.clear()
                if self._pending_advance:
                    state.stage_position += 1
                    if state.stage_position > len(state.template["stages"]):
                        state.cycle_number += 1
                        state.stage_position = 1
                        state.stage_result_ids.clear()
                        state.stage_result_origins.clear()
                        state.stage_attempt_numbers.clear()
                        state.last_result = None
                        state.last_result_id = None
                        for instance in state.services.values():
                            instance.restart_count = 0
                    self._pending_advance = False
                state.pause_requested = False
                state.phase = "stage_running"
                self._save_state()
                self._stage_task = asyncio.create_task(
                    self._stages.execute(
                        state,
                        manual=self._manual,
                        wait_services=self._services.wait_ready,
                    )
                )
                self._manual = False
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - Background failures become explicit experiment state.
            await self._fail(error, {})
        finally:
            tasks = [wake_task, readiness_task, self._stage_task, self._service_task]
            tasks = [task for task in tasks if task is not None]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._stage_task = self._service_task = None
            self._publish_resources()
            self._ready.set()
            self._idle.set()

    async def pause(self) -> None:
        if self._task is None or self._task.done():
            raise RuntimeError("There is no active experiment to pause.")
        self._desired_mode = "paused"
        if self._state is not None:
            self._state.mode = "paused"
            self._state.pause_requested = True
            self._save_state()
        await self._ready.wait()
        await self._idle.wait()
        self._require_active()
        self._state.mode = "paused"
        if self._state.phase == "starting":
            self._state.phase = "waiting"
        self._save_state()

    async def resume(self) -> None:
        if self._task is None:
            raise RuntimeError("There is no experiment to resume.")
        await self._ready.wait()
        state = self._require_active()
        if (
            self._maintenance
            or state.active_attempt is not None
            and self._stage_task is None
        ):
            raise RuntimeError(
                "Resolve the current snapshot, restoration, or unknown stage first."
            )
        if (
            any(
                instance.blocked_action is not None
                for instance in state.services.values()
            )
            or self._service_retrying
            or self._maintenance
        ):
            raise RuntimeError(
                "Resolve the blocked service before resuming the experiment."
            )
        state.mode = "running"
        self._desired_mode = "running"
        state.pause_requested = False
        self._save_state()
        self._wake.set()

    async def step(self) -> JsonObject:
        if self._maintenance:
            raise RuntimeError("Wait for snapshot or restoration before stepping.")
        if self._task is None:
            raise RuntimeError("There is no experiment to step.")
        await self._ready.wait()
        state = self._require_active()
        if (
            state.mode != "paused"
            or not self._idle.is_set()
            or self._step_future is not None
            or self._service_retrying
        ):
            raise RuntimeError(
                "step requires a paused experiment without an active attempt."
            )
        self._step_future = asyncio.get_running_loop().create_future()
        future = self._step_future
        self._wake.set()
        try:
            result = await asyncio.shield(future)
            if result.get("phase") == "completed" and self._task is not None:
                # A final step includes publishing its snapshot and closing the
                # DAG task, so a following command can use the completed state.
                await asyncio.shield(self._task)
            return result
        except asyncio.CancelledError:
            future.cancel()
            if self._step_future is future:
                self._step_future = None
            raise

    async def stop(self) -> JsonObject:
        self._stop_requested = True
        if (
            self._maintenance_task is not None
            and self._maintenance_task is not asyncio.current_task()
            and not self._maintenance_task.done()
        ):
            self._maintenance_task.cancel()
            await asyncio.gather(self._maintenance_task, return_exceptions=True)
        if self._task is not None and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._state is not None:
            failure = None
            try:
                stopped = await self._stages.interrupt(self._state, "stop")
                if not stopped:
                    failure = RuntimeError("Could not confirm stage termination.")
                else:
                    await self._stages.close(self._state)
            except Exception as error:  # noqa: BLE001 - Services must still receive stop after a stage error.
                stopped = False
                failure = error
            if stopped and self._state.active_attempt is not None:
                self._last_attempt = self._state.active_attempt
                self._state.active_attempt = None
                self._pending_advance = False
            try:
                if stopped and self._state.phase not in (
                    "completed",
                    "failed",
                    "stopped",
                    "restoring",
                ):
                    self._state.pending_advance = self._pending_advance
                    self._last_snapshot = await self._snapshots.finalize(self._state)
                    stopped = all(
                        item.stopped for item in self._state.services.values()
                    )
                else:
                    results = await self._services.stop_all(self._state)
                    stopped = stopped and all(
                        result["stopped"] for result in results.values()
                    )
                    if any(
                        not result["stopped"] or result["error"]
                        for result in results.values()
                    ):
                        failure = failure or RuntimeError(
                            f"Service shutdown failed: {results}"
                        )
            except Exception as error:  # noqa: BLE001 - Preserve the first failure and report unconfirmed shutdown.
                stopped = stopped and all(
                    item.stopped for item in self._state.services.values()
                )
                failure = failure or error
            if (
                not stopped
                and self._state.active_attempt is not None
                and self._state.active_attempt.service_id is not None
                and all(item.stopped for item in self._state.services.values())
                and self._stages.termination_confirmed(self._state)
            ):
                stopped = True
                if (
                    isinstance(failure, RuntimeError)
                    and str(failure) == "Could not confirm stage termination."
                ):
                    failure = None
            self._termination_confirmed = stopped
            await self._services.close()
            if failure is not None:
                await self._fail(failure, {})
                raise RuntimeError("Experiment shutdown failed.") from failure
            if self._state.phase == "restoring":
                self._state.phase = "failed"
            elif self._state.phase not in ("completed", "failed"):
                self._state.phase = "stopped"
            if self._state.active_attempt is not None:
                self._last_attempt = self._state.active_attempt
            self._state.active_attempt = None
            self._save_state()
        if self._step_future is not None:
            if not self._step_future.done():
                self._step_future.set_exception(
                    RuntimeError("Step was cancelled by stop.")
                )
            self._step_future = None
        self._idle.set()
        return self.get_state()

    async def rerun(
        self,
        scope: Literal["stage", "experiment"],
        *,
        position: int | None = None,
        experiment_id: str | None = None,
    ) -> JsonObject:
        if self._maintenance:
            raise RuntimeError("Wait for snapshot or restoration before rerunning.")
        if scope != "stage":
            if scope != "experiment" or position is not None:
                raise ValueError("rerun scope must be stage or experiment.")
            source_id = self._requested_id if experiment_id is None else experiment_id
            if source_id is None:
                raise ValueError("Experiment rerun requires a source experiment.")
            source = find_experiment(self._project_root, source_id)
            if self._task is not None and not self._task.done():
                await self.stop()
            return await self.run(source / "experiment.yaml", delayed_start=True)
        state = self._require_active()
        if type(position) is not int:
            raise ValueError("rerun position must be an integer.")
        if (
            position != state.stage_position
            or state.mode != "paused"
            or not self._idle.is_set()
        ):
            raise RuntimeError("rerun requires the paused pointer's stage.")
        self._pending_advance = False
        self._manual = True
        return await self.step()

    async def retry(self, position: int) -> JsonObject:
        if self._maintenance:
            raise RuntimeError(
                "Wait for snapshot or restoration before retrying a service."
            )
        state = self._require_active()
        if state.mode != "paused" or self._service_retrying:
            raise RuntimeError(
                "retry requires a paused experiment without another service retry."
            )
        if type(position) is not int or not 1 <= position <= len(
            state.template["services"]
        ):
            raise ValueError("Service position is outside the template.")
        definition = state.template["services"][position - 1]
        service_id = definition["service_id"]
        instance = state.services.get(service_id)
        if instance is None:
            raise RuntimeError(
                "Retry the failed preceding service before starting later services."
            )
        self._service_retrying = True
        try:
            action = await self._services.restart(state, service_id, automatic=False)
            if action == "ready":
                # Startup may have paused before reaching the rest of the template.
                action = await self._services.reconcile(state, state.template)
            if action == "stop":
                raise RuntimeError("Service retry requires experiment stop.")
            self._save_state()
            return {
                "service_id": service_id,
                "action": action,
                "service_instance_id": state.services[service_id].service_instance_id,
            }
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._fail(error, {"service_id": service_id})
            raise
        finally:
            self._service_retrying = False
            self._wake.set()

    def move(self, position: int) -> None:
        if self._maintenance:
            raise RuntimeError(
                "Wait for snapshot or restoration before moving the cursor."
            )
        state = self._require_active()
        if state.mode != "paused" or not self._idle.is_set():
            raise RuntimeError("move requires a paused experiment.")
        if type(position) is not int or not 1 <= position <= len(
            state.template["stages"]
        ):
            raise ValueError("Position is outside the DAG.")
        state.stage_position = position
        self._pending_advance = False
        self._save_state()

    def reset_retries(self, kind: ModuleRole, position: int) -> JsonObject:
        if self._maintenance:
            raise RuntimeError(
                "Wait for snapshot or restoration before resetting retries."
            )
        state = self._require_active()
        if kind not in ("stage", "service"):
            raise ValueError("kind must be stage or service.")
        if state.mode != "paused":
            raise RuntimeError("reset_retries requires a paused experiment.")
        if kind == "service":
            if self._service_retrying:
                raise RuntimeError(
                    "Wait for the service retry before resetting counters."
                )
            if type(position) is not int or not 1 <= position <= len(
                state.template["services"]
            ):
                raise ValueError("Service position is outside the template.")
            service_id = state.template["services"][position - 1]["service_id"]
            instance = state.services.get(service_id)
            if instance is None:
                raise ValueError("A started socket service is required.")
            old = instance.restart_count
            instance.restart_count = 0
            self._save_state()
            return {"service_id": service_id, "previous": old, "current": 0}
        if type(position) is not int or not 1 <= position <= len(
            state.template["stages"]
        ):
            raise ValueError("Position is outside the DAG.")
        stage_id = state.template["stages"][position - 1]["stage_id"]
        old = state.stage_retry_counts.get(stage_id, 0)
        state.stage_retry_counts[stage_id] = 0
        self._save_state()
        return {"stage_id": stage_id, "previous": old, "current": 0}

    async def replace(
        self, kind: ModuleRole, position: int, module: JsonObject, settings: JsonObject
    ) -> JsonObject:
        raise NotImplementedError(
            "Module replacement requires protected rebuilds and snapshots."
        )

    async def reload_template(self, template_path: Path | None = None) -> JsonObject:
        raise NotImplementedError(
            "Template reload requires protected rebuilds and snapshots."
        )

    async def _apply_template(
        self, template_yaml: str, template: JsonObject
    ) -> JsonObject:
        raise NotImplementedError("Protected rebuilding is not implemented.")

    async def snapshot(self, label: str | None = None) -> JsonObject:
        if label is not None:
            require_text(label, "snapshot label")
        state = self._require_active()
        if (
            self._maintenance
            or self._service_retrying
            or state.mode != "paused"
            or not self._idle.is_set()
            or state.active_attempt is not None
            or self._step_future is not None
        ):
            raise RuntimeError(
                "snapshot requires a stage-free pause without another maintenance operation."
            )
        self._maintenance = True
        self._maintenance_task = asyncio.current_task()
        state.phase = "snapshotting"
        self._save_state()
        try:
            self._last_snapshot = await self._snapshots.create(state, label)
            state.phase = "waiting"
            self._save_state()
            return self._last_snapshot
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._fail(error, {})
            raise
        finally:
            self._maintenance = False
            self._maintenance_task = None

    async def create_archive(
        self,
        archive_path: Path,
        experiment_id: str | None = None,
    ) -> JsonObject:
        """Export a stopped selection or an explicitly identified stopped experiment."""
        state = self._state
        if experiment_id is not None:
            require_text(experiment_id, "experiment_id")
            if state is None or state.experiment_id != experiment_id:
                root = find_experiment(self._project_root, experiment_id)
                state = self._state_store.load(root)
                if state.owner_identity is not None:
                    raise RuntimeError(
                        "Recover and stop the experiment before archiving an owned state."
                    )
        if state is None:
            raise RuntimeError("Select a stopped experiment or provide experiment_id.")
        if state is self._state and not self._termination_confirmed:
            raise RuntimeError("Participant shutdown is unconfirmed.")
        return await self._run_archive(self._archiver.create(state, archive_path))

    async def inspect_archive(self, archive_path: Path) -> JsonObject:
        """Validate a portable archive without changing the selected experiment."""
        return await self._run_archive(self._archiver.inspect(archive_path))

    async def install_archive(
        self, archive_path: Path, destination: Path
    ) -> JsonObject:
        """Install checked inputs while no DAG owns the local module store."""
        return await self._run_archive(
            self._archiver.install(archive_path, destination)
        )

    async def _run_archive(
        self, operation: Coroutine[None, None, JsonObject]
    ) -> JsonObject:
        if (
            self._closed
            or self._maintenance
            or not self._termination_confirmed
            or (self._task is not None and not self._task.done())
            or (
                self._state is not None
                and self._state.phase not in ("stopped", "completed", "failed")
            )
        ):
            operation.close()
            raise RuntimeError(
                "Stop the experiment and finish maintenance before archive operations."
            )
        self._maintenance = True
        self._maintenance_task = asyncio.current_task()
        try:
            return await operation
        finally:
            self._maintenance = False
            self._maintenance_task = None
            self._wake.set()

    async def rollback(self, snapshot_id: str) -> JsonObject:
        snapshot_id = str(UUID(require_text(snapshot_id, "snapshot_id")))
        if self._closed or self._state is None:
            raise RuntimeError("Select an experiment before rollback.")
        state = self._state
        pending = (
            self._project_root
            / "controller/restore_transactions"
            / f"{state.experiment_directory.name}.json"
        )
        if pending.exists() and read_json(pending).get("phase") not in (
            "complete",
            "failed",
        ):
            raise RuntimeError(
                "Use recover to finish the pending restoration transaction."
            )
        if (
            self._maintenance
            or self._service_retrying
            or state.active_attempt is not None
            or (
                state.phase not in ("completed", "stopped", "failed")
                and (state.mode != "paused" or not self._idle.is_set())
            )
        ):
            raise RuntimeError(
                "rollback requires a stage-free pause or a terminal experiment."
            )
        if self._resource_observer is not None and self._suspend_resources is None:
            raise RuntimeError("Resource observer must provide a restoration barrier.")
        self._maintenance = True
        self._maintenance_task = asyncio.current_task()
        previous_phase = state.phase
        state.phase = "restoring"
        self._publish_resources()
        try:
            # Reopening also permits rollback after an earlier journal writer failure.
            self._journal.close()
            self._journal.open(state, create=False)
            await self._snapshots.restore(
                state, snapshot_id, suspend_resources=self._suspend_resources
            )
            if self._task is not None and not self._task.done():
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            self._error = None
            self._stop_requested = False
            self._termination_confirmed = True
            self._pending_advance = state.pending_advance
            self._last_attempt = self._last_response = None
            self._desired_mode = "paused"
            self._continue_source = None
            self._recover_live = False
            self._wake.clear()
            self._ready.clear()
            self._save_state()
            self._task = asyncio.create_task(
                self._advance_dag(), name=f"dag:{state.experiment_id}"
            )
            return {
                "experiment_id": state.experiment_id,
                "snapshot_id": snapshot_id,
                "mode": "paused",
            }
        except (ValueError, FileNotFoundError):
            if (
                state.phase == "restoring"
                and not (
                    self._project_root
                    / "controller/restore_transactions"
                    / f"{state.experiment_directory.name}.json"
                ).exists()
            ):
                state.phase = previous_phase
            else:
                await self._fail(
                    RuntimeError("Snapshot restoration is incomplete."), {}
                )
            raise
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._fail(error, {})
            raise
        finally:
            self._maintenance = False
            self._maintenance_task = None
            self._publish_resources()
            if self._resume_resources is not None:
                self._resume_resources()

    async def recover(self, experiment_id: str) -> None:
        if self._closed or self._maintenance:
            raise RuntimeError("Runner is closed or already restoring an experiment.")
        if self._task is not None and not self._task.done():
            if (
                self._requested_id != experiment_id
                or self._state is None
                or self._state.mode != "paused"
                or self._state.active_attempt is None
                or self._state.active_attempt.outcome != "unknown"
            ):
                raise RuntimeError(
                    "Stop or detach the selected experiment before recovery."
                )
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        registry = read_json(self._project_root / "experiments.json")
        folder = registry.get(experiment_id)
        if (
            not isinstance(folder, str)
            or folder in (".", "..")
            or Path(folder).name != folder
        ):
            raise FileNotFoundError(f"Unknown experiment: {experiment_id}")
        root = (self._project_root / "experiments" / folder).resolve()
        if not root.is_relative_to((self._project_root / "experiments").resolve()):
            raise ValueError("Experiment registry path escapes the project.")
        marker = (
            self._project_root / "controller/restore_transactions" / f"{folder}.json"
        )
        transaction = read_json(marker) if marker.is_file() else None
        if transaction is not None and transaction.get("phase") != "complete":
            state = state_from_document(root, transaction["stopped_state"])
        else:
            try:
                state = self._state_store.load(root)
            except (FileNotFoundError, ValueError, TypeError, KeyError) as state_error:
                # state.json is optional. A committed checkpoint can bootstrap
                # recovery without accepting edits to the on-disk template.
                identity = read_json(root / "runner/journal.json")
                state = None
                for config_path in (root / "runner/logging").glob("*.json"):
                    try:
                        config = read_json(config_path)["logging"]
                    except (OSError, ValueError, TypeError, KeyError):
                        continue
                    if (
                        config.get("open_mode") != "existing"
                        or config.get("expected_journal") != identity
                        or Path(config.get("db_path", "")).resolve()
                        != root / "journals/events.sqlite"
                    ):
                        continue
                    reader = OperationLogger(config_path)
                    try:
                        reader.open()
                        checkpoint = boundary = None
                        while True:
                            page = await asyncio.to_thread(
                                reader.read_events, checkpoint, limit=1000
                            )
                            if boundary is None:
                                boundary = page["boundary"]["cursor"]
                            for entry in page["events"]:
                                if entry["cursor"] > boundary:
                                    break
                                event = entry["event"]
                                if (
                                    event["context"].get("experiment_id")
                                    != experiment_id
                                ):
                                    continue
                                if event["event_type"] == "runner.checkpoint":
                                    state = state_from_document(root, event["data"])
                                elif event["event_type"] == "experiment.restored":
                                    state = None
                            checkpoint = page["checkpoint"]
                            if checkpoint["cursor"] >= boundary or not page["has_more"]:
                                break
                    finally:
                        reader.close()
                    break
                if state is None:
                    raise RuntimeError(
                        "Recovery requires a valid saved state or committed journal checkpoint."
                    ) from state_error
        if state.experiment_id != experiment_id:
            raise ValueError("Saved state belongs to a different experiment.")
        if state.template_path != root / "experiment.yaml":
            raise ValueError("Saved template path is outside the experiment.")
        _, applied = self._assembler.load_template(
            state.template_path, template_yaml=state.template_yaml
        )
        if applied != state.template:
            raise ValueError("Saved template JSON differs from its applied YAML.")
        if transaction is None and set(state.services) != {
            item["service_id"] for item in state.template["services"]
        }:
            raise RuntimeError(
                "Saved service ownership is incomplete; recover participant metadata before continuing."
            )
        owner = state.owner_identity
        if owner is not None and owner["pid"] != os.getpid():
            try:
                if process_identity(owner["pid"]) == owner:
                    try:
                        psutil.Process(owner["pid"]).wait(timeout=0)
                    except psutil.TimeoutExpired as error:
                        raise RuntimeError(
                            "The previous experiment owner is still alive."
                        ) from error
            except psutil.NoSuchProcess:
                pass
            except OSError as error:
                if not isinstance(
                    error, (FileNotFoundError, ProcessLookupError)
                ) and getattr(error, "winerror", None) not in (87, 1168):
                    raise
        await self._services.close()
        await self._stages.close()
        self._journal.close()
        self._state = state
        self._requested_id = experiment_id
        self._desired_mode = "paused"
        self._continue_source = None
        self._last_attempt = self._last_response = None
        self._error = None
        self._maintenance = True
        self._maintenance_task = asyncio.current_task()
        try:
            if transaction is not None and transaction["phase"] != "complete":
                if (
                    self._resource_observer is not None
                    and self._suspend_resources is None
                ):
                    raise RuntimeError(
                        "Resource observer must provide a restoration barrier."
                    )
                source = (
                    self._project_root / "experiments" / transaction["source_folder"]
                    if transaction["clone"]
                    else None
                )
                await self._snapshots.restore(
                    state,
                    transaction["snapshot_id"],
                    source_directory=source,
                    suspend_resources=self._suspend_resources,
                    resume_transaction=True,
                )
                self._recover_live = False
            else:
                self._journal.open(state, create=False)
                checkpoint = None
                latest = None
                launches = []
                boundary = None
                while True:
                    page = await asyncio.to_thread(
                        self._journal.client.read_events, checkpoint, limit=1000
                    )
                    if boundary is None:
                        boundary = page["boundary"]["cursor"]
                    for entry in page["events"]:
                        if entry["cursor"] > boundary:
                            break
                        event = entry["event"]
                        if event["context"].get("experiment_id") != experiment_id:
                            continue
                        if event["event_type"] == "experiment.restored":
                            latest, launches = None, []
                        elif event["event_type"] == "runner.checkpoint":
                            latest, launches = event["data"], []
                        elif (
                            event["event_type"] == "control.intent"
                            and event["data"].get("action") == "start_stage"
                        ):
                            launches.append(
                                {
                                    **event["context"],
                                    "queued_monotonic": event["data"][
                                        "queued_monotonic"
                                    ],
                                    "queued_at": event["data"]["queued_at"],
                                }
                            )
                    checkpoint = page["checkpoint"]
                    if checkpoint["cursor"] >= boundary or not page["has_more"]:
                        break
                if (
                    latest is not None
                    and latest["checkpoint_id"] != state.checkpoint_id
                ):
                    # The mandatory journal is authoritative for DAG progress; the
                    # file may contain later participant observations with that cursor.
                    committed = state_from_document(root, latest)
                    if state.run_id == committed.run_id:
                        committed.services = state.services
                    vars(state).update(vars(committed))
                if launches:
                    launched = launches[-1]
                    if (
                        state.active_attempt is None
                        or state.active_attempt.attempt_id != launched["attempt_id"]
                    ):
                        definition = next(
                            item
                            for item in state.template["stages"]
                            if item["stage_id"] == launched["stage_id"]
                        )
                        directory = (
                            root
                            / "shared_artifacts"
                            / f"epoch_{launched['cycle_number']}"
                            / self._assembler.module_reference(
                                state.template, definition
                            )["name"]
                            / launched["stage_id"]
                            / f"attempt_{launched['attempt_number']}"
                        )
                        attempt = StageAttempt(
                            launched["attempt_id"],
                            launched["stage_id"],
                            launched["stage_execution_id"],
                            launched["cycle_number"],
                            launched["attempt_number"],
                            directory,
                            {},
                            {},
                            definition["timeout_seconds"],
                        )
                        # Bind ownership before optional files are read so failure
                        # handling still has to confirm this attempt's termination.
                        attempt.request_id = launched["request_id"]
                        attempt.participant = {
                            key: launched[key]
                            for key in (
                                "experiment_id",
                                "participant_id",
                                "participant_instance_id",
                            )
                        }
                        attempt.service_id = definition.get("service_id")
                        attempt.endpoint_path = (
                            root / "runner/endpoints" / f"{attempt.service_id}.json"
                            if attempt.service_id is not None
                            else directory / "executor.lock.json"
                        )
                        attempt.queued_monotonic = launched["queued_monotonic"]
                        attempt.queued_at = launched["queued_at"]
                        state.active_attempt = attempt
                        context = read_json(directory / "context.json")
                        if context["context"]["attempt_id"] != launched["attempt_id"]:
                            raise ValueError(
                                "Saved launch context has a different attempt identity."
                            )
                        attempt.input_data = context["input_data"]
                        attempt.effective_settings = context["settings"]
                        attempt.request_id = context["context"]["request_id"]
                        attempt.participant = {
                            key: context["context"][key]
                            for key in (
                                "experiment_id",
                                "participant_id",
                                "participant_instance_id",
                            )
                        }
                        attempt.endpoint_path = Path(context["endpoint_path"])
                        attempt.service_id = context["service_id"]
                        attempt.queued_at = context["queued_at"]
                        attempt.queued_monotonic = context["queued_monotonic"]
                        record_path = directory / "process.json"
                        if record_path.exists():
                            record = read_json(record_path)
                            if record.get("attempt_id") != attempt.attempt_id:
                                raise ValueError(
                                    "Saved process record has a different attempt identity."
                                )
                            attempt.process_identity = record["stage"]
                            attempt.started_at = record["started_at"]
                        state.active_attempt = attempt
                        state.stage_attempt_numbers[attempt.stage_id] = (
                            attempt.attempt_number
                        )
                self._journal.close()
                self._journal.open(state, create=False)
                self._recover_live = True
            self._pending_advance = state.pending_advance
            self._stop_requested = False
            self._termination_confirmed = True
            state.mode = "paused"
            state.pause_requested = True
            self._wake.clear()
            self._ready.clear()
            if (
                state.phase in ("completed", "stopped", "failed")
                and state.active_attempt is None
                and all(item.stopped for item in state.services.values())
            ):
                self._ready.set()
                self._idle.set()
                return
            # The DAG now owns startup failure handling. It must not cancel the
            # recovery caller that is waiting for its readiness notification.
            self._maintenance_task = None
            self._task = asyncio.create_task(
                self._advance_dag(), name=f"dag:{experiment_id}"
            )
            await self._ready.wait()
            if self._error is not None:
                raise RuntimeError(self._error["message"])
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._error is None:
                await self._fail(error, {})
            raise
        finally:
            self._maintenance = False
            self._maintenance_task = None
            self._publish_resources()
            if self._resume_resources is not None:
                self._resume_resources()

    async def _fail(self, error: Exception, context: JsonObject) -> None:
        self._error = {"type": type(error).__name__, "message": str(error)}
        self._stop_requested = True
        if (
            self._maintenance_task is not None
            and self._maintenance_task is not asyncio.current_task()
            and not self._maintenance_task.done()
        ):
            self._maintenance_task.cancel()
            await asyncio.gather(self._maintenance_task, return_exceptions=True)
        if (
            self._task is not None
            and self._task is not asyncio.current_task()
            and not self._task.done()
        ):
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._state is not None:
            if self._stage_task is not None and not self._stage_task.done():
                self._stage_task.cancel()
                await asyncio.gather(self._stage_task, return_exceptions=True)
            try:
                self._termination_confirmed = await self._stages.interrupt(
                    self._state, "failure"
                )
                if not self._termination_confirmed:
                    self._error["interruption_error"] = (
                        "Stage termination is unconfirmed."
                    )
            except Exception as interruption_error:  # noqa: BLE001 - Preserve both original and shutdown failures.
                self._termination_confirmed = False
                self._error["interruption_error"] = str(interruption_error)
            try:
                results = await self._services.stop_all(self._state)
                self._termination_confirmed = all(
                    result["stopped"] for result in results.values()
                ) and (
                    self._termination_confirmed
                    or (
                        self._state.active_attempt is not None
                        and self._state.active_attempt.service_id is not None
                        and self._stages.termination_confirmed(self._state)
                    )
                )
                if any(
                    not result["stopped"] or result["error"]
                    for result in results.values()
                ):
                    self._error["service_shutdown"] = results
            except Exception as service_error:  # noqa: BLE001 - Shutdown failures must not hide the experiment failure.
                self._termination_confirmed = False
                self._error["service_shutdown_error"] = (
                    f"{type(service_error).__name__}: {service_error}"
                )
            finally:
                await self._services.close()
            self._state.phase = "failed"
            self._publish_resources()
            try:
                self._journal.client.record_error(
                    error, context={"experiment_id": self._state.experiment_id}
                )
                self._save_state()
            except Exception as logging_error:  # noqa: BLE001 - Emergency storage must not recurse into the logger.
                write_json(
                    self._project_root / "controller" / f"failure-{uuid4()}.json",
                    {**self._error, "logging_error": str(logging_error)},
                )
        else:
            write_json(
                self._project_root / "controller" / f"failure-{uuid4()}.json",
                {**self._error, "experiment_id": self._requested_id},
            )
        if self._step_future is not None:
            if not self._step_future.done():
                self._step_future.set_exception(error)
            self._step_future = None
        if self._notify is not None:
            self._notify(self.get_state())

    def _require_active(self) -> RunnerState:
        if self._state is None or self._state.phase in (
            "stopped",
            "completed",
            "failed",
        ):
            raise RuntimeError("The experiment is not active.")
        return self._state

    def _save_state(self) -> None:
        self._state.pending_advance = self._pending_advance
        self._state.checkpoint_id = str(uuid4())
        self._state.owner_identity = process_identity(os.getpid())
        self._journal.client.record_event(
            "runner.checkpoint",
            state_to_document(self._state),
            context={
                "experiment_id": self._state.experiment_id,
                "run_id": self._state.run_id,
            },
        )
        self._publish_resources()
        try:
            self._state_store.save(self._state)
        except OSError as error:
            self._journal.client.record_error(
                error, context={"experiment_id": self._state.experiment_id}
            )
        self._journal.client.record_event(
            "experiment.state",
            self.get_state(),
            context={
                "experiment_id": self._state.experiment_id,
                "run_id": self._state.run_id,
            },
        )
        if self._notify is not None:
            self._notify(self.get_state())

    def set_resource_observer(
        self,
        observer: Callable[[JsonObject], None] | None,
        *,
        suspend: Callable[[], Awaitable[None]] | None = None,
        resume: Callable[[], None] | None = None,
    ) -> None:
        """Report target changes; the application controller owns their observation."""
        self._resource_observer = observer
        self._suspend_resources = suspend
        self._resume_resources = resume
        self._publish_resources()

    def _publish_resources(self) -> None:
        if self._resource_observer is None:
            return
        try:
            self._resource_observer(self.get_resource_snapshot())
            self._resource_error = None
        except Exception as error:  # noqa: BLE001 - Optional observers cannot change execution policy.
            self._resource_error = f"{type(error).__name__}: {error}"

    def get_resource_snapshot(self) -> JsonObject:
        """Describe actual targets without sampling the OS or exposing mutable state."""
        state = self._state
        path = self._journal.reader_config_path
        if (
            state is None
            or path is None
            or self._closed
            or state.phase in ("completed", "stopped", "failed", "restoring")
        ):
            return {"context": {}, "logging_config_path": None, "targets": []}
        context = {
            "experiment_id": state.experiment_id,
            "run_id": state.run_id,
            "cycle_number": state.cycle_number,
            "template_revision_id": state.template_revision_id,
        }
        targets = []
        attempt = state.active_attempt
        if (
            attempt is not None
            and attempt.service_id is None
            and attempt.process_identity is not None
            and not (attempt.executor_status or {}).get("finished", False)
        ):
            definition = next(
                item
                for item in state.template["stages"]
                if item["stage_id"] == attempt.stage_id
            )
            module = definition["module"]
            targets.append(
                {
                    "series_id": attempt.attempt_id,
                    "identity": dict(attempt.process_identity),
                    "context": {
                        **context,
                        "stage_id": attempt.stage_id,
                        "stage_execution_id": attempt.stage_execution_id,
                        "attempt_id": attempt.attempt_id,
                        "attempt_number": attempt.attempt_number,
                        "module_name": module["name"],
                        "module_version": module["version"],
                        "module_hash": module["hash"],
                    },
                }
            )
        for instance in state.services.values():
            if instance.stopped or instance.process_identity is None:
                continue
            module = instance.definition["module"]
            targets.append(
                {
                    "series_id": instance.service_instance_id,
                    "identity": dict(instance.process_identity),
                    "context": {
                        **context,
                        "service_id": instance.service_id,
                        "service_instance_id": instance.service_instance_id,
                        "module_name": module["name"],
                        "module_version": module["version"],
                        "module_hash": module["hash"],
                    },
                }
            )
        return {
            "context": context,
            "logging_config_path": str(path),
            "targets": targets,
        }

    def get_state(self) -> JsonObject:
        state = self._state
        attempt = (
            state.active_attempt
            if state is not None and state.active_attempt is not None
            else self._last_attempt
        )
        if state is not None:
            phase = state.phase
            if (
                phase == "completed"
                and self._task is not None
                and not self._task.done()
            ):
                # finalize serializes a terminal checkpoint before publishing its
                # snapshot. Public completion must wait for publication/teardown.
                phase = "snapshotting"
        elif self._error is not None:
            phase = "failed"
        elif self._stop_requested and self._requested_id is not None:
            phase = "stopped"
        elif self._task is not None and not self._task.done():
            phase = "starting"
        else:
            phase = "idle"
        services = []
        if state is not None:
            for position, definition in enumerate(state.template["services"], 1):
                instance = state.services.get(definition["service_id"])
                active = None if instance is None else instance.active_request
                services.append(
                    {
                        "position": position,
                        "service_id": definition["service_id"],
                        "module": definition["module"],
                        "service_instance_id": None
                        if instance is None
                        else instance.service_instance_id,
                        "implementation": None
                        if instance is None
                        else instance.implementation,
                        "ready": instance is not None and instance.ready,
                        "stopping": instance is not None and instance.stopping,
                        "stopped": instance is None or instance.stopped,
                        "restart_count": 0
                        if instance is None
                        else instance.restart_count,
                        "blocked_action": None
                        if instance is None
                        else instance.blocked_action,
                        "failure": None if instance is None else instance.failure,
                        "process": None
                        if instance is None
                        else instance.process_identity,
                        "last_status": None
                        if instance is None
                        else instance.last_status,
                        "active_request": None
                        if active is None
                        else {
                            key: active[key]
                            for key in ("request_id", "command", "timed_out")
                        },
                        "pending_requests": 0
                        if instance is None
                        else len(instance.pending_requests),
                    }
                )
        return {
            "experiment_id": self._requested_id,
            "phase": phase,
            "mode": state.mode if state is not None else self._desired_mode,
            "cycle_number": None if state is None else state.cycle_number,
            "stage_position": None if state is None else state.stage_position,
            "attempt_id": None if attempt is None else attempt.attempt_id,
            "executor": None if attempt is None else attempt.executor_status,
            "result": None if state is None else state.last_result,
            "error": self._error,
            "source": "runner",
            "fresh": not self._closed,
            "observed_at": datetime.now(UTC).isoformat(),
            "services": copy_json_object({"items": services}, "service state")["items"],
            "termination_confirmed": self._termination_confirmed,
            "resource_observer_error": self._resource_error,
            "stable_snapshot_id": None if state is None else state.stable_snapshot_id,
            "snapshot": self._last_snapshot,
        }

    async def read_events(
        self,
        experiment_id: str,
        checkpoint: JsonObject | None = None,
        *,
        limit: int = 100,
    ) -> JsonObject:
        if self._state is None or experiment_id != self._state.experiment_id:
            raise FileNotFoundError(
                "The selected experiment journal is not open in this runner."
            )
        return await asyncio.to_thread(
            self._journal.client.read_events, checkpoint, limit=limit
        )

    async def close(self) -> None:
        self._closed = True
        self._publish_resources()
        if (
            self._maintenance_task is not None
            and self._maintenance_task is not asyncio.current_task()
            and not self._maintenance_task.done()
        ):
            self._maintenance_task.cancel()
            await asyncio.gather(self._maintenance_task, return_exceptions=True)
        if self._task is not None and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        await self._stages.close()
        await self._services.close()
        if self._step_future is not None:
            self._step_future.cancel()
            self._step_future = None
        if self._state is not None and self._journal.reader_config_path is not None:
            marker = (
                self._project_root
                / "controller/restore_transactions"
                / f"{self._state.experiment_directory.name}.json"
            )
            if not marker.exists() or read_json(marker).get("phase") == "complete":
                self._state.owner_identity = None
                self._state.checkpoint_id = str(uuid4())
                try:
                    self._journal.client.record_event(
                        "runner.checkpoint",
                        state_to_document(self._state),
                        context={
                            "experiment_id": self._state.experiment_id,
                            "run_id": self._state.run_id,
                        },
                    )
                    self._state_store.save(self._state)
                except (LoggingError, OSError) as error:
                    write_json(
                        self._project_root
                        / "controller"
                        / f"owner-release-{uuid4()}.json",
                        {
                            "experiment_id": self._state.experiment_id,
                            "error": str(error),
                        },
                    )
        await asyncio.to_thread(self._journal.close)
