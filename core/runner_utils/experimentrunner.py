"""High-level orchestration for the initial sequential, stage-only runtime."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from core.experimentassembler import ExperimentAssembler, find_experiment
from core.modulemanager import ModuleManager
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.launch import ModuleLauncher
from core.runner_utils.runtimeio import write_json
from core.runner_utils.stages import StageRunner
from core.runner_utils.state import (
    JsonObject,
    ModuleRole,
    RunnerState,
    RunnerStateStore,
)


class ExperimentRunner:
    def __init__(
        self,
        project_root: Path,
        module_manager: ModuleManager,
        *,
        notify: Callable[[JsonObject], None] | None = None,
    ) -> None:
        self._project_root = Path(project_root)
        if not self._project_root.is_absolute():
            raise ValueError("project_root must be absolute.")
        self._assembler = ExperimentAssembler(self._project_root, module_manager)
        self._journal = RunnerJournal()
        self._state_store = RunnerStateStore()
        self._resource_observer: Callable[[JsonObject], None] | None = None
        self._resource_error: str | None = None
        self._stages = StageRunner(
            ModuleLauncher(self._assembler, self._journal),
            self._journal,
            self._state_store,
            notify_resources=self._publish_resources,
        )
        self._state: RunnerState | None = None
        self._task = None
        self._stage_task = None
        self._step_future = None
        self._last_attempt = None
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
        if not self._termination_confirmed:
            raise RuntimeError(
                "Previous stage termination is unconfirmed; no new experiment may start."
            )
        if type(delayed_start) is not bool or type(continue_run) is not bool:
            raise TypeError("Run flags must be booleans.")
        if continue_run:
            raise NotImplementedError(
                "Continue requires snapshot restoration, which is not implemented yet."
            )
        if self._task is not None and not self._task.done():
            if experiment_id != self._requested_id:
                raise RuntimeError(
                    "Stop the active experiment before creating another."
                )
            if self._state is not None and self._state.mode == "paused":
                await self.resume()
            return {"experiment_id": self._requested_id, "signaled": True}
        experiment_id = str(uuid4()) if experiment_id is None else experiment_id
        try:
            find_experiment(self._project_root, experiment_id)
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError(f"Experiment ID already exists: {experiment_id}")
        if template_path is None or not Path(template_path).is_absolute():
            raise ValueError("A new experiment requires an absolute template_path.")
        self._journal.close()
        self._requested_id = experiment_id
        self._template_path = Path(template_path)
        self._desired_mode = "paused" if delayed_start else "running"
        self._state = None
        self._publish_resources()
        self._error = None
        self._last_attempt = None
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
        try:
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
            state.phase = "waiting"
            self._save_state()
            self._ready.set()
            self._idle.set()
            while not self._stop_requested:
                await self._wake.wait()
                self._wake.clear()
                if state.mode == "paused" and self._step_future is None:
                    continue
                self._idle.clear()
                if self._pending_advance:
                    state.stage_position += 1
                    if state.stage_position > len(state.template["stages"]):
                        state.cycle_number += 1
                        state.stage_position = 1
                        state.stage_result_paths.clear()
                        state.stage_attempt_numbers.clear()
                        state.last_result = None
                        state.last_result_path = None
                    self._pending_advance = False
                state.pause_requested = False
                state.phase = "stage_running"
                self._save_state()
                self._stage_task = asyncio.create_task(
                    self._stages.execute(state, manual=self._manual)
                )
                self._manual = False
                outcome = await self._stage_task
                self._stage_task = None
                self._last_attempt = outcome.attempt
                state.active_attempt = None
                response = outcome.result
                if response is not None and response["result"] == "success":
                    state.last_result = response["data"]
                    state.last_result_path = outcome.attempt.result_path
                    state.stage_result_paths[outcome.attempt.stage_id] = (
                        outcome.attempt.result_path
                    )
                else:
                    state.last_result = None
                    state.last_result_path = None
                    state.stage_result_paths.pop(outcome.attempt.stage_id, None)
                self._pending_advance = outcome.action == "advance"
                if outcome.action == "stop":
                    raise RuntimeError(
                        f"Stage {outcome.attempt.stage_id} exhausted its retry policy."
                    )
                if outcome.action == "pause":
                    state.mode = "paused"
                final = (
                    self._pending_advance
                    and state.stage_position == len(state.template["stages"])
                    and state.cycle_number == state.template["cycles"]
                )
                state.phase = "completed" if final else "waiting"
                self._save_state()
                self._idle.set()
                if self._step_future is not None:
                    future, self._step_future = self._step_future, None
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
                if final:
                    break
                if state.mode == "running":
                    self._wake.set()
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - Background failures become explicit experiment state.
            await self._fail(error, {})
        finally:
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
        self._save_state()

    async def resume(self) -> None:
        if self._task is None:
            raise RuntimeError("There is no experiment to resume.")
        await self._ready.wait()
        state = self._require_active()
        state.mode = "running"
        state.pause_requested = False
        self._save_state()
        self._wake.set()

    async def step(self) -> JsonObject:
        if self._task is None:
            raise RuntimeError("There is no experiment to step.")
        await self._ready.wait()
        state = self._require_active()
        if (
            state.mode != "paused"
            or not self._idle.is_set()
            or self._step_future is not None
        ):
            raise RuntimeError(
                "step requires a paused experiment without an active attempt."
            )
        self._step_future = asyncio.get_running_loop().create_future()
        future = self._step_future
        self._wake.set()
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            future.cancel()
            if self._step_future is future:
                self._step_future = None
            raise

    async def stop(self) -> JsonObject:
        self._stop_requested = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._state is not None:
            stopped = await self._stages.interrupt(self._state, "stop")
            self._termination_confirmed = stopped
            if not stopped:
                await self._fail(
                    RuntimeError("Could not confirm stage termination."), {}
                )
                raise RuntimeError("Could not confirm stage termination.")
            if self._state.phase not in ("completed", "failed"):
                self._state.phase = "stopped"
            if self._state.active_attempt is not None:
                self._last_attempt = self._state.active_attempt
            self._state.active_attempt = None
            self._save_state()
        if self._step_future is not None:
            self._step_future.set_exception(RuntimeError("Step was cancelled by stop."))
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
        if scope != "stage":
            raise NotImplementedError("Experiment rerun requires snapshot support.")
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
        raise NotImplementedError(
            "Service modules are not part of the initial runtime."
        )

    def move(self, position: int) -> None:
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
        state = self._require_active()
        if kind != "stage" or state.mode != "paused":
            raise RuntimeError("reset_retries requires a paused stage.")
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
        raise NotImplementedError(
            "Snapshots are not implemented in the initial runtime."
        )

    async def rollback(self, snapshot_id: str) -> JsonObject:
        raise NotImplementedError("Snapshot restoration is not implemented.")

    async def recover(self, experiment_id: str) -> None:
        raise NotImplementedError(
            "Crash recovery is not implemented; saved results remain on disk."
        )

    async def _fail(self, error: Exception, context: JsonObject) -> None:
        self._error = {"type": type(error).__name__, "message": str(error)}
        self._stop_requested = True
        if (
            self._task is not None
            and self._task is not asyncio.current_task()
            and not self._task.done()
        ):
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._state is not None:
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
            self._state.phase = "failed"
            self._publish_resources()
            try:
                self._journal.client.record_error(
                    error, context={"experiment_id": self._state.experiment_id}
                )
                self._state_store.save(self._state)
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
        self, observer: Callable[[JsonObject], None] | None
    ) -> None:
        """Report target changes; the application controller owns their observation."""
        self._resource_observer = observer
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
        elif self._error is not None:
            phase = "failed"
        elif self._stop_requested and self._requested_id is not None:
            phase = "stopped"
        elif self._task is not None and not self._task.done():
            phase = "starting"
        else:
            phase = "idle"
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
            "services": [],
            "termination_confirmed": self._termination_confirmed,
            "resource_observer_error": self._resource_error,
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
        if self._task is not None and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        await self._stages.close()
        await asyncio.to_thread(self._journal.close)
