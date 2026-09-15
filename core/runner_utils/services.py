"""Runner-side service supervision and persistent working-request queues.

Socket/commands interface differences stay here. External service internals
belong to their Python proxies. Global pause/stop decisions return to the runner.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from core.runner_utils.connection import ParticipantConnection
from core.runner_utils.journal import RunnerJournal
from core.runner_utils.launch import ModuleLauncher
from core.runner_utils.state import (
    JsonObject,
    RunnerState,
    RunnerStateStore,
    ServiceInstance,
)

type ServiceAction = Literal["ready", "pause", "stop"]


class ServiceManager:
    """Own service connections and mutate only the service slice of runner state."""

    _launcher: ModuleLauncher
    _journal: RunnerJournal
    _state_store: RunnerStateStore
    _connections: dict[str, ParticipantConnection]

    def __init__(
        self,
        launcher: ModuleLauncher,
        journal: RunnerJournal,
        state_store: RunnerStateStore,
    ) -> None:
        # Retain dependencies without starting services or opening connections.
        pass

    async def start_all(self, state: RunnerState) -> ServiceAction:
        # Start services in template order and await readiness before returning.
        # Return readiness or the policy action for the runner to apply to the DAG.
        pass

    async def _start(
        self, state: RunnerState, definition: JsonObject
    ) -> ServiceInstance:
        # Prepare/check parameters, journal intent, and launch the configured command.
        # Socket readiness requires success status within start_timeout; commands-only
        # readiness means command launch, without waiting for exit/JSON or monitoring it.
        pass

    async def wait_ready(self, state: RunnerState) -> ServiceAction:
        # Wait for fresh readiness of all observed services without advancing the DAG.
        # Held stage output stays with the runner; service recovery does not release pause.
        pass

    async def monitor(self, state: RunnerState) -> Literal["pause", "stop"]:
        # Process service observations, heartbeat probes, and working-request deadlines.
        # Restart on status fail, expired grace, or confirmed crashes; retry malformed
        # replies with a fresh probe ID once. Startup uses its own start_timeout.
        # Continue ordinary supervision on pause; return when policy needs a DAG action.
        # A current stage may finish while a service restarts; never launch stages here.
        pass

    async def restart(
        self, state: RunnerState, service_id: str, *, automatic: bool
    ) -> ServiceAction:
        # Confirm old-instance stop, recheck integrity, and await fresh readiness.
        # Preserve pending requests; fail sent work without replay. R2 timeout retry
        # may explicitly issue a new request after restart, always with a new UUID.
        # Automatic service skip means unlimited recovery; manual retry retains pause.
        # Restart counts reset only at cycle end, not on a successful heartbeat.
        pass

    async def request(
        self, state: RunnerState, service_id: str, command: str, args: JsonObject
    ) -> JsonObject:
        # Allocate a never-reused UUID, persist the queue, and await this request's result.
        # Keep work serial per service; heartbeat, command-state queries, and stop remain
        # responsive. Do not mistake queue acceptance for completion of the command.
        pass

    async def _send_next(self, state: RunnerState, service_id: str) -> None:
        # Send only after previous actual work has ended; confirm journal intent first.
        # Record send time separately and preserve its deadline through reconnections.
        # Never reuse a sent request_id, including when its outcome is uncertain.
        pass

    async def _handle_message(
        self, state: RunnerState, service_id: str, message: JsonObject
    ) -> None:
        # Match request/instance identity; status and command_result have separate roles.
        # Record outcomes via journal.client.record_command_result(author="runner").
        # Late results remain ignored diagnostics, service data is not stage input,
        # and a command failure by itself does not imply that the service has failed.
        pass

    async def _handle_timeout(
        self, state: RunnerState, service_id: str, request_id: str
    ) -> Literal["wait", "restart", "stop"]:
        # Apply the explicit R2 policy at command_timeout_seconds: pause until actual
        # completion, restart with a new request, or stop. Return the required action.
        # Status never extends this deadline; no second timeout is introduced.
        # Exact template field/machine values remain TBD; late success stays ignored.
        pass

    async def prepare_rebuild(self, state: RunnerState, template: JsonObject) -> None:
        # After the pre-rebuild snapshot, stop removed/changed services and any service
        # sharing a code directory that must change. Confirm stops before file mutation.
        # Leave unchanged services running when their code remains untouched.
        pass

    async def reconcile(
        self, state: RunnerState, template: JsonObject
    ) -> ServiceAction:
        # After assembler.rebuild, start new/changed definitions and retain unchanged
        # instances. Await readiness without changing the experiment's pause mode.
        pass

    async def recover(self, state: RunnerState) -> ServiceAction:
        # Reconnect living instances and reconcile R1 command state against the journal.
        # Restore queues without replay; absence at the participant is not non-execution.
        # Previously ready services get fresh grace; unfinished startup and request
        # deadlines retain their remaining time. Require fresh success status.
        pass

    async def save_states(
        self, state: RunnerState, snapshot_id: str
    ) -> dict[str, Path]:
        # Freeze all included writes and await exports in allocated directories.
        # Validate experiment-relative state paths and ensure exports survive shutdown.
        # Heartbeats continue; replaced instances invalidate freeze and the snapshot.
        # Commands-only services provide no freeze guarantee; stateless exports may be absent.
        pass

    async def load_states(
        self, state: RunnerState, service_states: dict[str, Path]
    ) -> None:
        # Load every required state into the restored services and confirm readiness.
        # Missing optional stateless data is allowed; partial required state is an error.
        pass

    async def unfreeze(self, state: RunnerState, snapshot_id: str) -> None:
        # Confirm write resumption after success/cancellation, including uncertain
        # command effects. Raise on unconfirmed unfreeze so the runner stops the DAG.
        pass

    async def stop_all(self, state: RunnerState) -> JsonObject:
        # Request shutdown/action stop, using start_timeout rather than command timeout.
        # Confirm socket-service termination; do not control action-stop internals.
        # Return actual stop outcomes for the final journal and snapshot.
        pass

    async def close(self) -> None:
        # Close runner-side channels without stopping independently running services.
        pass
