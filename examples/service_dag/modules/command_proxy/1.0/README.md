# Command proxy

A `role: service`, `implementation: action` example using participant protocol 2.
The library starts `main.py`; the proxy owns the finite
`action.py start` and `action.py stop` commands. They create/remove only
`ready.txt` in the assigned `module_data_directory`. Code files stay unchanged.

The first successful heartbeat follows completion of the start action and
verification of the marker file. `execute` returns its input data unchanged;
per-call settings do not change the startup context.

Set `state_required: false` for this service in the template.
There is no required persistent state. Freeze/unfreeze controls admission of
work; `save_state` returns `state_path: null`.

Shutdown finishes or interrupts preparation and executes the stop action before
acknowledging shutdown. ParticipantServer records command results in the shared
journal. Losing the runner connection does not itself shut down the service.

This demonstrates command ownership, not a complete Docker/database proxy.
For an external resource, implement its real readiness and termination checks;
successful exit of a launcher alone does not prove readiness.

The example uses the project's Python environment and the SDK supplied by the
runner. Register it through [maintenance mode](../../../../../docs/storage.md),
then refer to its `service_id` from stage nodes.
See [service authoring](../../../../../docs/instructions/python_bridges.md)
and the [participant reference](../../../../../docs/instructions/participant_protocol.md).
