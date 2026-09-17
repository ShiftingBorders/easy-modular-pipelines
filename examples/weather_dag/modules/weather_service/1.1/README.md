# weather_service 1.1

Requires Python 3.12 and the EMP core supplied by the runner.
Entry point: `python -B main.py --emp-context <path>`. No startup settings.

Before readiness, it creates a JSON forecast containing `city`,
`temperature_c`, `condition`, `sequence`, and `generated_monotonic`.
It updates the forecast every 30 seconds using synthetic values and a PRNG
seed of 42. City and condition values are Russian text.
`generated_monotonic` is a local monotonic timestamp, not a calendar date.

`execute` returns the current forecast and ignores previous-stage input and
per-call settings. A repeat reads the latest forecast, so it can return different
data after an update. Request counts and events enter the shared journal.
The service does not use the internet or modify module code.

The working `forecast.json` lives in the assigned `module_data_directory`.
`freeze_writes` pauses updates, `save_state` exports the forecast and request
count to the allocated directory, `load_state` restores them, and
`unfreeze_writes` resumes updates. Set `state_required: true` in the template.

PRNG state and time remaining until the next update are not saved.
Rollback restores the current forecast but does not guarantee reproduction
of the future sequence.

`shutdown` stops the update loop and closes ParticipantServer and the logger.
After a process restart, the sequence starts from seed 42 again; the runner
can subsequently load saved state.

See the [complete experiment](../../../README.md) and
[service authoring](../../../../../docs/instructions/python_bridges.md).
