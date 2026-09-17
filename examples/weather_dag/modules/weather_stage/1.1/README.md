# weather_stage 1.1

Requires Python 3.12 and StageClient from the EMP core supplied by the runner.
Entry point: `python -B main.py --emp-context <path>`.
The required `operation` setting selects an action:

| Value | Input | Result |
| --- | --- | --- |
| `wait` | Unused | After 40 seconds: `waited_seconds`. |
| `format` | Forecast JSON: `city`, `temperature_c`, `condition`. | `text` and the original `forecast`. |
| `write` | `text` and `forecast`. | The same data and the file's relative `path`. |

Waiting checks cooperative cancellation every 0.25 seconds.
Formatting happens in memory. Writing creates UTF-8 `weather.txt` in the
attempt's supplied `artifacts_directory` and registers it in the journal.

Attempts receive separate directories from the runner; there are no external
effects. An unknown operation or invalid input fails the process.

Each successful action writes one JSON result through StageClient and exits.
The module does not change its code directory or maintain persistent state.
On cooperative cancellation, `wait` returns `fail` with reason `cancelled`;
the executor can force termination if needed.

See the [complete experiment](../../../README.md) and
[module authoring](../../../../../docs/instructions/modules.md).
