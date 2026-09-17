# Resource monitoring

The resource collector observes host CPU/RAM, disk capacity, selected network
interface I/O, and CPU/RAM of known experiment processes. The execution
controller owns its lifecycle. The dashboard reads these observations.

GPU/VRAM collection is not ready and is disabled. The reserved
`gpu_interval_seconds` setting currently has no effect.

## Read measurements

With a ready run-mode server:

```text
uv run python -B cli.py resources --watch 1
uv run python -B cli.py resource-history --after 0 --limit 20
```

Use `--config` or `--url` before the subcommand for a non-default API address.
The corresponding HTTP routes are described in the [system API](http_api.md).

RAM history pages include `cursor`, `history_id`, and `gap`.
When `history_id` changes, start from `after=0`.
`limit` is between 1 and 1000. A gap explicitly reports lost history.

Between experiments, observations live in a bounded RAM buffer.
During an experiment, measurements are also written to its journal.
Earlier RAM observations are not backfilled into a newly started experiment.
Stopping the collector can lose RAM history.

## Interpret the values

- Process CPU uses 100% per logical CPU and may exceed 100%.
- Process RAM is RSS / Windows working set.
- Missing values are `null`. The first CPU sample without a baseline is not
  a measured zero.
- Internet I/O is Mbps derived from the chosen interface's counters. It includes
  local traffic too; the name does not imply WAN-only traffic.
- The first network observation, an interface change, or a counter reset gives
  `null` instead of a false zero.
- A proxy's CPU/RAM describes that process, not a remote service or container.
- Freshness is based on the last successful observation of each metric.

Process identity includes PID and OS creation/machine/boot information.
A matching old PID is insufficient to identify a live experiment process.

## Configure the collector

Defaults are in [resource_collector.json](../default_settings/resource_collector.json).
Pass a different file to `webserver.py --resource-config <path>` or set
`resource_config_path` in server JSON. Restart the controller to apply changes.

| Setting | Default |
| --- | --- |
| `sample_interval_seconds` | 1 |
| `history_seconds` | 900 |
| `max_buffer_bytes` | 16777216 |
| `stale_after_intervals` | 3 |
| `status_interval_seconds` | 1 |
| `startup_timeout_seconds` | 10 |
| `heartbeat_timeout_seconds` | 10 |
| `shutdown_timeout_seconds` | 5 |
| `restart_delays_seconds` | `[1, 2, 4, 8, 16, 30]` |
| `stable_reset_seconds` | 60 |
| `logging_busy_timeout_seconds` | 0.05 |
| `logging_retry_seconds` | 5 |
| `disk_path` | `".."`, relative to the configuration file |
| `network_interface` | `null`: select by route |
| `network_reference_address` | `"1.1.1.1"` |
| `gpu_interval_seconds` | 5, reserved and unused |

Automatic network selection uses the IPv4 route to the reference address;
it does not send a probe packet. The RAM buffer limits its stored history,
not total process memory.

## Failures and location

A collector failure does not itself change the DAG state. The controller
restarts it with configured delays, confirming termination of the previous
instance before starting another. The collector exits when its controller
disappears.

Resource-log failures can produce a later `resources.gap` observation.
They do not override the policy for obligatory runner journal writes.

CPU/RAM/disk/network observations describe the runtime machine.
[Dashboard ICMP](dashboard.md#icmp-and-notifications) runs on the dashboard
server's machine, which can be different from both runtime and browser.
