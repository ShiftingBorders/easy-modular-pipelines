"""Loopback-only system API fixture for the approved manual browser review.

This is test data, never imported or started by the dashboard application.
GET /_scenario?mode=resources_error|resources_corrupt|restored|normal selects
the failure/recovery scenario without changing production state.
"""

import argparse
import json
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


class FixtureHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args) -> None:
        pass

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        if parsed.path == "/_scenario":
            self.server.scenario = parse_qs(parsed.query).get("mode", ["normal"])[0]
            self.send_json({"scenario": self.server.scenario})
            return
        path = parsed.path.removeprefix("/api/")
        scenario = getattr(self.server, "scenario", "normal")
        if path == "compute" and scenario == "resources_error":
            self.send_json(
                {
                    "error": {
                        "code": "resource_journal_corrupted",
                        "message": "Resource journal cannot be read",
                    }
                },
                status=503,
            )
            return
        if path == "compute" and scenario == "resources_corrupt":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'{"metrics":')
            return
        now = datetime.now(UTC)
        observed = now.isoformat()
        experiments = [
            {
                "experiment_id": "exp-test",
                "name": "Fixture experiment",
                "status": "paused" if scenario == "restored" else "running",
                "run_id": "run-test",
                "template_revision_id": "rev-2",
                "completed_cycles": 3,
                "total_cycles": 10,
                "started_at": (now - timedelta(hours=2)).isoformat(),
            },
            {
                "experiment_id": "exp-completed",
                "name": "Completed fixture",
                "status": "completed",
                "run_id": "run-completed",
                "template_revision_id": "rev-1",
                "completed_cycles": 1,
                "total_cycles": 1,
                "started_at": (now - timedelta(days=1)).isoformat(),
            },
        ]
        resources = json.loads(Path(__file__).with_name("resources.json").read_text())
        resources["history"] = {
            name: [
                {
                    "observed_at": (now - timedelta(minutes=2 - index)).isoformat(),
                    "value": value,
                }
                for index, value in enumerate([20, None, 31])
            ]
            for name in ["cpu", "ram", "vram", "disk"]
        }
        resources["observed_at"] = observed
        module = {
            "module_id": "prepare/1",
            "name": "prepare",
            "version": "1",
            "experiment_name": "Fixture experiment",
            "runs": 5,
            "error_count": 2,
            "restarts": 1,
            "p50_seconds": 2,
            "p95_seconds": 3600,
            "recent_attempts": [
                {
                    "experiment_id": "exp-test",
                    "run_id": "run-test",
                    "stage_id": "prepare",
                    "attempt_number": 2,
                    "status": "succeeded",
                    "duration_seconds": 2,
                }
            ],
        }
        service = {
            "instance_id": "service-test",
            "name": "Fixture service",
            "version": "1",
            "state": "ready",
            "observed_at": observed,
            "uptime_seconds": 7200,
            "queue_length": 0,
            "process_metrics": {
                "rss_bytes": 134217728,
                "cpu_percent": 112,
                "history": [
                    {
                        "observed_at": observed,
                        "rss_bytes": 134217728,
                        "cpu_percent": 112,
                    }
                ],
            },
        }
        root = {
            "overview": {
                "metrics": {
                    "active_experiments": 1,
                    "completed_experiments": 1,
                    "failed_experiments": 0,
                    "error_events": 2,
                },
                "attention": [experiments[0]],
                "compute": resources["metrics"],
                "active_alerts": 1,
            },
            "experiments": {"items": experiments},
            "modules": {"items": [module]},
            "services": {"items": [service]},
            "compute": resources,
            "alerts": {
                "active_count": 1,
                "items": [
                    {
                        "name": "Fixture disk rule",
                        "status": "active",
                        "started_at": observed,
                    }
                ],
            },
        }
        if path in root:
            self.send_json(root[path])
            return
        parts = path.split("/")
        if len(parts) != 3 or parts[0] != "experiments":
            self.send_json({"error": "not found"}, status=404)
            return
        experiment_id, view = parts[1:]
        experiment = next(
            (row for row in experiments if row["experiment_id"] == experiment_id), None
        )
        if not experiment:
            self.send_json({"error": "not found"}, status=404)
            return
        context = {
            "experiment_id": experiment_id,
            "run_id": experiment["run_id"],
            "module_name": "prepare",
        }
        event_page = json.loads(Path(__file__).with_name("events.json").read_text())
        event = event_page["items"][0]
        event.update(context=context, occurred_at=observed)
        late = {
            **event,
            "event_id": "late-reply",
            "event_type": "command.result",
            "ignored": "after request timeout",
        }
        queries = parse_qs(parsed.query)
        events = [event, late] if queries.get("view") == ["raw"] else [event]
        measurements = [
            {
                "module_name": name,
                "module_version": "1",
                "metric": metric,
                "unit": unit,
                "cycle_number": cycle,
                "value": value,
                "template_revision_id": "rev-2",
                "complete": True,
                "estimated": False,
            }
            for name, metric, unit, value in [
                ("prepare", "processed_items", "item", 100),
                ("prepare", "io_bytes", "byte", 524288),
                ("validate", "quality_score", "score", 0.95),
            ]
            for cycle in [1, 2, 3]
        ]
        nodes = [
            {
                "stage_id": name,
                "name": name.title(),
                "module": {"name": name, "version": "1"},
                "status": "ready",
            }
            for name in ["prepare", "validate"]
        ]
        operations = [
            {
                "operation_id": "root",
                "name": "DAG cycle",
                "parent_operation_id": None,
                "started_at": (now - timedelta(hours=2)).isoformat(),
                "finished_at": observed,
                "status": "succeeded",
            },
            {
                "operation_id": "child",
                "name": "Prepare <literal>",
                "parent_operation_id": "root",
                "started_at": (now - timedelta(hours=1)).isoformat(),
                "finished_at": observed,
                "status": "succeeded",
            },
            {
                "operation_id": "retry",
                "name": "Retry request",
                "parent_operation_id": "child",
                "started_at": (now - timedelta(seconds=2)).isoformat(),
                "finished_at": observed,
                "status": "failed",
            },
        ]
        if scenario == "restored":
            operations = operations[:2]
        views = {
            "summary": {**experiment, "observed_at": observed},
            "runs": {
                "items": [
                    {
                        "run_id": experiment["run_id"],
                        "template_revision_id": experiment["template_revision_id"],
                        "status": experiment["status"],
                    }
                ]
            },
            "operations": {"items": operations},
            "events": {"items": events},
            "errors": {
                "items": [
                    {
                        "error_id": "err-1",
                        "type": "TimeoutError",
                        "message": "Literal <script>alert(1)</script>",
                        "module_name": "prepare",
                        "stage_id": "prepare",
                        "phase": "execution",
                        "occurred_at": observed,
                    }
                ]
            },
            "measurements": {"items": measurements},
            "template": {
                "template_yaml": "stages:\n  - name: prepare\n",
                "template": {"stages": nodes},
                "nodes": nodes,
                "edges": [
                    {"from": "prepare", "to": "validate"},
                    {
                        "from": "validate",
                        "to": "prepare",
                        "condition": "completed_cycles < 10",
                    },
                ],
            },
            "parameters": {
                "items": [
                    {
                        "attempt_id": "attempt-2",
                        "module_name": "prepare",
                        "cycle_number": 3,
                        "settings": {"message": "<literal>"},
                    }
                ]
            },
            "commands": {
                "items": [
                    {
                        "command": "resume",
                        "kind": "control",
                        "target": "DAG",
                        "status": "completed",
                        "run_id": experiment["run_id"],
                        "sent_at": observed,
                    }
                ]
            },
            "snapshots": {
                "items": [
                    {
                        "snapshot_id": "snapshot-test",
                        "status": "ready",
                        "template_revision_id": "rev-2",
                        "cycle_number": 3,
                        "created_at": observed,
                    }
                ]
            },
            "artifacts": {
                "items": [
                    {
                        "path": "artifacts/result.json",
                        "purpose": "result",
                        "module_name": "prepare",
                        "attempt_id": "attempt-2",
                        "size_bytes": 128,
                    }
                ]
            },
            "forecast": {
                "completed_cycles": 3,
                "total_cycles": 10,
                "eta_seconds": 50400,
                "sample_mean_seconds": 7200,
                "sample_cycles": 3,
                "eta_low_seconds": 48000,
                "eta_high_seconds": 53000,
                "measurements": measurements,
                "component_durations": [
                    {"module_name": "prepare", "mean_seconds": 7000},
                    {"module_name": "validate", "mean_seconds": 200},
                ],
            },
        }
        if view not in views:
            self.send_json({"error": "not found"}, status=404)
            return
        self.send_json(
            {
                "experiment_id": experiment_id,
                "journal": {"journal_id": "fixture-journal", "generation": scenario},
                "observed_at": observed,
                **views[view],
            }
        )

    def send_json(self, document: dict, status: int = 200) -> None:
        payload = json.dumps(document).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8781)
    arguments = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", arguments.port), FixtureHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
