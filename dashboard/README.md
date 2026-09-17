# EMP Dashboard

A standalone FastAPI application for experiment history, live state, resources,
commands, artifacts, and alerts. It connects to an independently running EMP
server and reads history from a locally accessible project directory.

From the repository root:

```text
uv run --locked python -B -m dashboard --project-root examples/weather_dag --system-api-url http://127.0.0.1:8010/api/
```

Open <http://127.0.0.1:8765/>. Run a single worker.

- [Dashboard guide](../docs/dashboard.md): configuration, screens, history,
  commands, ICMP, and notifications.
- [Quickstart](../docs/quickstart.md): prepare and run the example runtime.
- [API contract](API_CONTRACT.md): dashboard endpoints and system integration.
- [Testing](../docs/testing.md#dashboard): backend, browser, and optional ICMP checks.

Base configuration is in [settings.json](settings.json). The dashboard does not
start the runtime and has no user authentication; use it in a trusted local
environment or behind an appropriately protected proxy.
