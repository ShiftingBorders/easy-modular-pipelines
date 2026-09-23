# Recorded Heilbronn history

This fixture is extracted from `heilbronn-full-access-5gen`, stopped on
2026-09-18 after three recorded completed cycles. Two cycles are eligible for
the forecast timing sample; the template originally requested six cycles.

`history.json.gz` contains 4,111 projection-relevant events from the original
9,788-event history, including all 3,791 process measurement events, 24 attempts,
24 command results and 21 artifact registrations. It is UTF-8 JSON compressed
with gzip, not a database. `manifest.json` records counts and its SHA-256.

Timestamps, execution identities, module versions/hashes, numeric observations,
measurement weights, outcomes and ordering are retained. Host-only resource
samples and event types unused by dashboard projections are omitted. The
experiment ID is replaced with the existing test fixture ID `exp-test`.
Host names, process identities, module settings, original template YAML,
process output, result bodies and command response bodies are omitted.
Only the template fields needed for projection calculations are retained.
Artifact paths remain experiment-relative; no artifact files are required.

`expected.json` is a frozen reference, not calculated by the test from its cache.
Summary, forecast and all 42 measurement aggregates were checked for exact
equality against the full original journal before extraction. Screen record
fields are captured from the corresponding full-history projection. Module
counts, retries and nearest-rank p50/p95 were independently reduced from the
recorded attempts. The fixture covers eight module identities.

The regression replays these events into a disposable journal, builds the
actual cache with a ten-event RAM window, and compares its outputs to the
reference. It requires no original experiment, network, Docker invocation or
machine-specific path. The same test runs on Windows and Linux; existing
temporary-directory cleanup selects any necessary platform behavior.

Run from a checkout using its uv environment:

```text
uv run --locked python -m unittest tests.dashboard_tests.test_historycache_real -v
```

Docker Desktop is only an external, manually started environment for Linux
validation. Neither this test nor its fixtures starts or controls Docker.
This extracted fixture does not cover original large-payload hydration,
host-resource monitoring, source cursor continuity or late command reconciliation;
those require the separate scenarios in the branch test plan.
