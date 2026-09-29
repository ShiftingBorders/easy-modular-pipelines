"""Maintainer-approved T01-T06: conditional failures and result contracts."""

import asyncio
import json
import unittest
from pathlib import Path
from uuid import uuid4

from core.runner_utils.experimentrunner import ExperimentRunner
from tests.helpers.dag import DagWorkspace, wait_until


class ConditionalStageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.workspace = DagWorkspace()
        self.addCleanup(self.workspace.close)
        fixture = Path(__file__).resolve().parents[1] / "helpers/conditional_stage.py"
        self.conditional = self.workspace.module(
            "condition", source=fixture, stage_kind="conditional"
        )
        self.ordinary = self.workspace.module("ordinary", source=fixture)
        self.probe = self.workspace.module("probe")

    async def _run_case(
        self, settings, *, returns_data=False, on_exhausted="pause", conditional=True
    ):
        before = self.workspace.stage(
            self.probe, settings={"label": "before"}, timeout=30
        )
        condition = self.workspace.stage(
            self.conditional if conditional else self.ordinary,
            settings=settings,
            retries=1,
            on_exhausted=on_exhausted,
            timeout=30,
        )
        if conditional:
            condition["returns_data"] = returns_data
        after = self.workspace.stage(
            self.probe, settings={"label": "after"}, timeout=30
        )
        template = self.workspace.template([before, condition, after])
        template["start_timeout"] = 10
        template["unknown_state"]["timeout_seconds"] = 5
        path = self.workspace.write_template(template)
        runner = ExperimentRunner(self.workspace.root, self.workspace.manager)
        try:
            await runner.run(path)

            def finished():
                state = runner.get_state()
                terminal = state["phase"] in ("completed", "failed")
                paused = state["phase"] == "waiting" and state["mode"] == "paused"
                return state if terminal or paused else None

            observed = await wait_until(finished, timeout=60)
            if observed["phase"] in ("completed", "failed"):
                await asyncio.wait_for(asyncio.shield(runner._task), 30)
            events = self.workspace.events(runner)
            attempts = [
                event
                for event in events
                if event["event_type"] == "stage.finished"
                and event["context"].get("stage_id") == condition["stage_id"]
            ]
            seed = next(
                event["data"]["result"]["data"]
                for event in events
                if event["event_type"] == "stage.finished"
                and event["context"].get("stage_id") == before["stage_id"]
            )
            trace = runner._state.experiment_directory / "shared_data/trace.jsonl"
            starts = [
                json.loads(line)
                for line in trace.read_text(encoding="utf-8").splitlines()
            ]
            return {
                "phase": observed["phase"],
                "runner_error": observed["error"],
                "attempts": attempts,
                "retry_count": runner._state.stage_retry_counts.get(
                    condition["stage_id"], 0
                ),
                "seed": seed,
                "after_starts": [
                    item
                    for item in starts
                    if item["event"] == "start"
                    and item["stage_id"] == after["stage_id"]
                ],
                "errors": [
                    event
                    for event in events
                    if event["event_type"] == "error.recorded"
                    and event["context"].get("stage_id") == condition["stage_id"]
                ],
            }
        finally:
            try:
                await runner.stop()
            finally:
                await runner.close()

    def _assert_exhausted_policy(self, result, policy, *, error_code=None):
        self.assertEqual(
            result["phase"],
            {"pause": "waiting", "stop": "failed", "skip": "completed"}[policy],
        )
        self.assertEqual(result["retry_count"], 1)
        self.assertEqual(
            [event["context"]["attempt_number"] for event in result["attempts"]],
            [1, 2],
            result["runner_error"],
        )
        self.assertEqual(len(result["after_starts"]), int(policy == "skip"))
        if result["after_starts"]:
            self.assertIsNone(result["after_starts"][0]["input"])
        for event in result["attempts"]:
            self.assertEqual(event["data"]["outcome"], "failed")
            response = event["data"]["result"]
            self.assertEqual(response["result"], "fail")
            self.assertNotIn("dag_decision", response.get("execution", {}))
            if error_code is not None:
                self.assertEqual(response["data"]["reason"], error_code)

    async def test_crash_uses_the_ordinary_stage_retry_policy(self):
        """T01: real process crashes retry and exhaust like ordinary stages."""
        for policy in ("pause", "stop", "skip"):
            for conditional in (False, True):
                with self.subTest(policy=policy, conditional=conditional):
                    result = await self._run_case(
                        {"crash_attempts": 2},
                        on_exhausted=policy,
                        conditional=conditional,
                    )
                    self._assert_exhausted_policy(result, policy)
        for conditional in (False, True):
            with self.subTest(recovers=True, conditional=conditional):
                result = await self._run_case(
                    {"crash_attempts": 1}, conditional=conditional
                )
                self.assertEqual(result["phase"], "completed")
                self.assertEqual(result["retry_count"], 1)
                self.assertEqual(
                    [event["data"]["result"]["result"] for event in result["attempts"]],
                    ["fail", "success"],
                )
                self.assertEqual(len(result["after_starts"]), 1)

    async def test_invalid_command_uses_configured_error_policy(self):
        """T02: an unsupported command fails without becoming a DAG action."""
        for policy in ("pause", "stop", "skip"):
            with self.subTest(policy=policy):
                result = await self._run_case(
                    {"decision": {"command": "unsupported"}}, on_exhausted=policy
                )
                self._assert_exhausted_policy(
                    result, policy, error_code="invalid_conditional_result"
                )

    async def test_move_to_nonexistent_stage_uses_configured_error_policy(self):
        """T03: a well-formed but absent stage UUID cannot move the cursor."""
        for policy in ("pause", "stop", "skip"):
            with self.subTest(policy=policy):
                result = await self._run_case(
                    {"decision": {"command": "move", "stage_id": str(uuid4())}},
                    on_exhausted=policy,
                )
                self._assert_exhausted_policy(
                    result, policy, error_code="invalid_conditional_result"
                )

    async def test_disabled_data_is_ignored_and_logged_without_failing(self):
        """T04: unexpected payload logs an error but preserves successful flow."""
        for payload in ({"unexpected": "discard this"}, None):
            with self.subTest(payload=payload):
                result = await self._run_case({"decision": {"data": payload}})
                self.assertEqual(result["phase"], "completed")
                self.assertEqual(result["retry_count"], 0)
                self.assertEqual(len(result["attempts"]), 1)
                event = result["attempts"][0]
                self.assertEqual(event["data"]["outcome"], "succeeded")
                self.assertEqual(event["data"]["result"]["data"], result["seed"])
                self.assertEqual(len(result["after_starts"]), 1)
                self.assertEqual(result["after_starts"][0]["input"], result["seed"])
                errors = [
                    error
                    for error in result["errors"]
                    if error["data"]["error_code"] == "conditional_unexpected_data"
                ]
                self.assertEqual(len(errors), 1)
                self.assertEqual(
                    errors[0]["context"]["attempt_id"], event["context"]["attempt_id"]
                )

    async def test_missing_required_data_stops_only_after_retries(self):
        """T05: absence forces stop on exhaustion; supplied null is not absence."""
        for decision in (None, {}):
            for policy in ("pause", "stop", "skip"):
                with self.subTest(decision=decision, policy=policy):
                    result = await self._run_case(
                        {"decision": decision}, returns_data=True, on_exhausted=policy
                    )
                    self._assert_exhausted_policy(
                        result, "stop", error_code="conditional_missing_data"
                    )
        result = await self._run_case({"decision": {"data": None}}, returns_data=True)
        self.assertEqual(result["phase"], "completed")
        self.assertEqual(result["retry_count"], 0)
        self.assertEqual(len(result["attempts"]), 1)
        self.assertEqual(len(result["after_starts"]), 1)

    async def test_invalid_response_structure_uses_configured_error_policy(self):
        """T06: bad envelopes, decision types, and fields follow ordinary policy."""
        cases = (
            {"raw_result": {"result": "success"}},
            {"decision": ["not a decision object"]},
            {"decision": {"command": "pause", "unexpected": True}},
        )
        for settings in cases:
            for policy in ("pause", "stop", "skip"):
                with self.subTest(settings=settings, policy=policy):
                    result = await self._run_case(settings, on_exhausted=policy)
                    self._assert_exhausted_policy(result, policy)
