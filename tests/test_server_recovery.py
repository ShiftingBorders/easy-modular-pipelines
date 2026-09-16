"""Approved B/D/G: saved native identities and explicit recovery after owner failure."""

import asyncio
import os

import yaml

from core.experimentassembler import ExperimentAssembler, find_experiment
from core.runner_utils.runtimeio import process_identity, read_json, write_json
from core.runner_utils.state import RunnerStateStore
from core.serverruntime import recovery_candidates
from tests.helpers.dag import process_running, terminate_owned, wait_until
from tests.helpers.http_runtime import ServerTestCase


class ServerRecoveryTests(ServerTestCase):
    async def test_saved_owners_distinguish_live_pid_reuse_boot_and_foreign_host(self):
        state = await self.w.prepare()
        identity = process_identity(os.getpid())
        store = RunnerStateStore()
        cases = (
            (identity, True),
            ({**identity, "created_at_os": identity["created_at_os"] + 1}, False),
            ({**identity, "boot_id": "another-boot"}, False),
            ({**identity, "host_id": "another-host"}, True),
            (None, False),
        )
        for owner, expected in cases:
            with self.subTest(owner=owner):
                state.owner_identity = owner
                store.save(state)
                self.assertEqual(
                    recovery_candidates(self.w.source),
                    [state.experiment_id] if expected else [],
                )
        state.owner_identity = None
        store.save(state)

    async def test_assembler_draft_corrupt_state_and_unfinished_restore_are_distinct(
        self,
    ):
        template = self.w.template()
        path = self.w.source / "draft.yaml"
        path.write_text(yaml.safe_dump(template), encoding="utf-8")
        assembler = ExperimentAssembler(self.w.source, self.w.manager)
        state = await assembler.assemble(path, "draft")
        self.assertEqual(recovery_candidates(self.w.source), [])
        store = RunnerStateStore()
        store.save(state)
        self.assertEqual(recovery_candidates(self.w.source), [])
        (state.experiment_directory / "runner/state.json").write_text(
            "{", encoding="utf-8"
        )
        self.assertEqual(recovery_candidates(self.w.source), ["draft"])
        store.save(state)
        marker = (
            self.w.source
            / "controller/restore_transactions"
            / (state.experiment_directory.name + ".json")
        )
        write_json(marker, {"phase": "restoring"})
        self.assertEqual(recovery_candidates(self.w.source), ["draft"])
        write_json(marker, {"phase": "complete"})
        self.assertEqual(recovery_candidates(self.w.source), [])

    async def test_dead_controller_requires_explicit_recovery_before_continuation(self):
        server = await self.start_server()
        state = await server.launch(self.w.template(services=True))
        experiment_id = state["experiment_id"]
        snapshot = await server.command("snapshot", {"label": "before crash"})
        self.assertEqual(snapshot["result"], "success", snapshot)
        health = await server.get("/health")
        server.observe_children()
        terminate_owned(health["controller"])
        await wait_until(lambda: not process_running(health["controller"]["pid"]))
        # Do not use fallback fixture cleanup: recover must deal with the old participants.
        (server.control / "stop").touch()
        await asyncio.wait_for(server.process.wait(), 12)
        fresh = await self.start_server()
        self.assertEqual(
            (await fresh.get("/state"))["recovery_required"], [experiment_id]
        )
        blocked = await fresh.command("resume")
        self.assertEqual(blocked["error"]["code"], "invalid_state", blocked)
        client = await self.start_cli(fresh, ["recover", experiment_id])
        code, _out, error = await client.finish()
        self.assertEqual(code, 0, error)
        self.assertEqual((await fresh.get("/state"))["recovery_required"], [])
        rollback = await fresh.command(
            "rollback", {"snapshot_id": snapshot["data"]["snapshot_id"]}
        )
        self.assertEqual(rollback["result"], "success", rollback)
        self.assertEqual((await fresh.command("resume"))["result"], "success")
        finished = await fresh.wait_state("completed")
        self.assertEqual(finished["experiment_id"], experiment_id)
        self.assertTrue(finished["snapshot"]["valid"])
        root = find_experiment(self.w.source, experiment_id)
        self.assertEqual(read_json(root / "runner/state.json")["phase"], "completed")

    async def test_ctrl_c_of_http_owner_performs_orderly_shutdown(self):
        server = await self.start_server()
        await server.launch(self.w.template(services=True))
        server.observe_children()
        (server.control / "interrupt").touch()
        await asyncio.wait_for(server.process.wait(), 20)
        for identity in server.owned.values():
            self.assertFalse(process_running(identity["pid"]), identity)
        self.assertEqual(recovery_candidates(self.w.source), [])
