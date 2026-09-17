"""Approved service_integration.md A: whole templates and actual module trees."""

import copy
import os
import unittest
from pathlib import Path
from uuid import UUID, uuid4

from core.experimentassembler import ExperimentAssembler
from tests.helpers.service_integration import ServiceDagWorkspace


class ServiceAssemblyTests(unittest.IsolatedAsyncioTestCase):
    async def test_service_reference_nodes_share_code_and_keep_call_settings_separate(
        self,
    ):
        """F14-F16: a service is declared once and may own several DAG calls."""
        service = self.w.service()
        service["settings"]["startup_only"] = 1
        nodes = []
        for value in (2, 3):
            nodes.append(
                {
                    "stage_id": str(uuid4()),
                    "service_id": service["service_id"],
                    "settings": {"call_only": value},
                    "timeout_seconds": 10,
                    "errors": {
                        "retries": 1,
                        "retry_delay_seconds": 0.1,
                        "on_exhausted": "pause",
                    },
                }
            )
        state = await self.assembler.assemble(
            self.w.write_template(self.w.template(nodes)), "service-nodes"
        )
        self.assertEqual(
            len(list((state.experiment_directory / "modules").glob("*/*/module.yaml"))),
            1,
        )
        self.assertEqual(
            [node["settings"] for node in state.template["stages"]],
            [{"call_only": 2}, {"call_only": 3}],
        )
        self.assertEqual(state.template["services"][0]["settings"]["startup_only"], 1)
        self.assembler.check_modules(state)

    async def test_invalid_service_references_and_old_template_have_no_assembly_side_effects(
        self,
    ):
        """F15/H21: reject old schema, unknown reference and duplicate module fields early."""
        service = self.w.service()
        node = {
            "stage_id": str(uuid4()),
            "service_id": service["service_id"],
            "settings": {},
            "timeout_seconds": 10,
            "errors": {
                "retries": 0,
                "retry_delay_seconds": 0.1,
                "on_exhausted": "pause",
            },
        }
        template = self.w.template([node])
        candidates = []
        for update in ({"service_id": str(uuid4())}, {"module": service["module"]}):
            changed = copy.deepcopy(template)
            changed["stages"][0].update(update)
            candidates.append(changed)
        candidates.append({**template, "schema_version": 1})
        for index, candidate in enumerate(candidates):
            with self.subTest(index=index), self.assertRaises((ValueError, TypeError)):
                await self.assembler.assemble(
                    self.w.write_template(candidate), f"rejected-{index}"
                )
        self.assertFalse((self.w.root / "experiments.json").exists())

    async def asyncSetUp(self):
        self.w = ServiceDagWorkspace()
        self.addAsyncCleanup(self.w.close)
        self.assembler = ExperimentAssembler(self.w.root, self.w.manager)

    async def test_all_service_interfaces_assign_ids_and_preserve_explicit_policies(
        self,
    ):
        """A1/A2: full/socket, action/socket and action/commands assemble together."""
        w = self.w
        services = [
            w.service(),
            w.service(implementation="action"),
            w.service(implementation="action", interface="commands"),
        ]
        template = w.template(services=copy.deepcopy(services))
        del template["services"][0]["service_id"]
        state = await self.assembler.assemble(w.write_template(template), "variants")
        identifiers = [item["service_id"] for item in state.template["services"]]
        self.assertEqual(len(set(identifiers)), 3)
        for identifier in identifiers:
            UUID(identifier)
        self.assertEqual(identifiers[1:], [item["service_id"] for item in services[1:]])
        self.assertEqual(
            state.template["services"][0]["heartbeat"],
            {"interval_seconds": 1, "grace_seconds": 10},
        )
        self.assertEqual(state.template["services"][0]["command_timeout_seconds"], 30)
        self.assembler.check_modules(state)

    async def test_invalid_service_schemas_and_cross_role_ids_leave_no_build(self):
        """A1/A2: bad service fields and duplicate module-data identities fail before assembly."""
        service = self.w.service()
        template = self.w.template()
        invalid = [
            {key: value for key, value in service.items() if key != missing}
            for missing in (
                "module",
                "settings",
                "heartbeat",
                "errors",
                "state_required",
            )
        ]
        invalid.extend(
            {**service, field: value}
            for field, value in (
                ("extra", True),
                ("service_id", "invalid"),
                ("command_timeout_seconds", True),
                ("state_required", 0),
                ("heartbeat", {"interval_seconds": 0, "grace_seconds": 10}),
                ("on_command_timeout", "skip"),
                (
                    "errors",
                    {"retries": -1, "retry_delay_seconds": 1, "on_exhausted": "pause"},
                ),
                ("service_id", template["stages"][0]["stage_id"]),
            )
        )
        for index, definition in enumerate(invalid):
            with self.subTest(index=index), self.assertRaises((ValueError, TypeError)):
                await self.assembler.assemble(
                    self.w.write_template({**template, "services": [definition]}),
                    f"invalid-{index}",
                )
        self.assertFalse((self.w.root / "experiments.json").exists())

    async def test_shared_service_code_is_copied_once_and_each_hash_is_checked(self):
        """A3: two definitions share code, while conflicting hashes cannot reuse a checked copy."""
        first, second = self.w.service(), self.w.service()
        second["module"] = copy.deepcopy(first["module"])
        template = self.w.template(services=[first, second])
        state = await self.assembler.assemble(self.w.write_template(template), "shared")
        self.assertEqual(
            len(list((state.experiment_directory / "modules").glob("*/*/module.yaml"))),
            2,
        )
        conflicting = copy.deepcopy(template)
        conflicting["services"][1]["module"]["hash"] = "0" * 64
        with self.assertRaises(ValueError):
            await self.assembler.assemble(
                self.w.write_template(conflicting), "conflicting"
            )
        self.assertEqual(
            list((self.w.root / "experiments").iterdir()), [state.experiment_directory]
        )

    async def test_role_and_interface_mismatches_remove_only_the_failed_build(self):
        """A2/A4: the schema and module.yaml must agree before publishing an experiment."""
        service = self.w.service()
        template = self.w.template()
        valid = await self.assembler.assemble(self.w.write_template(template), "valid")
        wrong_stage = copy.deepcopy(template)
        wrong_stage["stages"][0]["module"] = service["module"]
        wrong_service = copy.deepcopy(template)
        wrong_service["services"][0]["module"] = template["stages"][0]["module"]
        no_policy = copy.deepcopy(template)
        no_policy["services"] = [
            {key: service[key] for key in ("service_id", "module", "settings")}
        ]
        for index, candidate in enumerate((wrong_stage, wrong_service, no_policy)):
            with self.subTest(index=index), self.assertRaises(ValueError):
                await self.assembler.assemble(
                    self.w.write_template(candidate), f"wrong-{index}"
                )
        self.assertEqual(
            list((self.w.root / "experiments").iterdir()), [valid.experiment_directory]
        )

    async def test_foreign_cwd_resolves_resources_and_keeps_source_service_code_unchanged(
        self,
    ):
        """A4: service code and config-relative resources do not depend on the caller's cwd."""
        service = self.w.service()
        template = self.w.template()
        path = self.w.write_template(template, "settings/experiment.yaml")
        (path.parent / "input.bin").write_bytes(b"resource")
        template["resources"] = [{"name": "input", "path": "input.bin", "hash": None}]
        self.w.write_template(template, "settings/experiment.yaml")
        before = Path.cwd()
        try:
            os.chdir(self.w.root)
            state = await self.assembler.assemble(path, "foreign-cwd")
        finally:
            os.chdir(before)
        self.assertEqual(
            (state.experiment_directory / "shared_data/resources/input").read_bytes(),
            b"resource",
        )
        module = service["module"]
        self.assertEqual(
            self.w.manager.module_hash(
                module["name"],
                target_folder=self.w.root / "modules" / module["name"] / "1",
            ),
            module["hash"],
        )
