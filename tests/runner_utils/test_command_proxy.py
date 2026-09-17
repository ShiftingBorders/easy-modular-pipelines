"""Approved K10/K11/K23: shipped proxy runs finite real child commands."""

import shutil
import unittest

from core.runner_utils.runtimeio import read_json
from tests.helpers.dag import REPOSITORY, process_running
from tests.helpers.services import ServiceWorkspace


class CommandProxyTests(unittest.IsolatedAsyncioTestCase):
    async def test_ready_after_start_pass_through_and_stop_removes_resource(self):
        workspace = ServiceWorkspace()
        self.addAsyncCleanup(workspace.close)
        service = workspace.service(implementation="action", required=False)
        code = workspace.experiment / "modules" / service["module"]["name"] / "1"
        example = REPOSITORY / "examples/service_dag/modules/command_proxy/1.0"
        for name in ("main.py", "action.py"):
            shutil.copy2(example / name, code / name)
        digest = workspace.modules.module_hash(
            service["module"]["name"], target_folder=code
        )
        workspace.hashes.remove_module_hash(service["module"]["name"], "1")
        workspace.hashes.add_module_hash(service["module"]["name"], "1", digest)
        service["module"]["hash"] = digest
        self.assertEqual(await workspace.manager.start_all(workspace.state), "ready")
        instance = workspace.state.services[service["service_id"]]
        context = read_json(instance.artifacts_directory / "context.json")
        resource = (
            workspace.experiment / "module_data" / service["service_id"] / "ready.txt"
        )
        self.assertTrue(resource.exists())
        response = await workspace.manager.request(
            workspace.state,
            instance.service_id,
            "execute",
            {
                "context": context["context"],
                "input_data": {"message": "hello"},
                "settings": {},
            },
        )
        self.assertEqual(response["data"], {"message": "hello"})
        result = await workspace.manager.stop_all(workspace.state)
        self.assertTrue(result[instance.service_id]["stopped"], result)
        self.assertFalse(resource.exists())
        self.assertFalse(process_running(instance.process_identity["pid"]))
