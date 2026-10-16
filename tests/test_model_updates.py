"""Checked replacements preserve generated identities and field presence."""

import unittest
from unittest.mock import patch
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from core.models.dashboard_alerts import AlertRule
from core.models.server_commands import ServerChain, ServerCommand
from core.models.updates import _update_model


class AliasedIdentity(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True)

    identifier: str = Field(default_factory=lambda: str(uuid4()), alias="id")
    label: str = "original"


class ModelUpdateTests(unittest.TestCase):
    def test_generated_command_identity_and_omitted_factories_are_preserved(self):
        original = ServerCommand(command="pause")
        fields = set(original.model_fields_set)
        with patch(
            "core.models.server_commands.uuid4",
            side_effect=AssertionError("ID regenerated"),
        ):
            replacement = _update_model(original, command="resume")
        self.assertEqual(replacement.command_id, original.command_id)
        self.assertEqual(replacement.model_fields_set, fields)
        self.assertEqual(
            replacement.model_dump(exclude_unset=True), {"command": "resume"}
        )
        self.assertEqual(original.model_fields_set, fields)
        self.assertEqual(original.command, "pause")

    def test_nested_chain_identities_and_application_data_are_preserved(self):
        original = ServerChain.model_validate(
            {
                "commands": [{"command": "pause", "args": {"values": [1]}}],
            }
        )
        child = original.commands[0]
        child_fields = set(child.model_fields_set)
        with patch(
            "core.models.server_commands.uuid4",
            side_effect=AssertionError("ID regenerated"),
        ):
            replacement = _update_model(original, api_version=1)
        self.assertEqual(replacement.chain_id, original.chain_id)
        self.assertEqual(replacement.commands[0].command_id, child.command_id)
        self.assertEqual(replacement.commands[0].model_fields_set, child_fields)
        self.assertNotIn("chain_id", replacement.model_fields_set)
        replacement.commands[0].args["values"].append(2)
        self.assertEqual(child.args, {"values": [1]})

    def test_explicit_factory_updates_are_not_restored_to_omitted_fields(self):
        original = ServerCommand(command="pause")
        identifier = str(uuid4())
        replacement = _update_model(original, command_id=identifier, args={})
        self.assertEqual(replacement.command_id, identifier)
        self.assertIn("command_id", replacement.model_fields_set)
        self.assertIn("args", replacement.model_fields_set)
        self.assertEqual(original.model_fields_set, {"command"})

    def test_aliased_generated_identity_and_explicit_alias_updates_are_preserved(self):
        original = AliasedIdentity()
        replacement = _update_model(original, label="changed")
        self.assertEqual(replacement.identifier, original.identifier)
        self.assertNotIn("identifier", replacement.model_fields_set)
        self.assertEqual(
            replacement.model_dump(by_alias=True, exclude_unset=True),
            {"label": "changed"},
        )
        explicit = _update_model(original, id="new identity")
        self.assertEqual(explicit.identifier, "new identity")
        self.assertIn("identifier", explicit.model_fields_set)
        self.assertEqual(original.model_fields_set, set())

    def test_failed_candidate_preserves_values_presence_and_generated_identity(self):
        original = ServerCommand(command="pause")
        document = original.model_dump()
        fields = set(original.model_fields_set)
        with (
            patch(
                "core.models.server_commands.uuid4",
                side_effect=AssertionError("ID regenerated"),
            ),
            self.assertRaises(ValueError),
        ):
            _update_model(original, command="server.restart", args={"unexpected": 1})
        self.assertEqual(original.model_dump(), document)
        self.assertEqual(original.model_fields_set, fields)

    def test_replaced_children_keep_new_explicit_identity_fields(self):
        original = ServerChain.model_validate({"commands": [{"command": "pause"}]})
        identifier = str(uuid4())
        replacement = _update_model(
            original,
            commands=[
                {
                    "command": "resume",
                    "command_id": identifier,
                    "args": {},
                }
            ],
        )
        self.assertEqual(replacement.chain_id, original.chain_id)
        self.assertEqual(replacement.commands[0].command_id, identifier)
        self.assertIn("command_id", replacement.commands[0].model_fields_set)
        self.assertIn("args", replacement.commands[0].model_fields_set)
        self.assertNotIn("command_id", original.commands[0].model_fields_set)

    def test_alert_update_preserves_generated_rule_identity(self):
        original = AlertRule(name="errors", enabled=True, kind="errors", threshold=1)
        with patch(
            "core.models.dashboard_alerts.uuid4",
            side_effect=AssertionError("ID regenerated"),
        ):
            replacement = _update_model(original, threshold=2)
        self.assertEqual(replacement.id, original.id)
        self.assertNotIn("id", replacement.model_fields_set)
        self.assertEqual(replacement.document()["id"], original.id)
        self.assertEqual(original.threshold, 1)
