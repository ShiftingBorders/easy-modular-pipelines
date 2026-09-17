"""Approved G: delivery protocol and owned-process cleanup; real delivery checked by owner."""

import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from dashboard.notifications import deliver


@unittest.skipUnless(os.name == "nt", "Windows notification protocol; Linux deferred")
class NotificationTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_channels_do_not_spawn(self):
        with patch(
            "dashboard.notifications.asyncio.create_subprocess_exec", new=AsyncMock()
        ) as spawn:
            self.assertEqual(
                (await deliver("title", "body", {"desktop": False, "sound": False}))[
                    "status"
                ],
                "disabled",
            )
            spawn.assert_not_awaited()

    async def test_text_is_stdin_data_and_not_interpolated_into_command(self):
        process = SimpleNamespace(
            returncode=0, communicate=AsyncMock(return_value=(b"", b""))
        )
        text = "quotes ' and \" ; Unicode \u043f\u0440\u0438\u0432\u0435\u0442"
        with patch(
            "dashboard.notifications.asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=process),
        ) as spawn:
            result = await deliver("title", text, {"desktop": True, "sound": True})
        self.assertEqual(result["status"], "submitted")
        self.assertEqual(result["source"], "dashboard_host")
        self.assertNotIn(text, spawn.call_args.args[-1])
        payload = json.loads(process.communicate.call_args.args[0])
        self.assertEqual(payload["message"], text)
        self.assertNotEqual(spawn.call_args.kwargs["creationflags"], 0)

    async def test_launch_error_is_reported_and_cancelled_child_is_reaped(self):
        with patch(
            "dashboard.notifications.asyncio.create_subprocess_exec",
            new=AsyncMock(side_effect=OSError("no desktop")),
        ):
            self.assertEqual(
                (await deliver("title", "body", {"desktop": True}))["status"], "failed"
            )
        process = SimpleNamespace(
            returncode=None,
            communicate=AsyncMock(side_effect=asyncio.CancelledError),
            kill=Mock(),
            wait=AsyncMock(),
        )
        with (
            patch(
                "dashboard.notifications.asyncio.create_subprocess_exec",
                new=AsyncMock(return_value=process),
            ),
            self.assertRaises(asyncio.CancelledError),
        ):
            await deliver("title", "body", {"sound": True})
        process.kill.assert_called_once()
        process.wait.assert_awaited_once()
