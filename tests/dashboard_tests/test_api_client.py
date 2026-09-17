"""System API transport failures, payload boundaries and recovery."""

import asyncio
import gzip
import json
import os
import threading
import unittest
from http.server import ThreadingHTTPServer
from unittest.mock import patch

import httpx

from dashboard.api_client import SystemAPIClient, SystemAPIError
from tests.dashboard_tests.fixtures.system_api import FixtureHandler
from tests.dashboard_tests.helpers import FIXTURES, ResponseStream, settings_document


class SystemAPIClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.requests = []
        self.responses = []
        self.client = SystemAPIClient(
            settings_document(
                system_api_url="http://system.invalid/api/", request_timeout_seconds=0.1
            )
        )
        self.client._client = httpx.AsyncClient(
            transport=httpx.MockTransport(self.respond), follow_redirects=False
        )
        self.addAsyncCleanup(self.client.close)

    async def respond(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def test_no_connection_when_unconfigured(self) -> None:
        self.client.base_url = None
        with self.assertRaises(SystemAPIError) as caught:
            await self.client.read("compute")
        self.assertEqual(caught.exception.code, "not_configured")
        self.assertEqual(self.requests, [])

    async def test_reads_resource_fixture_and_passes_query(self) -> None:
        expected = json.loads((FIXTURES / "resources.json").read_text())
        self.responses.append(httpx.Response(200, json=expected))
        actual = await self.client.read(
            "compute", {"since": "2026-09-15T10:00:00+00:00"}
        )
        self.assertEqual(actual, expected)
        self.assertEqual(self.requests[0].url.path, "/api/compute")
        self.assertEqual(
            self.requests[0].url.params["since"], "2026-09-15T10:00:00+00:00"
        )
        self.assertIsNone(actual["metrics"]["vram"]["value"])

    async def test_preserves_empty_list_and_contextual_journal_page(self) -> None:
        event_page = json.loads((FIXTURES / "events.json").read_text())
        for expected in [{"items": []}, event_page]:
            self.responses.append(httpx.Response(200, json=expected))
            self.assertEqual(
                await self.client.read("experiments/exp-test/events"), expected
            )

    async def test_missing_list_is_preserved_for_the_view_to_reject(self) -> None:
        self.responses.append(httpx.Response(200, json={"observed_at": None}))
        result = await self.client.read("experiments")
        self.assertNotIn("items", result)

    async def test_closed_client_reports_not_connected(self) -> None:
        underlying = self.client._client
        await self.client.close()
        await self.client.close()
        self.assertTrue(underlying.is_closed)
        with self.assertRaises(SystemAPIError) as caught:
            await self.client.read("compute")
        self.assertEqual(caught.exception.code, "not_connected")

    async def test_http_errors_do_not_become_empty_success(self) -> None:
        for status, code in [
            (404, "unavailable"),
            (501, "unavailable"),
            (401, "upstream_error"),
            (500, "upstream_error"),
            (503, "upstream_error"),
        ]:
            with self.subTest(status=status):
                self.responses.append(
                    httpx.Response(
                        status, json={"error": {"code": "resource_journal_corrupted"}}
                    )
                )
                with self.assertRaises(SystemAPIError) as caught:
                    await self.client.read("compute")
                self.assertEqual(caught.exception.code, code)

    async def test_redirect_is_not_followed(self) -> None:
        self.responses.append(
            httpx.Response(302, headers={"Location": "http://other.invalid/private"})
        )
        with self.assertRaises(SystemAPIError):
            await self.client.read("compute")
        self.assertEqual(len(self.requests), 1)

    async def test_connection_and_timeout_failures_recover_on_next_read(self) -> None:
        failures = [
            httpx.ConnectError("refused"),
            httpx.ReadError("connection reset"),
            httpx.RemoteProtocolError("truncated transfer"),
            httpx.ConnectTimeout("connect timeout"),
            httpx.ReadTimeout("read timeout"),
            httpx.PoolTimeout("pool timeout"),
        ]
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                self.responses.extend(
                    [
                        httpx.Response(200, json={"metrics": {"cpu": {"value": 34}}}),
                        failure,
                        httpx.Response(200, json={"metrics": {"cpu": {"value": 46}}}),
                    ]
                )
                self.assertEqual(
                    (await self.client.read("compute"))["metrics"]["cpu"]["value"], 34
                )
                with self.assertRaises(SystemAPIError) as caught:
                    await self.client.read("compute")
                self.assertEqual(
                    caught.exception.code,
                    "timeout"
                    if isinstance(failure, httpx.TimeoutException)
                    else "connection_error",
                )
                self.assertEqual(
                    (await self.client.read("compute"))["metrics"]["cpu"]["value"], 46
                )

    async def test_rejects_corrupt_json_wrong_root_and_nonfinite_values(self) -> None:
        for content in [
            b"{",
            b"<html>error</html>",
            b"[]",
            b"null",
            b"12",
            b'"text"',
            b"\xff",
            b'{"value":NaN}',
            b'{"nested":{"value":Infinity}}',
            b'{"value":1e999}',
        ]:
            with self.subTest(content=content):
                self.responses.append(httpx.Response(200, content=content))
                with self.assertRaises(SystemAPIError) as caught:
                    await self.client.read("compute")
                self.assertEqual(caught.exception.code, "invalid_response")

    async def test_accepts_exact_response_limit_and_rejects_next_byte(self) -> None:
        data = b'{"value":123}'
        self.client.max_bytes = len(data)
        self.responses.append(
            httpx.Response(200, stream=ResponseStream([data[:5], data[5:]]))
        )
        self.assertEqual(await self.client.read("compute"), {"value": 123})
        stream = ResponseStream([data, b" "])
        self.responses.append(httpx.Response(200, stream=stream))
        with self.assertRaises(SystemAPIError) as caught:
            await self.client.read("compute")
        self.assertEqual(caught.exception.code, "response_too_large")
        self.assertTrue(stream.closed)

    async def test_limit_applies_to_decoded_compressed_body(self) -> None:
        content = json.dumps({"value": "x" * 4000}).encode()
        compressed = gzip.compress(content)
        self.client.max_bytes = 500
        self.assertLess(len(compressed), self.client.max_bytes)
        self.responses.append(
            httpx.Response(
                200,
                headers={"Content-Encoding": "gzip"},
                stream=ResponseStream([compressed]),
            )
        )
        with self.assertRaises(SystemAPIError) as caught:
            await self.client.read("compute")
        self.assertEqual(caught.exception.code, "response_too_large")

    async def test_total_deadline_covers_streaming_not_just_headers(self) -> None:
        stream = ResponseStream([b'{"value":', b"1}"], delay=0.2)
        self.responses.append(httpx.Response(200, stream=stream))
        with self.assertRaises(SystemAPIError) as caught:
            await asyncio.wait_for(self.client.read("compute"), 2)
        self.assertEqual(caught.exception.code, "timeout")
        self.assertTrue(stream.closed)

    async def test_partial_transfer_failure_does_not_publish_partial_measurements(
        self,
    ) -> None:
        stream = ResponseStream(
            [b'{"metrics":{"cpu":'], failure=httpx.ReadError("reset")
        )
        self.responses.append(httpx.Response(200, stream=stream))
        with self.assertRaises(SystemAPIError):
            await self.client.read("compute")
        self.assertTrue(stream.closed)

    async def test_request_cancellation_releases_stream_and_propagates(self) -> None:
        stream = ResponseStream([b"{}"], delay=1)
        self.responses.append(httpx.Response(200, stream=stream))
        task = asyncio.create_task(self.client.read("compute"))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(stream.closed)

    async def test_real_http_client_ignores_environment_proxy_and_closes(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        client = SystemAPIClient(
            settings_document(
                system_api_url=f"http://127.0.0.1:{server.server_port}/api/"
            )
        )
        try:
            with patch.dict(
                os.environ,
                {
                    "HTTP_PROXY": "http://127.0.0.1:1",
                    "http_proxy": "http://127.0.0.1:1",
                    "NO_PROXY": "",
                    "no_proxy": "",
                },
            ):
                await client.open()
                result = await client.read("compute")
                self.assertEqual(result["metrics"]["cpu"]["value"], 31)
            underlying = client._client
            await client.close()
            self.assertTrue(underlying.is_closed)
        finally:
            await client.close()
            await asyncio.to_thread(server.shutdown)
            server.server_close()
            thread.join(1)
