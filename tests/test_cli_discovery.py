"""Approved in-process CLI/API boundaries and streamed downloads Q2-18, Q2-26–33."""

import asyncio
import io
import json
import os
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx

import cli
import webserver
from core.serverruntime import ServerRuntime, load_server_settings
from tests.helpers.dag import DagWorkspace


class ArtifactStream(httpx.AsyncByteStream):
    def __init__(self, failure=None, callback=None, *, stall=False):
        self.failure = failure
        self.callback = callback
        self.closed = False
        self.stall = stall

    async def __aiter__(self):
        yield b"first"
        if self.callback is not None:
            self.callback()
        if self.stall:
            await asyncio.Event().wait()
        if self.failure is not None:
            raise self.failure
        yield b"second"

    async def aclose(self):
        self.closed = True


class DownloadTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.files = DagWorkspace()
        self.addCleanup(self.files.close)
        self.destination = self.files.root / "download.bin"
        self.client = cli.APIClient(cli.load_settings(None, {}))

    async def download(self, response):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: response)
        ) as http:
            self.client.http = http
            return await self.client.download("saved", "output", self.destination)

    def assert_clean(self):
        self.assertFalse(self.destination.exists())
        self.assertEqual(list(self.files.root.glob(".emp-download-*")), [])

    async def test_complete_stream_and_native_publication(self):
        stream = ArtifactStream()
        self.client.max_bytes = 1
        result = await self.download(
            httpx.Response(
                200,
                headers={
                    "content-type": "application/octet-stream",
                    "content-length": "11",
                },
                stream=stream,
            )
        )
        self.assertEqual(self.destination.read_bytes(), b"firstsecond")
        self.assertEqual(result["size_bytes"], 11)
        self.assertTrue(stream.closed)
        self.assertEqual(list(self.files.root.glob(".emp-download-*")), [])

    async def test_existing_destination_and_publication_race_q2_26(self):
        self.destination.write_bytes(b"keep")
        with self.assertRaises(FileExistsError):
            await self.download(httpx.Response(200, content=b"replacement"))
        self.assertEqual(self.destination.read_bytes(), b"keep")
        self.destination.unlink()
        stream = ArtifactStream(
            callback=lambda: self.destination.write_bytes(b"racing writer")
        )
        with self.assertRaises(FileExistsError):
            await self.download(
                httpx.Response(
                    200,
                    headers={"content-type": "application/octet-stream"},
                    stream=stream,
                )
            )
        self.assertEqual(self.destination.read_bytes(), b"racing writer")
        self.assertEqual(list(self.files.root.glob(".emp-download-*")), [])

    async def test_filesystem_failures_q2_27(self):
        response = lambda: httpx.Response(
            200,
            headers={"content-type": "application/octet-stream"},
            stream=ArtifactStream(),
        )
        self.destination = self.files.root / "absent/download"
        with self.assertRaises(FileNotFoundError):
            await self.download(response())
        self.destination = self.files.root / "download.bin"
        for error in (
            PermissionError("access denied"),
            OSError("hard links unsupported"),
        ):
            with (
                self.subTest(error=error),
                patch("cli.os.link", side_effect=error),
                self.assertRaises(OSError),
            ):
                await self.download(response())
            self.assert_clean()
        original = Path.open

        def open_file(path, *args, **kwargs):
            if path.name == "artifact":
                output = original(path, *args, **kwargs)
                self.addCleanup(output.close)
                return FailingOutput(output)
            return original(path, *args, **kwargs)

        with patch.object(Path, "open", open_file), self.assertRaises(OSError):
            await self.download(response())
        self.assert_clean()

    async def test_transport_timeout_and_cancellation_cleanup_q2_28_30(self):
        for failure in (
            httpx.ReadError("disconnected"),
            httpx.ReadTimeout("timeout"),
            asyncio.CancelledError(),
        ):
            stream = ArtifactStream(failure)
            expected = (
                asyncio.CancelledError
                if isinstance(failure, asyncio.CancelledError)
                else cli.ClientError
            )
            with self.subTest(failure=failure), self.assertRaises(expected):
                await self.download(
                    httpx.Response(
                        200,
                        headers={"content-type": "application/octet-stream"},
                        stream=stream,
                    )
                )
            self.assertTrue(stream.closed)
            self.assert_clean()

    async def test_bad_response_and_missing_server_file_q2_29_30(self):
        for response in (
            httpx.Response(200, headers={"content-type": "text/html"}, content=b"oops"),
            httpx.Response(
                200,
                headers={
                    "content-type": "application/octet-stream",
                    "content-length": "99",
                },
                stream=ArtifactStream(),
            ),
            httpx.Response(
                200,
                headers={
                    "content-type": "application/octet-stream",
                    "content-length": "invalid",
                },
                stream=ArtifactStream(),
            ),
            httpx.Response(500, content=b"not json"),
            httpx.Response(500, json={"error": "wrong envelope"}),
            httpx.Response(
                404, json={"error": {"code": "not_found", "message": "removed"}}
            ),
        ):
            with self.subTest(response=response), self.assertRaises(cli.ClientError):
                await self.download(response)
            self.assert_clean()

    async def test_download_deadline_after_partial_bytes_q2_28(self):
        stream = ArtifactStream(stall=True)
        self.client.timeout = 0.02
        with self.assertRaises(cli.ClientError) as error:
            await self.download(
                httpx.Response(
                    200,
                    headers={"content-type": "application/octet-stream"},
                    stream=stream,
                )
            )
        self.assertEqual(error.exception.code, "http_timeout")
        self.assertTrue(stream.closed)
        self.assert_clean()


class FailingOutput:
    def __init__(self, output):
        self.output = output

    def __enter__(self):
        return self

    def write(self, data):
        self.output.write(data[:1])
        raise OSError("disk write failed")

    def __exit__(self, *error):
        self.output.close()


class DiscoveryAPITests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.files = DagWorkspace()
        self.addCleanup(self.files.close)
        self.runtime = ServerRuntime(
            load_server_settings(overrides={"project_root": str(self.files.root)})
        )
        self.runtime.read = AsyncMock(
            return_value={"result": "success", "data": {"items": []}}
        )
        for name, value in (("runtime", self.runtime), ("api_token", "test-token")):
            context = patch.object(webserver.app.state, name, value, create=True)
            context.start()
            self.addCleanup(context.stop)

    async def request(self, method, path, **kwargs):
        headers = kwargs.pop("headers", {"Authorization": "Bearer test-token"})
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=webserver.app), base_url="http://test"
        ) as client:
            return await client.request(method, path, headers=headers, **kwargs)

    async def test_authentication_covers_new_routes_q2_32(self):
        routes = [
            "/api/experiments",
            "/api/experiments/inspect?experiment_id=x",
            "/api/snapshots",
            "/api/snapshots/inspect?snapshot_id=x",
            "/api/modules",
            "/api/modules/inspect?name=x&version=1",
            "/api/artifacts?experiment_id=x",
            "/api/artifacts/download?experiment_id=x&artifact_id=y",
            "/api/commands",
        ]
        for path in routes:
            for headers in ({}, {"Authorization": "Bearer wrong"}):
                with self.subTest(path=path, headers=headers):
                    self.assertEqual(
                        (await self.request("GET", path, headers=headers)).status_code,
                        401,
                    )
        self.assertEqual(
            (
                await self.request(
                    "POST",
                    "/api/templates/validate",
                    headers={},
                    json={"template_path": "/x"},
                )
            ).status_code,
            401,
        )
        self.runtime.read.assert_not_awaited()

    async def test_query_and_body_validation_q2_32(self):
        for path in (
            "/api/experiments?unknown=1",
            "/api/modules?unknown=1",
            "/api/artifacts?experiment_id=x&experiment_id=y",
            "/api/commands?unknown=1",
            "/api/snapshots?experiment_id=x&experiment_id=y",
        ):
            with self.subTest(path=path):
                self.assertEqual((await self.request("GET", path)).status_code, 400)
        for path in (
            "/api/experiments/inspect",
            "/api/modules/inspect?name=x",
            "/api/artifacts",
            "/api/artifacts/download?experiment_id=x",
            "/api/snapshots/inspect",
        ):
            with self.subTest(path=path):
                self.assertEqual((await self.request("GET", path)).status_code, 422)
        self.assertEqual(
            (
                await self.request(
                    "POST",
                    "/api/templates/validate",
                    content="{}",
                    headers={
                        "Authorization": "Bearer test-token",
                        "Content-Type": "text/plain",
                    },
                )
            ).status_code,
            415,
        )
        self.assertEqual(
            (
                await self.request(
                    "POST",
                    "/api/templates/validate",
                    content="{",
                    headers={
                        "Authorization": "Bearer test-token",
                        "Content-Type": "application/json",
                    },
                )
            ).status_code,
            400,
        )
        self.runtime.read.assert_not_awaited()

    async def test_receipt_response_size_q2_18(self):
        self.runtime.settings.max_response_bytes = 1
        response = await self.request("GET", "/api/commands")
        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json()["error"]["code"], "response_too_large")

    async def test_read_error_translation_q2_31(self):
        for path in (
            "/api/experiments",
            "/api/snapshots?experiment_id=x",
            "/api/artifacts?experiment_id=x",
        ):
            self.runtime.read.return_value = {
                "result": None,
                "error": {"code": "response_too_large", "message": "too large"},
            }
            with self.subTest(path=path):
                response = await self.request("GET", path)
                self.assertEqual(response.status_code, 502)
                self.assertNotIn("items", response.json())

    async def test_maintenance_read_queue_overflow_returns_http_429(self):
        self.runtime.read.return_value = {
            "result": "fail",
            "data": None,
            "error": {
                "code": "too_many_reads",
                "message": "Too many queued maintenance reads.",
            },
        }
        response = await self.request("GET", "/api/modules")
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.json()["error"]["code"], "too_many_reads")

    async def test_download_valid_path_missing_and_escaping_q2_22_30(self):
        path = self.files.root / "experiments/saved/output.bin"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"artifact")
        self.runtime.read.return_value = {
            "result": "success",
            "data": {"path": str(path)},
        }
        response = await self.request(
            "GET", "/api/artifacts/download?experiment_id=saved&artifact_id=id"
        )
        self.assertEqual(response.content, b"artifact")
        path.unlink()
        self.assertEqual(
            (
                await self.request(
                    "GET", "/api/artifacts/download?experiment_id=saved&artifact_id=id"
                )
            ).status_code,
            404,
        )
        self.runtime.read.return_value = {
            "result": "success",
            "data": {"path": str(self.files.root / "outside")},
        }
        self.assertEqual(
            (
                await self.request(
                    "GET", "/api/artifacts/download?experiment_id=saved&artifact_id=id"
                )
            ).status_code,
            400,
        )


class DiscoveryCLITests(unittest.IsolatedAsyncioTestCase):
    async def test_request_mapping_json_and_snapshot_compatibility(self):
        parser = cli.build_parser()
        identifier = str(uuid4())
        path = "C:/server/template.yaml" if os.name == "nt" else "/server/template.yaml"
        cases = [
            (["experiment", "list"], "GET", "/experiments", {}),
            (
                ["experiment", "inspect", "saved:parent"],
                "GET",
                "/experiments/inspect",
                {"experiment_id": "saved:parent"},
            ),
            (
                ["snapshot", "list", "--experiment-id", "saved"],
                "GET",
                "/snapshots",
                {"experiment_id": "saved"},
            ),
            (
                ["snapshot", "inspect", identifier],
                "GET",
                "/snapshots/inspect",
                {"snapshot_id": identifier},
            ),
            (["module", "list"], "GET", "/modules", {}),
            (
                ["module", "inspect", "--name", "demo", "--version", "1"],
                "GET",
                "/modules/inspect",
                {"name": "demo", "version": "1"},
            ),
            (
                ["artifact", "list", "--experiment-id", "saved"],
                "GET",
                "/artifacts",
                {"experiment_id": "saved"},
            ),
            (
                ["commands", "list", "--state", "failed"],
                "GET",
                "/commands",
                {"state": "failed", "limit": 100},
            ),
        ]
        for arguments, method, route, params in cases:
            client = SimpleNamespace(request=AsyncMock(return_value={"items": []}))
            output = io.StringIO()
            with self.subTest(arguments=arguments), redirect_stdout(output):
                self.assertEqual(
                    await cli.execute(
                        parser.parse_args(arguments), client, as_json=True
                    ),
                    0,
                )
            self.assertEqual(json.loads(output.getvalue()), {"items": []})
            client.request.assert_awaited_once_with(method, route, params=params)
        client = SimpleNamespace(request=AsyncMock(return_value={"valid": True}))
        with redirect_stdout(io.StringIO()):
            await cli.execute(
                parser.parse_args(["template", "validate", path]), client, as_json=True
            )
        client.request.assert_awaited_once_with(
            "POST", "/templates/validate", document={"template_path": path}
        )
        creation = cli.command_document(
            parser.parse_args(["snapshot", "--label", "before", "--wait"])
        )
        self.assertEqual(creation["command"], "snapshot")
        self.assertEqual(creation["args"], {"label": "before"})
        with self.assertRaises(ValueError):
            await cli.execute(
                parser.parse_args(["snapshot", "--wait", "list"]), client, as_json=True
            )

    async def test_client_malformed_response_and_deadline_q2_31_32(self):
        client = cli.APIClient(cli.load_settings(None, {}))
        for response in (
            httpx.Response(200, content=b"bad"),
            httpx.Response(200, json=[]),
        ):
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request, response=response: response
                )
            ) as http:
                client.http = http
                with self.assertRaises(cli.ClientError) as error:
                    await client.request("GET", "/experiments")
                self.assertEqual(error.exception.code, "invalid_response")

        async def stalled(request):
            await asyncio.Event().wait()

        client.timeout = 0.02
        async with httpx.AsyncClient(transport=httpx.MockTransport(stalled)) as http:
            client.http = http
            with self.assertRaises(cli.ClientError) as error:
                await client.request("GET", "/snapshots")
            self.assertEqual(error.exception.code, "http_timeout")
