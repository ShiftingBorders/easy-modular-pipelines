"""Bounded HTTP reads from the system API; no runtime imports or local DB access."""

import asyncio
import json
import os

import httpx


class SystemAPIError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 503) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.upstream_code: str | None = None


class SystemAPIClient:
    def __init__(self, settings: dict) -> None:
        self.base_url = settings["system_api_url"]
        self.timeout = settings["request_timeout_seconds"]
        self.max_bytes = settings["max_response_bytes"]
        self.token_env = settings.get("system_api_token_env")
        self._client: httpx.AsyncClient | None = None

    async def open(self) -> None:
        headers = {}
        if self.token_env is not None:
            token = os.environ.get(self.token_env)
            if not token or any(not 33 <= ord(character) <= 126 for character in token):
                raise ValueError(
                    f"Set {self.token_env} to a bearer token without whitespace."
                )
            headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=False,
            trust_env=False,
            limits=httpx.Limits(max_connections=12, max_keepalive_connections=6),
            headers=headers,
        )

    async def read(self, path: str, params: dict | None = None) -> dict:
        return await self._request("GET", path, params=params)

    async def submit(self, document: dict) -> dict:
        return await self._request("POST", "commands", document=document)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        document: dict | None = None,
    ) -> dict:
        if self.base_url is None:
            raise SystemAPIError(
                "not_configured", "Cannot connect to the system. It may be offline."
            )
        if self._client is None:
            raise SystemAPIError(
                "not_connected", "Cannot connect to the system. It may be offline."
            )
        try:
            async with asyncio.timeout(self.timeout):
                async with self._client.stream(
                    method, self.base_url + path, params=params, json=document
                ) as response:
                    payload = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(payload) + len(chunk) > self.max_bytes:
                            raise SystemAPIError(
                                "response_too_large",
                                "System API response exceeds the configured limit.",
                                502,
                            )
                        payload.extend(chunk)
                    if response.status_code not in {200, 202}:
                        unavailable = response.status_code in {404, 501}
                        message = (
                            "This system API resource is unavailable."
                            if unavailable
                            else f"System API returned HTTP {response.status_code}."
                        )
                        upstream_code = None
                        try:
                            failure = json.loads(payload)
                            detail = (
                                failure.get("error")
                                if isinstance(failure, dict)
                                else None
                            )
                            if isinstance(detail, dict):
                                if isinstance(detail.get("message"), str):
                                    message = detail["message"]
                                if isinstance(detail.get("code"), str):
                                    upstream_code = detail["code"]
                        except (ValueError, UnicodeError):
                            pass
                        status = (
                            response.status_code
                            if unavailable or response.status_code in {401, 403, 409}
                            else 502
                        )
                        error = SystemAPIError(
                            "unavailable" if unavailable else "upstream_error",
                            message,
                            status,
                        )
                        error.upstream_code = upstream_code
                        raise error
            document = json.loads(payload)
            if not isinstance(document, dict):
                raise TypeError("Expected an object.")
            json.dumps(document, allow_nan=False)
            return document
        except (TimeoutError, httpx.TimeoutException) as error:
            raise SystemAPIError(
                "timeout", "The system did not respond in time.", 504
            ) from error
        except httpx.HTTPError as error:
            raise SystemAPIError(
                "connection_error", "Cannot connect to the system. It may be offline."
            ) from error
        except (TypeError, ValueError, UnicodeError) as error:
            raise SystemAPIError(
                "invalid_response", "System API returned invalid JSON.", 502
            ) from error

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
