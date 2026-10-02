"""Archive client configuration; construction performs no HTTP requests."""

import httpx
from pydantic import BaseModel, ConfigDict, field_validator

from core.models.values import PositiveNumber


class FilerConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    filer_url: str
    max_archive_gb: PositiveNumber = 5
    timeout: PositiveNumber = 60

    @field_validator("filer_url", mode="before")
    @classmethod
    def validate_filer_url(cls, value: object) -> str:
        if not isinstance(value, str):
            raise TypeError("filer_url must be an HTTP(S) URL.")
        try:
            url = httpx.URL(value)
        except httpx.InvalidURL as error:
            raise ValueError("Invalid Filer URL.") from error
        if url.scheme not in {"http", "https"} or not url.host:
            raise ValueError("filer_url must be an HTTP(S) URL.")
        return value
