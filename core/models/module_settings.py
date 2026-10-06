"""Strict performance settings for module hashing, independent of storage."""

from pydantic import BaseModel, ConfigDict, Field

from core.models.values import NonnegativeInteger, PositiveInteger


class ModuleHashingSettings(BaseModel):
    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, hide_input_in_errors=True
    )

    # At most 64 MiB per reader; worker count also bounds open files and buffers.
    hash_small_file_threshold_bytes: NonnegativeInteger = Field(
        default=65536, le=64 * 1024 * 1024
    )
    hash_chunk_size_bytes: PositiveInteger = Field(default=65536, le=64 * 1024 * 1024)
    hash_max_workers: PositiveInteger = Field(default=2, le=32)
