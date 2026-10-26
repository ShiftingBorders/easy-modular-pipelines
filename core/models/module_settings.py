"""Strict performance settings for module hashing, independent of storage."""

from pydantic import BaseModel, ConfigDict, Field

from core.models.values import NonnegativeInteger, PositiveInteger


class ModuleHashingSettings(BaseModel):
    """Bounded hashing buffer sizes in bytes and maximum concurrent readers.

    Args:
        hash_small_file_threshold_bytes: Files at or below this byte size use
            the small-file hashing path. Defaults to 65536.
        hash_chunk_size_bytes: Maximum bytes read per hashing chunk. Defaults to
            65536.
        hash_max_workers: Maximum concurrent hashing readers, also bounding
            buffers and open files. Defaults to 2.
    """
    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, hide_input_in_errors=True
    )

    # At most 64 MiB per reader; worker count also bounds open files and buffers.
    hash_small_file_threshold_bytes: NonnegativeInteger = Field(
        default=65536, le=64 * 1024 * 1024
    )
    hash_chunk_size_bytes: PositiveInteger = Field(default=65536, le=64 * 1024 * 1024)
    hash_max_workers: PositiveInteger = Field(default=2, le=32)
