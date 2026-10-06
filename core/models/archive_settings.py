"""Size and compression limits for experiment archive operations."""

from pydantic import BaseModel, ConfigDict, Field

from core.models.values import NonnegativeInteger, PositiveInteger, SchemaVersionOne


class ArchiveConfiguration(BaseModel):
    """Archive size limits in bytes, member limit, and compression settings.

    Args:
        schema_version: Persisted document format version; only the versions
            declared by this model are accepted.
        max_archive_bytes: Maximum compressed archive size in bytes.
        max_unpacked_bytes: Maximum total unpacked payload bytes.
        max_members: Maximum archive member count, including directories and the
            manifest.
        max_manifest_bytes: Maximum encoded manifest size in bytes.
        max_decompression_memory_bytes: Maximum memory bytes the XZ decoder may
            allocate.
        min_free_bytes: Nonnegative free-space reserve in bytes required before
            storage writes.
        compression_preset: XZ compression preset from 0 through 9.
    """
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: SchemaVersionOne
    max_archive_bytes: PositiveInteger
    max_unpacked_bytes: PositiveInteger
    max_members: PositiveInteger
    max_manifest_bytes: PositiveInteger
    max_decompression_memory_bytes: PositiveInteger
    min_free_bytes: NonnegativeInteger
    compression_preset: NonnegativeInteger = Field(le=9)
