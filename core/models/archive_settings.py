"""Size and compression limits for experiment archive operations."""

from pydantic import BaseModel, ConfigDict, Field

from core.models.values import NonnegativeInteger, PositiveInteger, SchemaVersionOne


class ArchiveConfiguration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: SchemaVersionOne
    max_archive_bytes: PositiveInteger
    max_unpacked_bytes: PositiveInteger
    max_members: PositiveInteger
    max_manifest_bytes: PositiveInteger
    max_decompression_memory_bytes: PositiveInteger
    min_free_bytes: NonnegativeInteger
    compression_preset: NonnegativeInteger = Field(le=9)
