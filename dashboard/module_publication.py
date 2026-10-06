"""Computed cache inventories projected at publication and aggregation boundaries."""

import sqlite3
from dataclasses import dataclass

from core.models.journal_cache import CacheIdentity, CacheSource
from core.primitives.json_values import JsonObject, JsonValue


@dataclass
class ModuleSource:
    directory: str
    complete: bool = False
    error: str | None = None
    cache: CacheSource | None = None
    version: int | None = None
    # Only forwarded into the source fingerprint/file; strict publication parsing
    # remains at its existing read boundary, including corrupt-cache errors.
    cached_through: JsonValue = None

    def document(self) -> JsonObject:
        document: JsonObject = {"directory": self.directory, "complete": self.complete}
        if self.cache is not None:
            document.update(
                journal=self.cache.identity.model_dump(),
                file_key=list(self.cache.file_key),
                cache_schema_version=self.cache.version,
                version=self.version,
                cached_through=self.cached_through,
            )
        if self.error is not None:
            document["error"] = self.error
        return document


@dataclass
class ModuleDataset:
    experiment_id: str
    name: JsonValue
    complete: bool
    identity: CacheIdentity
    connection: sqlite3.Connection

    def document(self) -> JsonObject:
        """Bind the existing public module_statistics callback's JSON input."""
        return {
            "experiment_id": self.experiment_id,
            "name": self.name,
            "complete": self.complete,
            "identity": self.identity.model_dump(),
        }
