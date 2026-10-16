"""Owned cache-reader metadata retained separately from the public JSON dataset."""

from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path

from core.journal.history_cache import JournalHistoryCache
from core.models.journal_cache import (
    CacheGap,
    CacheIdentity,
    CachePublication,
    CacheReaderContext,
    CacheSource,
    JournalBoundary,
)
from core.models.updates import _update_model
from core.primitives.json_values import JsonObject


@dataclass
class CacheReader:
    reader: JournalHistoryCache
    identity: CacheIdentity
    file_key: tuple[int, int]
    checked: float = 0


@dataclass
class CacheWindow:
    identity: CacheIdentity
    file_key: tuple[int, int]
    entries: OrderedDict[str, dict]
    publication: CachePublication | JournalBoundary

    @property
    def boundary(self) -> JournalBoundary | None:
        if isinstance(self.publication, JournalBoundary):
            return self.publication
        return self.publication.boundary

    @property
    def predecessor(self) -> int | None:
        if isinstance(self.publication, CachePublication):
            return self.publication.window_predecessor_cursor
        return None


@dataclass
class CachedDataset:
    source: CacheSource
    context: CacheReaderContext
    publication: CachePublication
    reader: JournalHistoryCache
    refreshed: float
    entries: list[dict] = field(default_factory=list)
    error: str | None = None
    include_cache: bool = True

    @property
    def file_key(self) -> tuple[int, int]:
        return self.source.file_key[0], self.source.file_key[1]

    def publication_in_window(self, window: CacheWindow) -> CachePublication:
        """Overlay the observed RAM suffix without changing the disk publication."""
        first = next(iter(window.entries.values()), None)
        start = first["entry"]["cursor"] if first else None
        observed = window.boundary
        target = self.publication.target_boundary
        complete = self.publication.complete
        if observed is not None:
            if observed.change_cursor > (target.change_cursor if target else 0):
                target = observed
            cached = self.publication.cached_through
            if (
                cached.cursor < observed.cursor
                or cached.change_cursor < observed.change_cursor
            ):
                complete = False
        gap = self.publication.gap
        if (
            window.predecessor is not None
            and self.publication.cached_through.cursor < window.predecessor
        ):
            gap = CacheGap.model_validate(
                {"after": self.publication.cached_through.cursor, "before": start}
            )
            complete = False
        return _update_model(
            self.publication,
            window_start_cursor=start,
            window_count=len(window.entries),
            target_boundary=target,
            complete=complete,
            gap=gap,
        )

    def document(self, publication: CachePublication | None = None) -> dict:
        """Project metadata at the existing public dataset/callback boundary."""
        document = {
            "experiment_id": self.source.experiment_id,
            "directory": Path(self.context.directory),
            "identity": self.source.identity.model_dump(),
            "file_key": self.file_key,
            "state": deepcopy(self.context.state),
            "entries": deepcopy(self.entries),
            **(publication or self.publication).model_dump(exclude_unset=True),
            "error": self.error,
            "refreshed": self.refreshed,
        }
        if self.include_cache:
            document["cache"] = self.reader
        return document


def _history_dataset(
    reader: JournalHistoryCache,
    directory: Path,
    project: str,
    state: JsonObject,
    publication: CachePublication,
    entries: list[dict],
    refreshed: float,
) -> CachedDataset:
    """Reconstruct retained metadata separately from history-reading operations."""
    available = (
        publication.cache_available
        if "cache_available" in publication.model_fields_set
        else True
    )
    return CachedDataset(
        source=CacheSource(
            version=JournalHistoryCache.SCHEMA_VERSION,
            identity=reader.identity,
            file_key=list(reader.file_key),
            experiment_id=reader.experiment_id,
        ),
        context=CacheReaderContext.model_validate(
            {"directory": str(directory), "project_root": project, "state": state}
        ),
        publication=publication,
        reader=reader,
        refreshed=refreshed,
        entries=entries,
        error=None if available else "The history cache is being initialized.",
        include_cache=bool(available),
    )
