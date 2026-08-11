from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypeAlias


JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class FileMetadata:
    """Metadata used to identify and conditionally replace one stored file."""

    path: str
    size: int
    etag: str
    modified_ns: int | None = None
    last_modified: str | None = None
    content_type: str | None = None


@dataclass(frozen=True, slots=True)
class FilePage:
    """One paginated file listing."""

    files: tuple[FileMetadata, ...]
    next_cursor: str | None = None


@dataclass(frozen=True, slots=True)
class FileRange:
    """The byte interval returned for a ranged file read."""

    start: int
    end: int
    total: int


@dataclass(frozen=True, slots=True)
class FileDownload:
    """Stored file bytes and the metadata observed with the read."""

    metadata: FileMetadata
    content: bytes
    content_range: FileRange | None = None


@dataclass(frozen=True, slots=True)
class FileMutation:
    """The committed state returned by a replace or append operation."""

    path: str
    size: int
    etag: str
    offset: int | None = None
    modified_ns: int | None = None
    replayed: bool = False
    details: JsonObject = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class VersionedDocument:
    """A JSON state document and the entity tag required for remote CAS."""

    schema_version: str
    payload: JsonObject
    etag: str


@dataclass(frozen=True, slots=True)
class ObservationChange:
    """One committed observation revision in an ingest result."""

    cursor: int
    observation_time_ms: int
    market_id: str
    sample_kind: str
    revision: int


@dataclass(frozen=True, slots=True)
class ObservationIngestResult:
    """The durable outcome of one idempotent observation batch."""

    schema_version: str
    gate_id: str
    batch_id: str
    metric: str
    rows_received: int
    rows_written: int
    rows_deduplicated: int
    cursor_start: int | None
    cursor_end: int | None
    changes: tuple[ObservationChange, ...]
    details: JsonObject = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StateMutation:
    """The durable result of replacing a gate-owned JSON document."""

    schema_version: str
    etag: str
    size: int
    snapshot: str | None = None
    details: JsonObject = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RunAppendResult:
    """The committed byte interval of one gate run event."""

    schema_version: str
    offset: int
    length: int
    size: int
    details: JsonObject = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LatestResult:
    """Latest observation rows keyed by market identity."""

    schema_version: str
    metric: str
    rows: dict[str, JsonObject]
    details: JsonObject = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class HistoryPage:
    """One page from an observation history query."""

    schema_version: str
    rows: tuple[JsonObject, ...]
    present_market_ids: tuple[str, ...]
    has_more: bool
    next_after: tuple[int, str, str] | None
    details: JsonObject = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ResumeResult:
    """Resume timestamps keyed by requested market identity."""

    schema_version: str
    metric: str
    cursors: dict[str, int]
    details: JsonObject = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GateUniverse:
    """The current universe document published by one collector gate."""

    gate_id: str
    universe: JsonObject


@dataclass(frozen=True, slots=True)
class UniverseList:
    """The current universe documents published by all collector gates."""

    schema_version: str
    universes: tuple[GateUniverse, ...]


@dataclass(frozen=True, slots=True)
class FirstOpenInterestTimes:
    """The earliest stored open-interest time for each requested market identity."""

    schema_version: str
    times: dict[str, int]


@dataclass(frozen=True, slots=True)
class CommittedObservationChange:
    """One globally ordered observation revision from the recovery feed."""

    cursor: int
    gate_id: str
    batch_id: str
    metric: str
    observation_time_ms: int
    market_id: str
    sample_kind: str
    revision: int
    row: JsonObject


@dataclass(frozen=True, slots=True)
class ChangePage:
    """A resumable page of committed storage changes."""

    schema_version: str
    changes: tuple[CommittedObservationChange, ...]
    next_cursor: int
    reset_required: bool
    last_cursor: int
