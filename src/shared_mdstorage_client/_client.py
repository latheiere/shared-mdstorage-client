from __future__ import annotations

import re
from ipaddress import ip_address
from collections.abc import Mapping, Sequence
from typing import TypeAlias
from urllib.parse import quote, urlsplit

import httpx

from ._errors import (
    AuthenticationError,
    ConflictError,
    NotFoundError,
    ProtocolError,
    TransportError,
)
from ._identifiers import validate_structural_identifier
from ._version import __version__
from ._models import (
    ChangePage,
    CommittedObservationChange,
    FileDownload,
    FileMetadata,
    FileMutation,
    FilePage,
    FileRange,
    FirstOpenInterestTimes,
    GateUniverse,
    HistoryPage,
    JsonObject,
    JsonValue,
    LatestResult,
    ObservationChange,
    ObservationIngestResult,
    ResumeResult,
    RunAppendResult,
    StateMutation,
    UniverseList,
    VersionedDocument,
)


QueryValue: TypeAlias = None | bool | int | float | str
QueryParameters: TypeAlias = Mapping[str, QueryValue]
JsonMapping: TypeAlias = Mapping[str, JsonValue]

_CONTENT_RANGE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")
_ERROR_BODY_LIMIT = 2_048


def _validated_base_url(base_url: str) -> str:
    if not isinstance(base_url, str) or not base_url.strip():
        raise ValueError("base_url must be a non-empty absolute URL")
    if base_url != base_url.strip():
        raise ValueError("base_url must not contain surrounding whitespace")

    parsed = urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
        raise ValueError("base_url must use the http or https scheme")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("base_url must not contain credentials")
    if parsed.query or parsed.fragment:
        raise ValueError("base_url must not contain a query or fragment")

    if parsed.scheme == "http":
        hostname = parsed.hostname.lower()
        loopback = hostname == "localhost"
        if not loopback:
            try:
                loopback = ip_address(hostname).is_loopback
            except ValueError:
                loopback = False
        if not loopback:
            raise ValueError("non-loopback storage endpoints must use https")
    return base_url.rstrip("/") + "/"


def _default_headers(token: str | None) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": f"shared-mdstorage-client/{__version__}",
    }
    if token is not None:
        if (
            not token
            or token != token.strip()
            or any(ord(character) < 0x21 or ord(character) > 0x7E for character in token)
        ):
            raise ValueError(
                "token must use visible ASCII without surrounding whitespace"
            )
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _relative_file_path(path: str) -> str:
    if not path or path.startswith("/"):
        raise ValueError("file path must be a non-empty relative path")
    if any(part in {"", ".", ".."} for part in path.split("/")):
        raise ValueError("file path must be normalized and must not traverse parents")
    return quote(path, safe="/-._~")


def _path_segment(value: str, *, label: str) -> str:
    return quote(validate_structural_identifier(value, field=label), safe="-._~")


def _idempotency_headers(idempotency_key: str) -> dict[str, str]:
    if not idempotency_key or idempotency_key != idempotency_key.strip():
        raise ValueError("idempotency key must be non-empty and trimmed")
    return {"Idempotency-Key": idempotency_key}


def _write_headers(
    *,
    idempotency_key: str,
    if_match: str | None = None,
    if_none_match: bool = False,
    content_type: str | None = None,
) -> dict[str, str]:
    if if_match is not None and if_none_match:
        raise ValueError("if_match and if_none_match are mutually exclusive")
    headers = _idempotency_headers(idempotency_key)
    if if_match is not None:
        if not if_match.strip():
            raise ValueError("if_match must not be empty")
        headers["If-Match"] = if_match
    elif if_none_match:
        headers["If-None-Match"] = "*"
    if content_type is not None:
        if not content_type.strip():
            raise ValueError("content_type must not be empty")
        headers["Content-Type"] = content_type
    return headers


def _state_write_headers(
    *,
    idempotency_key: str,
    if_match: str | None,
    if_none_match: bool,
) -> dict[str, str]:
    if if_match is not None and if_none_match:
        raise ValueError("if_match and if_none_match are mutually exclusive")
    headers = _idempotency_headers(idempotency_key)
    if if_match is not None:
        if not if_match.strip():
            raise ValueError("if_match must not be empty")
        headers["If-Match"] = if_match
    elif if_none_match:
        headers["If-None-Match"] = "*"
    return headers


def _range_header(start: int | None, end: int | None) -> str | None:
    if start is None and end is None:
        return None
    if start is None:
        raise ValueError("range_start is required when range_end is set")
    if type(start) is not int or start < 0:
        raise ValueError("range_start must be a non-negative integer")
    if end is not None and (type(end) is not int or end < start):
        raise ValueError("range_end must be an integer at or after range_start")
    return f"bytes={start}-{'' if end is None else end}"


def _error_body(response: httpx.Response) -> str | None:
    if not response.content:
        return None
    return response.content[:_ERROR_BODY_LIMIT].decode("utf-8", errors="replace")


def _check_response(response: httpx.Response) -> None:
    if 200 <= response.status_code < 300:
        return
    request = response.request
    context = {
        "status_code": response.status_code,
        "method": request.method,
        "url": str(request.url),
        "response_body": _error_body(response),
    }
    message = f"storage service returned HTTP {response.status_code}"
    if response.status_code in {401, 403}:
        raise AuthenticationError(message, **context)
    if response.status_code == 404:
        raise NotFoundError(message, **context)
    if response.status_code in {409, 412}:
        raise ConflictError(message, **context)
    raise ProtocolError(message, **context)


def _protocol_error(response: httpx.Response, message: str) -> ProtocolError:
    return ProtocolError(
        message,
        status_code=response.status_code,
        method=response.request.method,
        url=str(response.request.url),
        response_body=_error_body(response),
    )


def _json_object(response: httpx.Response) -> JsonObject:
    try:
        value = response.json()
    except ValueError as exc:
        raise _protocol_error(
            response, "storage service returned invalid JSON"
        ) from exc
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise _protocol_error(
            response, "storage service returned a non-object JSON body"
        )
    return value


def _required_string(
    value: object, *, field: str, response: httpx.Response
) -> str:
    if not isinstance(value, str) or not value:
        raise _protocol_error(response, f"storage response has invalid {field}")
    return value


def _optional_string(
    value: object, *, field: str, response: httpx.Response
) -> str | None:
    if value is None:
        return None
    return _required_string(value, field=field, response=response)


def _nonnegative_int(
    value: object, *, field: str, response: httpx.Response
) -> int:
    if type(value) is not int or value < 0:
        raise _protocol_error(response, f"storage response has invalid {field}")
    return value


def _optional_nonnegative_int(
    value: object, *, field: str, response: httpx.Response
) -> int | None:
    if value is None:
        return None
    return _nonnegative_int(value, field=field, response=response)


def _metadata_from_object(
    value: object, *, response: httpx.Response
) -> FileMetadata:
    if not isinstance(value, dict):
        raise _protocol_error(response, "file listing contains invalid metadata")
    return FileMetadata(
        path=_required_string(value.get("path"), field="path", response=response),
        size=_nonnegative_int(value.get("size"), field="size", response=response),
        etag=_required_string(value.get("etag"), field="etag", response=response),
        modified_ns=_optional_nonnegative_int(
            value.get("modified_ns"), field="modified_ns", response=response
        ),
        last_modified=_optional_string(
            value.get("last_modified"), field="last_modified", response=response
        ),
        content_type=_optional_string(
            value.get("content_type"), field="content_type", response=response
        ),
    )


def _parse_file_page(response: httpx.Response) -> FilePage:
    payload = _json_object(response)
    _schema_version_one(payload, response=response)
    files = payload.get("files")
    if not isinstance(files, list):
        raise _protocol_error(response, "file listing does not contain a files array")
    cursor = _optional_string(
        payload.get("next_cursor"), field="next_cursor", response=response
    )
    return FilePage(
        files=tuple(_metadata_from_object(item, response=response) for item in files),
        next_cursor=cursor,
    )


def _etag(response: httpx.Response) -> str:
    return _required_string(
        response.headers.get("ETag"), field="ETag", response=response
    )


def _parse_stat(response: httpx.Response, *, path: str) -> FileMetadata:
    raw_size = response.headers.get("Content-Length")
    try:
        size = int(raw_size) if raw_size is not None else -1
    except ValueError as exc:
        raise _protocol_error(
            response, "file response has invalid Content-Length"
        ) from exc
    if size < 0:
        raise _protocol_error(response, "file response is missing Content-Length")
    return FileMetadata(
        path=path,
        size=size,
        etag=_etag(response),
        modified_ns=_header_nonnegative_int(
            response, "X-Modified-Nanoseconds"
        ),
        last_modified=response.headers.get("Last-Modified"),
        content_type=response.headers.get("Content-Type"),
    )


def _parse_download(
    response: httpx.Response, *, path: str, ranged: bool
) -> FileDownload:
    if ranged and response.status_code != 206:
        raise _protocol_error(response, "storage service did not honor the byte range")
    if not ranged and response.status_code != 200:
        raise _protocol_error(
            response, "storage service returned an invalid file status"
        )

    content_range: FileRange | None = None
    if ranged:
        raw_range = response.headers.get("Content-Range", "")
        match = _CONTENT_RANGE.fullmatch(raw_range)
        if match is None:
            raise _protocol_error(
                response, "ranged file response has invalid Content-Range"
            )
        start, end, total = (int(value) for value in match.groups())
        if end < start or total <= end or len(response.content) != end - start + 1:
            raise _protocol_error(
                response, "ranged file response has inconsistent length"
            )
        content_range = FileRange(start=start, end=end, total=total)
        size = total
    else:
        size = len(response.content)
        raw_size = response.headers.get("Content-Length")
        if raw_size is not None:
            try:
                declared_size = int(raw_size)
            except ValueError as exc:
                raise _protocol_error(
                    response, "file response has invalid Content-Length"
                ) from exc
            if declared_size != size:
                raise _protocol_error(response, "file response has inconsistent length")

    metadata = FileMetadata(
        path=path,
        size=size,
        etag=_etag(response),
        modified_ns=_header_nonnegative_int(
            response, "X-Modified-Nanoseconds"
        ),
        last_modified=response.headers.get("Last-Modified"),
        content_type=response.headers.get("Content-Type"),
    )
    return FileDownload(
        metadata=metadata,
        content=response.content,
        content_range=content_range,
    )


def _parse_mutation(response: httpx.Response, *, path: str) -> FileMutation:
    payload = _json_object(response)
    returned_path = _required_string(
        payload.get("path"), field="path", response=response
    )
    if returned_path != path:
        raise _protocol_error(response, "file mutation returned a different path")
    raw_replayed = payload.get("replayed", False)
    if type(raw_replayed) is not bool:
        raise _protocol_error(response, "file mutation has invalid replayed state")
    raw_offset = payload.get("offset")
    offset = (
        None
        if raw_offset is None
        else _nonnegative_int(raw_offset, field="offset", response=response)
    )
    return FileMutation(
        path=returned_path,
        size=_nonnegative_int(payload.get("size"), field="size", response=response),
        etag=_required_string(payload.get("etag"), field="etag", response=response),
        offset=offset,
        modified_ns=_optional_nonnegative_int(
            payload.get("modified_ns"), field="modified_ns", response=response
        ),
        replayed=raw_replayed,
        details=payload,
    )


def _header_nonnegative_int(
    response: httpx.Response, name: str
) -> int | None:
    value = response.headers.get(name)
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise _protocol_error(
            response, f"file response has invalid {name}"
        ) from exc
    if parsed < 0:
        raise _protocol_error(response, f"file response has invalid {name}")
    return parsed


def _schema_version_one(payload: JsonObject, *, response: httpx.Response) -> str:
    version = _required_string(
        payload.get("schema_version"), field="schema_version", response=response
    )
    if version != "1":
        raise _protocol_error(response, "storage response uses an unsupported schema")
    return version


def _parse_universe_list(response: httpx.Response) -> UniverseList:
    payload = _json_object(response)
    schema_version = _schema_version_one(payload, response=response)
    raw_universes = payload.get("universes")
    if not isinstance(raw_universes, list):
        raise _protocol_error(response, "universe listing has invalid universes")
    universes: list[GateUniverse] = []
    for item in raw_universes:
        if not isinstance(item, dict):
            raise _protocol_error(
                response, "universe listing contains an invalid entry"
            )
        universe = item.get("universe")
        if not isinstance(universe, dict):
            raise _protocol_error(
                response, "universe listing contains an invalid document"
            )
        universes.append(
            GateUniverse(
                gate_id=_required_string(
                    item.get("gate_id"), field="gate_id", response=response
                ),
                universe=universe,
            )
        )
    return UniverseList(schema_version=schema_version, universes=tuple(universes))


def _parse_first_open_interest_times(
    response: httpx.Response,
) -> FirstOpenInterestTimes:
    payload = _json_object(response)
    schema_version = _schema_version_one(payload, response=response)
    raw_times = payload.get("times")
    if not isinstance(raw_times, dict):
        raise _protocol_error(response, "first-observation response has invalid times")
    times: dict[str, int] = {}
    for market_id, value in raw_times.items():
        if not isinstance(market_id, str) or not market_id:
            raise _protocol_error(
                response, "first-observation response has an invalid market identity"
            )
        times[market_id] = _nonnegative_int(
            value, field="first open-interest time", response=response
        )
    return FirstOpenInterestTimes(schema_version=schema_version, times=times)


def _parse_changes(response: httpx.Response, *, after: int) -> ChangePage:
    payload = _json_object(response)
    schema_version = _schema_version_one(payload, response=response)
    raw_changes = payload.get("changes")
    if not isinstance(raw_changes, list):
        raise _protocol_error(response, "change response has invalid changes")
    changes: list[CommittedObservationChange] = []
    for raw_change in raw_changes:
        if not isinstance(raw_change, dict):
            raise _protocol_error(response, "change response has an invalid entry")
        row = raw_change.get("row")
        if not isinstance(row, dict):
            raise _protocol_error(response, "change response has an invalid row")
        metric = _required_string(
            raw_change.get("metric"), field="metric", response=response
        )
        if metric not in {"open_interest", "funding"}:
            raise _protocol_error(response, "change response has an invalid metric")
        change = CommittedObservationChange(
            cursor=_positive_response_int(
                raw_change.get("cursor"), field="cursor", response=response
            ),
            gate_id=_response_structural_identifier(
                raw_change.get("gate_id"), field="gate_id", response=response
            ),
            batch_id=_response_structural_identifier(
                raw_change.get("batch_id"), field="batch_id", response=response
            ),
            metric=metric,
            observation_time_ms=_positive_response_int(
                raw_change.get("observation_time_ms"),
                field="observation_time_ms",
                response=response,
            ),
            market_id=_required_string(
                raw_change.get("market_id"), field="market_id", response=response
            ),
            sample_kind=_required_string(
                raw_change.get("sample_kind"), field="sample_kind", response=response
            ),
            revision=_positive_response_int(
                raw_change.get("revision"), field="revision", response=response
            ),
            row=row,
        )
        if changes and change.cursor != changes[-1].cursor + 1:
            raise _protocol_error(response, "change response cursor order is not contiguous")
        changes.append(change)
    reset_required = payload.get("reset_required")
    if type(reset_required) is not bool:
        raise _protocol_error(response, "change response has invalid reset_required")
    next_cursor = _nonnegative_int(
        payload.get("next_cursor"), field="next_cursor", response=response
    )
    last_cursor = _nonnegative_int(
        payload.get("last_cursor"), field="last_cursor", response=response
    )
    if next_cursor > last_cursor:
        raise _protocol_error(response, "change response cursor exceeds last_cursor")
    if changes:
        if changes[0].cursor != after + 1:
            raise _protocol_error(response, "change response does not continue the cursor")
        if next_cursor != changes[-1].cursor:
            raise _protocol_error(response, "change response next_cursor is inconsistent")
    elif not reset_required and next_cursor != after:
        raise _protocol_error(response, "empty change response advances the cursor")
    if reset_required and (changes or next_cursor != last_cursor):
        raise _protocol_error(response, "change reset response is inconsistent")
    return ChangePage(
        schema_version=schema_version,
        changes=tuple(changes),
        next_cursor=next_cursor,
        reset_required=reset_required,
        last_cursor=last_cursor,
    )


def _positive_response_int(
    value: object, *, field: str, response: httpx.Response
) -> int:
    parsed = _nonnegative_int(value, field=field, response=response)
    if parsed == 0:
        raise _protocol_error(response, f"storage response has invalid {field}")
    return parsed


def _response_structural_identifier(
    value: object, *, field: str, response: httpx.Response
) -> str:
    try:
        return validate_structural_identifier(value, field=field)
    except ValueError as exc:
        raise _protocol_error(
            response, f"storage response has invalid {field}"
        ) from exc


def _parse_versioned_document(response: httpx.Response) -> VersionedDocument:
    document = _json_object(response)
    schema_version = _schema_version_one(document, response=response)
    payload = document.get("payload")
    if not isinstance(payload, dict):
        raise _protocol_error(response, "state response has an invalid payload")
    return VersionedDocument(
        schema_version=schema_version,
        payload=payload,
        etag=_etag(response),
    )


def _expected_string(
    value: object,
    *,
    field: str,
    expected: str,
    response: httpx.Response,
) -> str:
    parsed = _required_string(value, field=field, response=response)
    if parsed != expected:
        raise _protocol_error(response, f"storage response has mismatched {field}")
    return parsed


def _parse_observation_change(
    value: object, *, response: httpx.Response
) -> ObservationChange:
    if not isinstance(value, dict):
        raise _protocol_error(response, "ingest response has an invalid change entry")
    return ObservationChange(
        cursor=_positive_response_int(
            value.get("cursor"), field="cursor", response=response
        ),
        observation_time_ms=_positive_response_int(
            value.get("observation_time_ms"),
            field="observation_time_ms",
            response=response,
        ),
        market_id=_required_string(
            value.get("market_id"), field="market_id", response=response
        ),
        sample_kind=_required_string(
            value.get("sample_kind"), field="sample_kind", response=response
        ),
        revision=_positive_response_int(
            value.get("revision"), field="revision", response=response
        ),
    )


def _parse_ingest_result(
    response: httpx.Response,
    *,
    gate_id: str,
    batch_id: str,
    metric: str,
    expected_rows_received: int,
) -> ObservationIngestResult:
    payload = _json_object(response)
    schema_version = _schema_version_one(payload, response=response)
    changes_value = payload.get("changes")
    if not isinstance(changes_value, list):
        raise _protocol_error(response, "ingest response has an invalid changes array")
    changes = tuple(
        _parse_observation_change(item, response=response) for item in changes_value
    )
    rows_received = _nonnegative_int(
        payload.get("rows_received"), field="rows_received", response=response
    )
    rows_written = _nonnegative_int(
        payload.get("rows_written"), field="rows_written", response=response
    )
    rows_deduplicated = _nonnegative_int(
        payload.get("rows_deduplicated"),
        field="rows_deduplicated",
        response=response,
    )
    cursor_start = _optional_nonnegative_int(
        payload.get("cursor_start"), field="cursor_start", response=response
    )
    cursor_end = _optional_nonnegative_int(
        payload.get("cursor_end"), field="cursor_end", response=response
    )
    if len(changes) != rows_written:
        raise _protocol_error(response, "ingest response has inconsistent change count")
    if rows_received != expected_rows_received:
        raise _protocol_error(
            response, "ingest response does not match the submitted row count"
        )
    if rows_received != rows_written + rows_deduplicated:
        raise _protocol_error(response, "ingest response has inconsistent row counts")
    if changes:
        if cursor_start != changes[0].cursor or cursor_end != changes[-1].cursor:
            raise _protocol_error(response, "ingest response has inconsistent cursors")
        if any(
            current.cursor != previous.cursor + 1
            for previous, current in zip(changes, changes[1:])
        ):
            raise _protocol_error(response, "ingest response cursors are not contiguous")
    elif cursor_start is not None or cursor_end is not None:
        raise _protocol_error(response, "ingest response has cursors without changes")
    return ObservationIngestResult(
        schema_version=schema_version,
        gate_id=_expected_string(
            payload.get("gate_id"), field="gate_id", expected=gate_id, response=response
        ),
        batch_id=_expected_string(
            payload.get("batch_id"),
            field="batch_id",
            expected=batch_id,
            response=response,
        ),
        metric=_expected_string(
            payload.get("metric"), field="metric", expected=metric, response=response
        ),
        rows_received=rows_received,
        rows_written=rows_written,
        rows_deduplicated=rows_deduplicated,
        cursor_start=cursor_start,
        cursor_end=cursor_end,
        changes=changes,
        details=payload,
    )


def _parse_state_mutation(response: httpx.Response) -> StateMutation:
    payload = _json_object(response)
    return StateMutation(
        schema_version=_schema_version_one(payload, response=response),
        etag=_required_string(payload.get("etag"), field="etag", response=response),
        size=_nonnegative_int(payload.get("size"), field="size", response=response),
        snapshot=_optional_string(
            payload.get("snapshot"), field="snapshot", response=response
        ),
        details=payload,
    )


def _parse_run_append(response: httpx.Response) -> RunAppendResult:
    payload = _json_object(response)
    offset = _nonnegative_int(payload.get("offset"), field="offset", response=response)
    length = _nonnegative_int(payload.get("length"), field="length", response=response)
    size = _nonnegative_int(payload.get("size"), field="size", response=response)
    if offset + length != size:
        raise _protocol_error(response, "run append response has inconsistent size")
    return RunAppendResult(
        schema_version=_schema_version_one(payload, response=response),
        offset=offset,
        length=length,
        size=size,
        details=payload,
    )


def _parse_latest(response: httpx.Response, *, metric: str) -> LatestResult:
    payload = _json_object(response)
    raw_rows = payload.get("rows")
    if not isinstance(raw_rows, dict) or not all(
        isinstance(market_id, str)
        and market_id
        and isinstance(row, dict)
        for market_id, row in raw_rows.items()
    ):
        raise _protocol_error(response, "latest response has invalid rows")
    return LatestResult(
        schema_version=_schema_version_one(payload, response=response),
        metric=_expected_string(
            payload.get("metric"), field="metric", expected=metric, response=response
        ),
        rows=dict(raw_rows),
        details=payload,
    )


def _parse_history(response: httpx.Response) -> HistoryPage:
    payload = _json_object(response)
    raw_rows = payload.get("rows")
    if not isinstance(raw_rows, list) or not all(
        isinstance(row, dict) for row in raw_rows
    ):
        raise _protocol_error(response, "history response has invalid rows")
    raw_present = payload.get("present_market_ids")
    if not isinstance(raw_present, list) or not all(
        isinstance(market_id, str) and market_id for market_id in raw_present
    ):
        raise _protocol_error(
            response, "history response has invalid present_market_ids"
        )
    has_more = payload.get("has_more")
    if type(has_more) is not bool:
        raise _protocol_error(response, "history response has invalid has_more")
    raw_after = payload.get("next_after")
    next_after: tuple[int, str, str] | None = None
    if raw_after is not None:
        if (
            not isinstance(raw_after, list)
            or len(raw_after) != 3
            or type(raw_after[0]) is not int
            or raw_after[0] < 0
            or not isinstance(raw_after[1], str)
            or not raw_after[1]
            or not isinstance(raw_after[2], str)
            or not raw_after[2]
        ):
            raise _protocol_error(response, "history response has invalid next_after")
        next_after = (raw_after[0], raw_after[1], raw_after[2])
    if has_more != (next_after is not None):
        raise _protocol_error(response, "history response has inconsistent pagination")
    return HistoryPage(
        schema_version=_schema_version_one(payload, response=response),
        rows=tuple(raw_rows),
        present_market_ids=tuple(raw_present),
        has_more=has_more,
        next_after=next_after,
        details=payload,
    )


def _parse_resume(response: httpx.Response, *, metric: str) -> ResumeResult:
    payload = _json_object(response)
    raw_cursors = payload.get("cursors")
    if not isinstance(raw_cursors, dict):
        raise _protocol_error(response, "resume response has invalid cursors")
    cursors: dict[str, int] = {}
    for market_id, cursor in raw_cursors.items():
        if not isinstance(market_id, str) or not market_id:
            raise _protocol_error(response, "resume response has invalid market identity")
        cursors[market_id] = _nonnegative_int(
            cursor, field="resume cursor", response=response
        )
    return ResumeResult(
        schema_version=_schema_version_one(payload, response=response),
        metric=_expected_string(
            payload.get("metric"), field="metric", expected=metric, response=response
        ),
        cursors=cursors,
        details=payload,
    )


def _market_id_payload(market_ids: Sequence[str]) -> JsonObject:
    if isinstance(market_ids, str):
        raise ValueError("market_ids must be a sequence of market identities")
    values = list(market_ids)
    if any(not isinstance(value, str) or not value for value in values):
        raise ValueError("each market identity must be a non-empty string")
    return {"market_ids": values}


def _latest_payload(
    payload: JsonMapping, *, before_ms: int | None
) -> JsonObject:
    body = dict(payload)
    if "before_ms" in body:
        raise ValueError("before_ms must be passed as the named argument")
    if before_ms is not None:
        if type(before_ms) is not int or before_ms <= 0:
            raise ValueError("before_ms must be a positive integer")
        body["before_ms"] = before_ms
    return body


def _ingest_payload(payload: JsonMapping) -> JsonObject:
    body = dict(payload)
    validate_structural_identifier(body.get("gate_id"), field="gate_id")
    validate_structural_identifier(body.get("batch_id"), field="batch_id")
    _request_metric(body)
    rows = body.get("rows")
    if not isinstance(rows, list) or not 1 <= len(rows) <= 100_000:
        raise ValueError("rows must be a non-empty list with at most 100000 entries")
    if not all(isinstance(row, dict) for row in rows):
        raise ValueError("each observation row must be a JSON object")
    return body


def _changes_params(params: QueryParameters | None) -> dict[str, int]:
    if params is None:
        return {}
    body = dict(params)
    unexpected = set(body) - {"after", "limit"}
    if unexpected:
        raise ValueError("changes parameters support only after and limit")
    after = body.get("after", 0)
    limit = body.get("limit", 1_000)
    if type(after) is not int or after < 0:
        raise ValueError("after must be a non-negative integer")
    if type(limit) is not int or not 1 <= limit <= 10_000:
        raise ValueError("limit must be an integer from 1 through 10000")
    result: dict[str, int] = {}
    if "after" in body:
        result["after"] = after
    if "limit" in body:
        result["limit"] = limit
    return result


def _request_metric(payload: Mapping[str, object]) -> str:
    metric = payload.get("metric")
    if not isinstance(metric, str) or metric not in {"open_interest", "funding"}:
        raise ValueError("metric must be open_interest or funding")
    return metric


class StorageClient:
    """Synchronous client for the versioned storage service contract."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float | httpx.Timeout = 10.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            base_url=_validated_base_url(base_url),
            headers=_default_headers(token),
            timeout=timeout,
            transport=transport,
        )

    def __enter__(self) -> StorageClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def _request(self, method: str, url: str, **kwargs: object) -> httpx.Response:
        try:
            response = self._client.request(method, url, **kwargs)
        except httpx.RequestError as exc:
            raise TransportError(
                "storage request failed at the transport boundary", cause=exc
            ) from exc
        _check_response(response)
        return response

    def list_files(
        self,
        *,
        prefix: str = "",
        cursor: str | None = None,
        limit: int | None = None,
    ) -> FilePage:
        if limit is not None and (type(limit) is not int or limit <= 0):
            raise ValueError("limit must be a positive integer")
        params: dict[str, QueryValue] = {"prefix": prefix}
        if cursor is not None:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = limit
        return _parse_file_page(self._request("GET", "v1/files", params=params))

    def stat_file(self, path: str) -> FileMetadata:
        endpoint = f"v1/files/{_relative_file_path(path)}"
        return _parse_stat(self._request("HEAD", endpoint), path=path)

    def get_file(
        self,
        path: str,
        *,
        range_start: int | None = None,
        range_end: int | None = None,
    ) -> FileDownload:
        range_value = _range_header(range_start, range_end)
        headers = {"Accept": "application/octet-stream"}
        if range_value is not None:
            headers["Range"] = range_value
        response = self._request(
            "GET", f"v1/files/{_relative_file_path(path)}", headers=headers
        )
        return _parse_download(response, path=path, ranged=range_value is not None)

    def put_file(
        self,
        path: str,
        content: bytes,
        *,
        idempotency_key: str,
        if_match: str | None = None,
        if_none_match: bool = False,
        content_type: str = "application/octet-stream",
    ) -> FileMutation:
        response = self._request(
            "PUT",
            f"v1/files/{_relative_file_path(path)}",
            content=content,
            headers=_write_headers(
                idempotency_key=idempotency_key,
                if_match=if_match,
                if_none_match=if_none_match,
                content_type=content_type,
            ),
        )
        return _parse_mutation(response, path=path)

    def append_file(
        self,
        path: str,
        content: bytes,
        *,
        idempotency_key: str,
        content_type: str = "application/octet-stream",
    ) -> FileMutation:
        response = self._request(
            "POST",
            f"v1/files/{_relative_file_path(path)}:append",
            content=content,
            headers=_write_headers(
                idempotency_key=idempotency_key,
                content_type=content_type,
            ),
        )
        return _parse_mutation(response, path=path)

    def ingest_observations(
        self, payload: JsonMapping, *, idempotency_key: str
    ) -> ObservationIngestResult:
        body = _ingest_payload(payload)
        response = self._request(
            "POST",
            "v1/observations:ingest",
            json=body,
            headers=_idempotency_headers(idempotency_key),
        )
        return _parse_ingest_result(
            response,
            gate_id=str(body["gate_id"]),
            batch_id=str(body["batch_id"]),
            metric=_request_metric(body),
            expected_rows_received=len(body["rows"]),
        )

    def append_run(
        self,
        gate_id: str,
        run_id: str,
        payload: JsonMapping,
        *,
        idempotency_key: str,
    ) -> RunAppendResult:
        gate = _path_segment(gate_id, label="gate_id")
        run = _path_segment(run_id, label="run_id")
        endpoint = f"v1/gates/{gate}/runs/{run}/events:append"
        return _parse_run_append(
            self._request(
                "POST", endpoint, json=dict(payload),
                headers=_idempotency_headers(idempotency_key),
            )
        )

    def put_status(
        self,
        gate_id: str,
        payload: JsonMapping,
        *,
        idempotency_key: str,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> StateMutation:
        endpoint = f"v1/gates/{_path_segment(gate_id, label='gate_id')}/status"
        return _parse_state_mutation(
            self._request(
                "PUT",
                endpoint,
                json={"payload": dict(payload)},
                headers=_state_write_headers(
                    idempotency_key=idempotency_key,
                    if_match=if_match,
                    if_none_match=if_none_match,
                ),
            )
        )

    def get_status(self, gate_id: str) -> JsonObject:
        return self.get_status_document(gate_id).payload

    def get_status_document(self, gate_id: str) -> VersionedDocument:
        endpoint = f"v1/gates/{_path_segment(gate_id, label='gate_id')}/status"
        return _parse_versioned_document(self._request("GET", endpoint))

    def put_universe(
        self,
        gate_id: str,
        payload: JsonMapping,
        *,
        idempotency_key: str,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> StateMutation:
        endpoint = f"v1/gates/{_path_segment(gate_id, label='gate_id')}/universe"
        return _parse_state_mutation(
            self._request(
                "PUT",
                endpoint,
                json={"payload": dict(payload)},
                headers=_state_write_headers(
                    idempotency_key=idempotency_key,
                    if_match=if_match,
                    if_none_match=if_none_match,
                ),
            )
        )

    def get_universe(self, gate_id: str) -> JsonObject:
        return self.get_universe_document(gate_id).payload

    def get_universe_document(self, gate_id: str) -> VersionedDocument:
        endpoint = (
            f"v1/gates/{_path_segment(gate_id, label='gate_id')}/universe/current"
        )
        return _parse_versioned_document(self._request("GET", endpoint))

    def list_universes(self) -> UniverseList:
        return _parse_universe_list(self._request("GET", "v1/gates/universes"))

    def latest(
        self, payload: JsonMapping, *, before_ms: int | None = None
    ) -> LatestResult:
        body = _latest_payload(payload, before_ms=before_ms)
        return _parse_latest(
            self._request(
                "POST",
                "v1/query/latest",
                json=body,
            ),
            metric=_request_metric(body),
        )

    def history(self, payload: JsonMapping) -> HistoryPage:
        body = dict(payload)
        _request_metric(body)
        return _parse_history(
            self._request("POST", "v1/query/history", json=body)
        )

    def resume(self, payload: JsonMapping) -> ResumeResult:
        body = dict(payload)
        metric = _request_metric(body)
        return _parse_resume(
            self._request("POST", "v1/query/resume", json=body), metric=metric
        )

    def first_open_interest_times(
        self, market_ids: Sequence[str]
    ) -> FirstOpenInterestTimes:
        response = self._request(
            "POST",
            "v1/query/first-open-interest-times",
            json=_market_id_payload(market_ids),
        )
        return _parse_first_open_interest_times(response)

    def changes(self, params: QueryParameters | None = None) -> ChangePage:
        query = _changes_params(params)
        return _parse_changes(
            self._request("GET", "v1/changes", params=query),
            after=query.get("after", 0),
        )

    def readiness(self) -> JsonObject:
        return _json_object(self._request("GET", "readyz"))


class AsyncStorageClient:
    """Asynchronous client for the versioned storage service contract."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float | httpx.Timeout = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=_validated_base_url(base_url),
            headers=_default_headers(token),
            timeout=timeout,
            transport=transport,
        )

    async def __aenter__(self) -> AsyncStorageClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _request(
        self, method: str, url: str, **kwargs: object
    ) -> httpx.Response:
        try:
            response = await self._client.request(method, url, **kwargs)
        except httpx.RequestError as exc:
            raise TransportError(
                "storage request failed at the transport boundary", cause=exc
            ) from exc
        _check_response(response)
        return response

    async def list_files(
        self,
        *,
        prefix: str = "",
        cursor: str | None = None,
        limit: int | None = None,
    ) -> FilePage:
        if limit is not None and (type(limit) is not int or limit <= 0):
            raise ValueError("limit must be a positive integer")
        params: dict[str, QueryValue] = {"prefix": prefix}
        if cursor is not None:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = limit
        response = await self._request("GET", "v1/files", params=params)
        return _parse_file_page(response)

    async def stat_file(self, path: str) -> FileMetadata:
        endpoint = f"v1/files/{_relative_file_path(path)}"
        return _parse_stat(await self._request("HEAD", endpoint), path=path)

    async def get_file(
        self,
        path: str,
        *,
        range_start: int | None = None,
        range_end: int | None = None,
    ) -> FileDownload:
        range_value = _range_header(range_start, range_end)
        headers = {"Accept": "application/octet-stream"}
        if range_value is not None:
            headers["Range"] = range_value
        response = await self._request(
            "GET", f"v1/files/{_relative_file_path(path)}", headers=headers
        )
        return _parse_download(response, path=path, ranged=range_value is not None)

    async def put_file(
        self,
        path: str,
        content: bytes,
        *,
        idempotency_key: str,
        if_match: str | None = None,
        if_none_match: bool = False,
        content_type: str = "application/octet-stream",
    ) -> FileMutation:
        response = await self._request(
            "PUT",
            f"v1/files/{_relative_file_path(path)}",
            content=content,
            headers=_write_headers(
                idempotency_key=idempotency_key,
                if_match=if_match,
                if_none_match=if_none_match,
                content_type=content_type,
            ),
        )
        return _parse_mutation(response, path=path)

    async def append_file(
        self,
        path: str,
        content: bytes,
        *,
        idempotency_key: str,
        content_type: str = "application/octet-stream",
    ) -> FileMutation:
        response = await self._request(
            "POST",
            f"v1/files/{_relative_file_path(path)}:append",
            content=content,
            headers=_write_headers(
                idempotency_key=idempotency_key,
                content_type=content_type,
            ),
        )
        return _parse_mutation(response, path=path)

    async def ingest_observations(
        self, payload: JsonMapping, *, idempotency_key: str
    ) -> ObservationIngestResult:
        body = _ingest_payload(payload)
        response = await self._request(
            "POST",
            "v1/observations:ingest",
            json=body,
            headers=_idempotency_headers(idempotency_key),
        )
        return _parse_ingest_result(
            response,
            gate_id=str(body["gate_id"]),
            batch_id=str(body["batch_id"]),
            metric=_request_metric(body),
            expected_rows_received=len(body["rows"]),
        )

    async def append_run(
        self,
        gate_id: str,
        run_id: str,
        payload: JsonMapping,
        *,
        idempotency_key: str,
    ) -> RunAppendResult:
        gate = _path_segment(gate_id, label="gate_id")
        run = _path_segment(run_id, label="run_id")
        endpoint = f"v1/gates/{gate}/runs/{run}/events:append"
        response = await self._request(
            "POST",
            endpoint,
            json=dict(payload),
            headers=_idempotency_headers(idempotency_key),
        )
        return _parse_run_append(response)

    async def put_status(
        self,
        gate_id: str,
        payload: JsonMapping,
        *,
        idempotency_key: str,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> StateMutation:
        endpoint = f"v1/gates/{_path_segment(gate_id, label='gate_id')}/status"
        response = await self._request(
            "PUT",
            endpoint,
            json={"payload": dict(payload)},
            headers=_state_write_headers(
                idempotency_key=idempotency_key,
                if_match=if_match,
                if_none_match=if_none_match,
            ),
        )
        return _parse_state_mutation(response)

    async def get_status(self, gate_id: str) -> JsonObject:
        return (await self.get_status_document(gate_id)).payload

    async def get_status_document(self, gate_id: str) -> VersionedDocument:
        endpoint = f"v1/gates/{_path_segment(gate_id, label='gate_id')}/status"
        return _parse_versioned_document(await self._request("GET", endpoint))

    async def put_universe(
        self,
        gate_id: str,
        payload: JsonMapping,
        *,
        idempotency_key: str,
        if_match: str | None = None,
        if_none_match: bool = False,
    ) -> StateMutation:
        endpoint = f"v1/gates/{_path_segment(gate_id, label='gate_id')}/universe"
        response = await self._request(
            "PUT",
            endpoint,
            json={"payload": dict(payload)},
            headers=_state_write_headers(
                idempotency_key=idempotency_key,
                if_match=if_match,
                if_none_match=if_none_match,
            ),
        )
        return _parse_state_mutation(response)

    async def get_universe(self, gate_id: str) -> JsonObject:
        return (await self.get_universe_document(gate_id)).payload

    async def get_universe_document(self, gate_id: str) -> VersionedDocument:
        endpoint = (
            f"v1/gates/{_path_segment(gate_id, label='gate_id')}/universe/current"
        )
        return _parse_versioned_document(await self._request("GET", endpoint))

    async def list_universes(self) -> UniverseList:
        response = await self._request("GET", "v1/gates/universes")
        return _parse_universe_list(response)

    async def latest(
        self, payload: JsonMapping, *, before_ms: int | None = None
    ) -> LatestResult:
        body = _latest_payload(payload, before_ms=before_ms)
        return _parse_latest(
            await self._request(
                "POST",
                "v1/query/latest",
                json=body,
            ),
            metric=_request_metric(body),
        )

    async def history(self, payload: JsonMapping) -> HistoryPage:
        body = dict(payload)
        _request_metric(body)
        return _parse_history(
            await self._request("POST", "v1/query/history", json=body)
        )

    async def resume(self, payload: JsonMapping) -> ResumeResult:
        body = dict(payload)
        metric = _request_metric(body)
        return _parse_resume(
            await self._request("POST", "v1/query/resume", json=body),
            metric=metric,
        )

    async def first_open_interest_times(
        self, market_ids: Sequence[str]
    ) -> FirstOpenInterestTimes:
        response = await self._request(
            "POST",
            "v1/query/first-open-interest-times",
            json=_market_id_payload(market_ids),
        )
        return _parse_first_open_interest_times(response)

    async def changes(
        self, params: QueryParameters | None = None
    ) -> ChangePage:
        query = _changes_params(params)
        response = await self._request("GET", "v1/changes", params=query)
        return _parse_changes(response, after=query.get("after", 0))

    async def readiness(self) -> JsonObject:
        return _json_object(await self._request("GET", "readyz"))
