from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import Callable

import httpx
import pytest

from shared_mdstorage_client import (
    AsyncStorageClient,
    AuthenticationError,
    ConflictError,
    NotFoundError,
    ProtocolError,
    StorageClient,
    TransportError,
    __version__,
    validate_structural_identifier,
)


OBSERVATION_TIME_MS = 1_786_348_800_000


def open_interest_row() -> dict[str, object]:
    return {
        "observation_time_ms": OBSERVATION_TIME_MS,
        "collected_at_ms": OBSERVATION_TIME_MS + 100,
        "sample_kind": "current",
        "market_id": "market-a",
        "oi_value_usd": "100.5",
    }


def funding_row() -> dict[str, object]:
    return {
        "observation_time_ms": OBSERVATION_TIME_MS,
        "collected_at_ms": OBSERVATION_TIME_MS + 100,
        "sample_kind": "current",
        "market_id": "market-a",
        "funding_rate": "0.0001",
        "funding_kind": "indicative",
        "funding_interval_kind": "explicit_duration",
        "funding_interval_ms": 28_800_000,
    }


def universe_snapshot() -> dict[str, object]:
    return {
        "schema_version": "1",
        "fetched_at_ms": OBSERVATION_TIME_MS,
        "universe_hash": "revision-a",
        "markets": [],
    }


def test_sync_file_contract_preserves_paths_preconditions_and_ranges() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        raw_path = request.url.raw_path.decode("ascii")
        if request.method == "GET" and raw_path.startswith("/v1/files?"):
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "files": [
                        {
                            "path": "observations/day.csv",
                            "size": 9,
                            "etag": '"file-1"',
                            "modified_ns": 100,
                            "last_modified": "Tue, 11 Aug 2026 01:00:00 GMT",
                            "content_type": "text/csv",
                        }
                    ],
                    "next_cursor": "next-page",
                },
            )
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={
                    "Content-Length": "9",
                    "ETag": '"file-1"',
                    "X-Modified-Nanoseconds": "100",
                    "Content-Type": "text/csv",
                },
            )
        if request.method == "GET":
            return httpx.Response(
                206,
                content=b"cde",
                headers={
                    "Content-Range": "bytes 2-4/9",
                    "ETag": '"file-1"',
                    "X-Modified-Nanoseconds": "100",
                    "Content-Type": "text/csv",
                },
            )
        if request.method == "PUT":
            assert raw_path == "/v1/files/observations/new%20day.csv"
            assert request.headers["If-None-Match"] == "*"
            assert request.headers["Idempotency-Key"] == "replace-1"
            assert request.content == b"new"
            return httpx.Response(
                201,
                json={
                    "path": "observations/new day.csv",
                    "size": 3,
                    "etag": '"file-2"',
                    "modified_ns": 200,
                    "replayed": False,
                },
            )
        assert request.method == "POST"
        assert raw_path == "/v1/files/observations/day.csv:append"
        assert request.headers["Idempotency-Key"] == "append-1"
        return httpx.Response(
            200,
            json={
                "path": "observations/day.csv",
                "size": 12,
                "etag": '"file-3"',
                "offset": 9,
                "modified_ns": 300,
                "replayed": False,
            },
        )

    with StorageClient(
        "https://storage.test",
        token="service-token",
        transport=httpx.MockTransport(handler),
    ) as client:
        page = client.list_files(prefix="observations/", cursor="page-1", limit=20)
        metadata = client.stat_file("observations/day.csv")
        download = client.get_file(
            "observations/day.csv", range_start=2, range_end=4
        )
        replacement = client.put_file(
            "observations/new day.csv",
            b"new",
            idempotency_key="replace-1",
            if_none_match=True,
            content_type="text/csv",
        )
        append = client.append_file(
            "observations/day.csv",
            b"new",
            idempotency_key="append-1",
            content_type="text/csv",
        )

    assert page.next_cursor == "next-page"
    assert page.files[0].path == "observations/day.csv"
    assert page.files[0].modified_ns == 100
    assert metadata.size == 9
    assert metadata.modified_ns == 100
    assert download.content == b"cde"
    assert download.content_range is not None
    assert download.content_range.total == 9
    assert replacement.etag == '"file-2"'
    assert replacement.modified_ns == 200
    assert append.offset == 9
    assert append.modified_ns == 300
    assert dict(requests[0].url.params) == {
        "prefix": "observations/",
        "cursor": "page-1",
        "limit": "20",
    }
    assert all(
        request.headers["Authorization"] == "Bearer service-token"
        for request in requests
    )
    assert all(
        request.headers["User-Agent"] == f"shared-mdstorage-client/{__version__}"
        for request in requests
    )
    assert requests[2].headers["Range"] == "bytes=2-4"


def test_sync_domain_methods_use_the_versioned_routes() -> None:
    observed: list[tuple[str, str, object, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body: object = None
        if request.content:
            body = json.loads(request.content)
        observed.append(
            (
                request.method,
                request.url.path,
                body,
                request.headers.get("Idempotency-Key"),
            )
        )
        if request.url.path == "/v1/gates/universes":
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "universes": [
                        {"gate_id": "gate-a", "universe": universe_snapshot()}
                    ],
                },
            )
        if request.url.path == "/v1/query/first-open-interest-times":
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "times": {"market-a": 100, "market-b": 200},
                },
            )
        if request.url.path == "/v1/changes":
            assert dict(request.url.params) == {"after": "2", "limit": "10"}
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "changes": [
                        {
                            "cursor": 3,
                            "gate_id": "gate-a",
                            "batch_id": "batch-a",
                            "metric": "open_interest",
                            "observation_time_ms": 1_786_348_800_000,
                            "market_id": "market-a",
                            "sample_kind": "current",
                            "revision": 1,
                            "row": {"market_id": "market-a"},
                        }
                    ],
                    "next_cursor": 3,
                    "reset_required": False,
                    "last_cursor": 8,
                },
            )
        if request.url.path == "/v1/observations:ingest":
            assert isinstance(body, dict)
            assert body == {
                "gate_id": "gate-a",
                "batch_id": "batch-a",
                "metric": "open_interest",
                "rows": [open_interest_row()],
            }
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "gate_id": body["gate_id"],
                    "batch_id": body["batch_id"],
                    "metric": body["metric"],
                    "rows_received": 1,
                    "rows_written": 1,
                    "rows_deduplicated": 0,
                    "cursor_start": 1,
                    "cursor_end": 1,
                    "changes": [
                        {
                            "cursor": 1,
                            "observation_time_ms": OBSERVATION_TIME_MS,
                            "market_id": "market-a",
                            "sample_kind": "current",
                            "revision": 1,
                        }
                    ],
                },
            )
        if "/runs/" in request.url.path:
            assert body == {
                "started_at_ms": OBSERVATION_TIME_MS,
                "status": "complete",
            }
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "offset": 0,
                    "length": 10,
                    "size": 10,
                },
            )
        if request.method == "PUT" and request.url.path.endswith("/status"):
            return httpx.Response(
                200,
                json={"schema_version": "1", "etag": '"status"', "size": 20},
            )
        if request.method == "GET" and request.url.path.endswith("/status"):
            return httpx.Response(
                200,
                json={"schema_version": "1", "payload": {"state": "ready"}},
                headers={"ETag": '"status"'},
            )
        if request.method == "PUT" and request.url.path.endswith("/universe"):
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "etag": '"universe"',
                    "size": 40,
                    "snapshot": "snapshot.json",
                },
            )
        if request.url.path.endswith("/universe/current"):
            return httpx.Response(
                200,
                json={"schema_version": "1", "payload": universe_snapshot()},
                headers={"ETag": '"universe"'},
            )
        if request.url.path == "/v1/query/latest":
            assert body == {
                "metric": "open_interest",
                "market_ids": ["market-a"],
                "sample_kinds": ["current"],
                "before_ms": 1_800_000_000_000,
            }
            return httpx.Response(
                200,
                json={"schema_version": "1", "metric": body["metric"], "rows": {}},
            )
        if request.url.path == "/v1/query/history":
            assert body == {
                "metric": "funding",
                "start_ms": OBSERVATION_TIME_MS - 1_000,
                "stop_ms": OBSERVATION_TIME_MS + 1_000,
                "market_ids": ["market-a"],
                "limit": 100,
            }
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "rows": [],
                    "present_market_ids": [],
                    "has_more": False,
                    "next_after": None,
                },
            )
        if request.url.path == "/v1/query/resume":
            assert body == {
                "metric": "open_interest",
                "requests": [
                    {
                        "market_id": "market-a",
                        "floor_ms": OBSERVATION_TIME_MS,
                        "interval_seconds": 300,
                    }
                ],
                "sample_kind": "history",
            }
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "metric": body["metric"],
                    "cursors": {},
                },
            )
        return httpx.Response(200, json={"accepted": True})

    with StorageClient(
        "https://storage.test", transport=httpx.MockTransport(handler)
    ) as client:
        ingest = client.ingest_observations(
            {
                "gate_id": "gate-a",
                "batch_id": "batch-a",
                "metric": "open_interest",
                "rows": [open_interest_row()],
            },
            idempotency_key="ingest-1",
        )
        run = client.append_run(
            "gate-a",
            "run-a",
            {"started_at_ms": OBSERVATION_TIME_MS, "status": "complete"},
            idempotency_key="event-1",
        )
        status_mutation = client.put_status(
            "gate-a",
            {"state": "ready"},
            idempotency_key="status-1",
            if_match='"status-1"',
        )
        status_payload = client.get_status("gate-a")
        universe_mutation = client.put_universe(
            "gate-a",
            universe_snapshot(),
            idempotency_key="universe-1",
            if_none_match=True,
        )
        universe_payload = client.get_universe("gate-a")
        universes = client.list_universes()
        latest = client.latest(
            {
                "metric": "open_interest",
                "market_ids": ["market-a"],
                "sample_kinds": ["current"],
            },
            before_ms=1_800_000_000_000,
        )
        history = client.history(
            {
                "metric": "funding",
                "start_ms": OBSERVATION_TIME_MS - 1_000,
                "stop_ms": OBSERVATION_TIME_MS + 1_000,
                "market_ids": ["market-a"],
                "limit": 100,
            }
        )
        resume = client.resume(
            {
                "metric": "open_interest",
                "requests": [
                    {
                        "market_id": "market-a",
                        "floor_ms": OBSERVATION_TIME_MS,
                        "interval_seconds": 300,
                    }
                ],
                "sample_kind": "history",
            }
        )
        first_times = client.first_open_interest_times(["market-a", "market-b"])
        changes = client.changes({"after": 2, "limit": 10})
        client.readiness()

    assert [(method, path) for method, path, _, _ in observed] == [
        ("POST", "/v1/observations:ingest"),
        ("POST", "/v1/gates/gate-a/runs/run-a/events:append"),
        ("PUT", "/v1/gates/gate-a/status"),
        ("GET", "/v1/gates/gate-a/status"),
        ("PUT", "/v1/gates/gate-a/universe"),
        ("GET", "/v1/gates/gate-a/universe/current"),
        ("GET", "/v1/gates/universes"),
        ("POST", "/v1/query/latest"),
        ("POST", "/v1/query/history"),
        ("POST", "/v1/query/resume"),
        ("POST", "/v1/query/first-open-interest-times"),
        ("GET", "/v1/changes"),
        ("GET", "/readyz"),
    ]
    assert observed[0][3] == "ingest-1"
    assert observed[1][3] == "event-1"
    assert observed[2][3] == "status-1"
    assert observed[4][3] == "universe-1"
    assert observed[2][2] == {"payload": {"state": "ready"}}
    assert observed[4][2] == {
        "payload": universe_snapshot()
    }
    assert observed[7][2] == {
        "metric": "open_interest",
        "market_ids": ["market-a"],
        "sample_kinds": ["current"],
        "before_ms": 1_800_000_000_000,
    }
    assert observed[10][2] == {"market_ids": ["market-a", "market-b"]}
    assert universes.universes[0].gate_id == "gate-a"
    assert first_times.times == {"market-a": 100, "market-b": 200}
    assert changes.next_cursor == 3
    assert changes.reset_required is False
    assert ingest.gate_id == "gate-a"
    assert ingest.rows_written == 1
    assert run.size == 10
    assert status_mutation.etag == '"status"'
    assert status_payload == {"state": "ready"}
    assert universe_mutation.snapshot == "snapshot.json"
    assert universe_payload == universe_snapshot()
    assert latest.metric == "open_interest"
    assert latest.rows == {}
    assert history.has_more is False
    assert resume.metric == "open_interest"


def test_state_document_reads_expose_etags_for_sync_and_async_cas() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        kind = "status" if request.url.path.endswith("/status") else "universe"
        return httpx.Response(
            200,
            json={
                "schema_version": "1",
                "payload": {"kind": kind},
            },
            headers={"ETag": f'"{kind}-revision"'},
        )

    transport = httpx.MockTransport(handler)
    with StorageClient("https://storage.test", transport=transport) as client:
        status = client.get_status_document("gate-a")
        universe = client.get_universe_document("gate-a")

    assert status.payload == {"kind": "status"}
    assert status.etag == '"status-revision"'
    assert universe.payload == {"kind": "universe"}
    assert universe.etag == '"universe-revision"'

    async def scenario() -> None:
        async with AsyncStorageClient(
            "https://storage.test", transport=transport
        ) as client:
            async_status = await client.get_status_document("gate-a")
            async_universe = await client.get_universe_document("gate-a")
        assert async_status == status
        assert async_universe == universe

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [
        (401, AuthenticationError),
        (403, AuthenticationError),
        (404, NotFoundError),
        (409, ConflictError),
        (412, ConflictError),
        (422, ProtocolError),
        (503, ProtocolError),
    ],
)
def test_http_failures_map_to_typed_errors(
    status_code: int, error_type: type[ProtocolError]
) -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            status_code, json={"detail": "request could not be completed"}
        )
    )
    with StorageClient("https://storage.test", transport=transport) as client:
        with pytest.raises(error_type) as caught:
            client.readiness()

    assert caught.value.status_code == status_code
    assert caught.value.method == "GET"
    assert caught.value.response_body is not None


def test_transport_failure_preserves_the_original_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("service unavailable", request=request)

    with StorageClient(
        "https://storage.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(TransportError) as caught:
            client.readiness()

    assert isinstance(caught.value.cause, httpx.ConnectError)


@pytest.mark.parametrize(
    "base_url",
    [
        "http://storage.test",
        "http://192.0.2.10",
        "ftp://localhost",
        "https://user:secret@storage.test",
        "https://storage.test?mode=unsafe",
    ],
)
def test_sync_client_rejects_insecure_or_ambiguous_base_urls(
    base_url: str,
) -> None:
    with pytest.raises(ValueError):
        StorageClient(base_url, transport=httpx.MockTransport(lambda _: None))


@pytest.mark.parametrize(
    "base_url",
    [
        "http://localhost",
        "http://127.0.0.1:8080",
        "http://[::1]:8080",
        "https://storage.test",
    ],
)
def test_sync_client_accepts_https_and_loopback_http(base_url: str) -> None:
    with StorageClient(
        base_url,
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"status": "ready"})
        ),
    ) as client:
        assert client.readiness() == {"status": "ready"}


def test_async_client_enforces_the_same_transport_boundary() -> None:
    with pytest.raises(ValueError):
        AsyncStorageClient(
            "http://storage.test",
            transport=httpx.MockTransport(lambda _: None),
        )

    async def scenario() -> None:
        async with AsyncStorageClient(
            "http://localhost:8080",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json={"status": "ready"})
            ),
        ) as client:
            assert await client.readiness() == {"status": "ready"}

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "operation",
    [
        lambda client: client.stat_file("/absolute.csv"),
        lambda client: client.stat_file("folder/../outside.csv"),
        lambda client: client.get_file("file.csv", range_end=5),
        lambda client: client.get_file("file.csv", range_start=4, range_end=3),
        lambda client: client.put_file(
            "file.csv",
            b"value",
            idempotency_key="replace-1",
            if_match='"current"',
            if_none_match=True,
        ),
        lambda client: client.put_status(
            "gate-a", {"state": "ready"}, idempotency_key=""
        ),
        lambda client: client.append_run(
            "invalid/gate", "run-a", {}, idempotency_key="run-1"
        ),
        lambda client: client.append_run(
            "gate-a", ".invalid", {}, idempotency_key="run-1"
        ),
        lambda client: client.ingest_observations(
            {
                "gate_id": "gate-a",
                "batch_id": "invalid batch",
                "metric": "funding",
                "rows": [],
            },
            idempotency_key="ingest-1",
        ),
        lambda client: client.first_open_interest_times("market-a"),
        lambda client: client.latest({"metric": "funding"}, before_ms=0),
        lambda client: client.latest(
            {"metric": "funding", "before_ms": 100}, before_ms=200
        ),
        lambda client: client.changes({"after": "cursor"}),
        lambda client: client.changes({"limit": True}),
    ],
)
def test_invalid_requests_fail_before_transport(
    operation: Callable[[StorageClient], object],
) -> None:
    def unexpected(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    with StorageClient(
        "https://storage.test", transport=httpx.MockTransport(unexpected)
    ) as client:
        with pytest.raises(ValueError):
            operation(client)


def test_invalid_success_body_raises_protocol_error() -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, content=b"not-json")
    )
    with StorageClient("https://storage.test", transport=transport) as client:
        with pytest.raises(ProtocolError, match="invalid JSON"):
            client.readiness()


@pytest.mark.parametrize(
    "token",
    ["", " token", "token ", "line\nbreak", "tökén", "\x7ftoken"],
)
def test_clients_reject_tokens_that_cannot_form_safe_bearer_headers(
    token: str,
) -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(200, json={"status": "ready"})
    )
    with pytest.raises(ValueError, match="visible ASCII"):
        StorageClient("https://storage.test", token=token, transport=transport)
    with pytest.raises(ValueError, match="visible ASCII"):
        AsyncStorageClient("https://storage.test", token=token, transport=transport)


@pytest.mark.parametrize(
    ("value", "accepted"),
    [
        ("gate-a", True),
        ("A.1_b-c", True),
        (".hidden", False),
        ("contains space", False),
        ("segment/path", False),
        ("a" * 129, False),
    ],
)
def test_public_structural_identifier_validator_matches_route_contract(
    value: str, accepted: bool
) -> None:
    if accepted:
        assert validate_structural_identifier(value, field="gate_id") == value
    else:
        with pytest.raises(ValueError, match="gate_id"):
            validate_structural_identifier(value, field="gate_id")


def test_async_domain_identifiers_fail_before_transport() -> None:
    def unexpected(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    async def scenario() -> None:
        async with AsyncStorageClient(
            "https://storage.test", transport=httpx.MockTransport(unexpected)
        ) as client:
            with pytest.raises(ValueError, match="gate_id"):
                await client.get_status("invalid/gate")
            with pytest.raises(ValueError, match="run_id"):
                await client.append_run(
                    "gate-a", "invalid run", {}, idempotency_key="run-1"
                )
            with pytest.raises(ValueError, match="batch_id"):
                await client.ingest_observations(
                    {
                        "gate_id": "gate-a",
                        "batch_id": "invalid batch",
                        "metric": "funding",
                        "rows": [],
                    },
                    idempotency_key="ingest-1",
                )

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "operation",
    [
        lambda client: client.ingest_observations(
            {
                "gate_id": "gate-a",
                "batch_id": "batch-a",
                "metric": "funding",
                "rows": [funding_row()],
            },
            idempotency_key="ingest-1",
        ),
        lambda client: client.append_run(
            "gate-a",
            "run-a",
            {"completed_at_ms": OBSERVATION_TIME_MS, "status": "complete"},
            idempotency_key="run-1",
        ),
        lambda client: client.put_status(
            "gate-a", {}, idempotency_key="status-1"
        ),
        lambda client: client.latest(
            {
                "metric": "funding",
                "market_ids": ["market-a"],
                "sample_kinds": ["current"],
            }
        ),
        lambda client: client.history(
            {
                "metric": "funding",
                "start_ms": OBSERVATION_TIME_MS - 1_000,
                "stop_ms": OBSERVATION_TIME_MS + 1_000,
                "market_ids": ["market-a"],
            }
        ),
        lambda client: client.resume(
            {
                "metric": "funding",
                "requests": [
                    {
                        "market_id": "market-a",
                        "floor_ms": OBSERVATION_TIME_MS,
                        "interval_seconds": 300,
                    }
                ],
                "sample_kind": "history",
            }
        ),
    ],
)
def test_domain_success_without_a_supported_envelope_is_rejected(
    operation: Callable[[StorageClient], object],
) -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(200, json={"accepted": True})
    )
    with StorageClient("https://storage.test", transport=transport) as client:
        with pytest.raises(ProtocolError):
            operation(client)


@pytest.mark.parametrize(
    "mutation",
    [
        "zero-cursor",
        "cursor-gap",
        "row-count",
        "request-row-count",
        "gate-identity",
    ],
)
def test_ingest_success_enforces_positive_ordered_identity_bound_result(
    mutation: str,
) -> None:
    first = {
        "cursor": 1,
        "observation_time_ms": 1_786_348_800_000,
        "market_id": "market-a",
        "sample_kind": "current",
        "revision": 1,
    }
    payload = {
        "schema_version": "1",
        "gate_id": "gate-a",
        "batch_id": "batch-a",
        "metric": "open_interest",
        "rows_received": 2,
        "rows_written": 2,
        "rows_deduplicated": 0,
        "cursor_start": 1,
        "cursor_end": 2,
        "changes": [first, {**first, "cursor": 2, "revision": 2}],
    }
    malformed = copy.deepcopy(payload)
    if mutation == "zero-cursor":
        malformed["changes"][0]["cursor"] = 0
        malformed["cursor_start"] = 0
    elif mutation == "cursor-gap":
        malformed["changes"][1]["cursor"] = 3
        malformed["cursor_end"] = 3
    elif mutation == "row-count":
        malformed["rows_received"] = 3
    elif mutation == "request-row-count":
        malformed["rows_received"] = 3
        malformed["rows_deduplicated"] = 1
    else:
        malformed["gate_id"] = "gate-b"
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=malformed))
    with StorageClient("https://storage.test", transport=transport) as client:
        with pytest.raises(ProtocolError):
            client.ingest_observations(
                {
                    "gate_id": "gate-a",
                    "batch_id": "batch-a",
                    "metric": "open_interest",
                    "rows": [{}, {}],
                },
                idempotency_key="ingest-1",
            )


def test_change_page_rejects_incomplete_or_noncontiguous_records() -> None:
    incomplete = {
        "schema_version": "1",
        "changes": [{"cursor": 1}],
        "next_cursor": 1,
        "last_cursor": 1,
        "reset_required": False,
    }
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=incomplete))
    with StorageClient("https://storage.test", transport=transport) as client:
        with pytest.raises(ProtocolError):
            client.changes()

    advanced_empty = {
        "schema_version": "1",
        "changes": [],
        "next_cursor": 2,
        "last_cursor": 2,
        "reset_required": False,
    }
    transport = httpx.MockTransport(
        lambda _: httpx.Response(200, json=advanced_empty)
    )
    with StorageClient("https://storage.test", transport=transport) as client:
        with pytest.raises(ProtocolError):
            client.changes()


def test_async_client_uses_the_same_contract_and_error_mapping() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/files/observations/day.csv":
            return httpx.Response(
                206,
                content=b"data",
                headers={
                    "Content-Range": "bytes 0-3/8",
                    "ETag": '"file-1"',
                },
            )
        if request.url.path == "/v1/gates/gate-a/status":
            return httpx.Response(404, json={"detail": "resource is absent"})
        if request.url.path == "/v1/observations:ingest":
            body = json.loads(request.content)
            assert body == {
                "gate_id": "gate-a",
                "batch_id": "batch-a",
                "metric": "funding",
                "rows": [funding_row()],
            }
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "gate_id": body["gate_id"],
                    "batch_id": body["batch_id"],
                    "metric": body["metric"],
                    "rows_received": 1,
                    "rows_written": 1,
                    "rows_deduplicated": 0,
                    "cursor_start": 1,
                    "cursor_end": 1,
                    "changes": [
                        {
                            "cursor": 1,
                            "observation_time_ms": OBSERVATION_TIME_MS,
                            "market_id": "market-a",
                            "sample_kind": "current",
                            "revision": 1,
                        }
                    ],
                },
            )
        if request.url.path == "/v1/query/latest":
            body = json.loads(request.content)
            assert body == {
                "metric": "funding",
                "market_ids": ["market-a"],
                "sample_kinds": ["current"],
                "before_ms": 1_800_000_000_000,
            }
            return httpx.Response(
                200,
                json={"schema_version": "1", "metric": body["metric"], "rows": {}},
            )
        if request.url.path == "/v1/gates/universes":
            return httpx.Response(
                200, json={"schema_version": "1", "universes": []}
            )
        if request.url.path == "/v1/query/first-open-interest-times":
            return httpx.Response(
                200,
                json={"schema_version": "1", "times": {"market-a": 100}},
            )
        if request.url.path == "/v1/changes":
            return httpx.Response(
                200,
                json={
                    "schema_version": "1",
                    "changes": [],
                    "next_cursor": 0,
                    "reset_required": False,
                    "last_cursor": 0,
                },
            )
        return httpx.Response(200, json={"accepted": True})

    async def scenario() -> None:
        async with AsyncStorageClient(
            "https://storage.test",
            transport=httpx.MockTransport(handler),
        ) as client:
            download = await client.get_file(
                "observations/day.csv", range_start=0, range_end=3
            )
            assert download.content == b"data"
            ingest = await client.ingest_observations(
                {
                    "gate_id": "gate-a",
                    "batch_id": "batch-a",
                    "metric": "funding",
                    "rows": [funding_row()],
                },
                idempotency_key="ingest-1",
            )
            assert ingest.metric == "funding"
            latest = await client.latest(
                {
                    "metric": "funding",
                    "market_ids": ["market-a"],
                    "sample_kinds": ["current"],
                },
                before_ms=1_800_000_000_000,
            )
            assert latest.rows == {}
            assert (await client.list_universes()).universes == ()
            assert (
                await client.first_open_interest_times(["market-a"])
            ).times == {"market-a": 100}
            assert (await client.changes()).last_cursor == 0
            with pytest.raises(NotFoundError):
                await client.get_status("gate-a")

    asyncio.run(scenario())
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/v1/files/observations/day.csv"),
        ("POST", "/v1/observations:ingest"),
        ("POST", "/v1/query/latest"),
        ("GET", "/v1/gates/universes"),
        ("POST", "/v1/query/first-open-interest-times"),
        ("GET", "/v1/changes"),
        ("GET", "/v1/gates/gate-a/status"),
    ]
