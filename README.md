# Shared MDStorage Client

This distribution provides synchronous and asynchronous Python clients for the
versioned Shared MDStorage HTTP API. It covers conditional file replacement, idempotent
append operations, collector ingestion and state operations, chart-oriented
queries, change retrieval, and readiness checks.

The client maps authentication, missing-resource, write-conflict, transport,
and invalid-protocol failures to distinct exception types. File writes require
an idempotency key. Conditional replacement supports either an entity tag or a
create-only precondition.

The constructor accepts HTTPS endpoints everywhere. Cleartext HTTP is accepted
only for `localhost` and numeric loopback addresses, so application code cannot
bypass the service transport boundary for a remote endpoint.

Bearer tokens must use visible ASCII without surrounding whitespace. Gate,
run, and batch identifiers use the public
`validate_structural_identifier()` contract before any request is sent.

## Install

Release wheels are attached to GitHub Releases and published to PyPI from the
same build artifact. Consumers should lock the exact released version and wheel
hash in their dependency input. The package contains no server implementation
and can be installed independently:

```console
python -m pip install shared-mdstorage-client==0.1.0
```

The initial private end-to-end rollout may install the wheel directly from the
corresponding GitHub Release asset with its SHA-256 hash pinned by the consumer.

```python
from shared_mdstorage_client import StorageClient

with StorageClient("https://storage.internal", token="...") as storage:
    page = storage.list_files(prefix="observations/")
```

Use `AsyncStorageClient` with `async with` for asynchronous applications.

## Versioned API mapping

Both clients expose the same operation names and return types.

| Client operation | HTTP operation |
| --- | --- |
| `list_files` | `GET /v1/files` |
| `stat_file` | `HEAD /v1/files/{path}` |
| `get_file` | `GET /v1/files/{path}` |
| `put_file` | `PUT /v1/files/{path}` |
| `append_file` | `POST /v1/files/{path}:append` |
| `ingest_observations` | `POST /v1/observations:ingest` |
| `append_run` | `POST /v1/gates/{gate_id}/runs/{run_id}/events:append` |
| `put_status`, `get_status`, `get_status_document` | `PUT`, `GET /v1/gates/{gate_id}/status` |
| `put_universe` | `PUT /v1/gates/{gate_id}/universe` |
| `get_universe`, `get_universe_document` | `GET /v1/gates/{gate_id}/universe/current` |
| `list_universes` | `GET /v1/gates/universes` |
| `latest` | `POST /v1/query/latest` |
| `history` | `POST /v1/query/history` |
| `resume` | `POST /v1/query/resume` |
| `first_open_interest_times` | `POST /v1/query/first-open-interest-times` |
| `changes` | `GET /v1/changes` |
| `readiness` | `GET /readyz` |

`latest(..., before_ms=value)` restricts the lookup to observations whose
observation time is strictly earlier than the positive millisecond timestamp.

`put_status` and `put_universe` accept the logical state document and wrap it in
the API's `{"payload": ...}` transport envelope. Universe callers supply the
direct schema-1 document with top-level `markets`; no nested universe/monitoring
envelope is used.

`get_status_document` and `get_universe_document` return a
`VersionedDocument` containing the logical payload, response entity tag, and
schema version. Pass its `etag` to the corresponding PUT method's `if_match`
argument for compare-and-swap. The convenience `get_status` and `get_universe`
methods return the logical payload directly.

Domain mutations and queries validate every required schema-1 response field
before returning typed `ObservationIngestResult`, `RunAppendResult`,
`StateMutation`, `LatestResult`, `HistoryPage`, and `ResumeResult` values.
`ChangePage.changes` contains typed `CommittedObservationChange` values so a
consumer never has to reconstruct the recovery-feed envelope.

Generic file compare-and-swap reads the entity tag from `stat_file` or
`get_file().metadata`, then calls `put_file(..., if_match=etag)`. Use
`if_none_match=True` for create-only writes. Stale conditions raise
`ConflictError`; a caller may reread, reapply its logical update, and retry with
a new idempotency key.
