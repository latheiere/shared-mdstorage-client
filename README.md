# Shared MDStorage Client

Typed synchronous and asynchronous Python clients for the versioned Shared
MDStorage HTTP API. The package contains transport code only—no storage server
implementation or market-specific collection logic.

## Install

```bash
python -m pip install shared-mdstorage-client==0.2.0
```

Pin the exact version and artifact hash in production dependency locks.

## Use

```python
from shared_mdstorage_client import StorageClient

with StorageClient("https://storage.internal", token="...") as storage:
    status = storage.readiness()
    universes = storage.list_universes()
```

Use `AsyncStorageClient` with `async with` in asynchronous services. HTTPS is
accepted everywhere; cleartext HTTP is limited to loopback addresses.

## What it covers

- conditional file reads, replacements, and idempotent appends;
- typed observation ingestion, run, status, and universe operations;
- latest, history, first-observation, and change queries with validated
  cursor-bounded snapshot support;
- resume planning queries;
- entity-tag compare-and-swap helpers; and
- distinct authentication, conflict, protocol, and transport errors.

Domain methods validate versioned response fields before returning typed
results. Callers do not need to reconstruct transport envelopes or cursors.

## Develop and release

```bash
python -m pytest -q
python -m build --no-isolation
python scripts/verify_wheel_boundary.py dist/*.whl
```

GitHub Releases are the artifact source. PyPI publication reuses those verified
artifacts through an OIDC trusted publisher.

## Documentation

- [API reference](docs/modules/ROOT/pages/api.adoc)
- [Client boundary](docs/modules/ROOT/pages/boundary.adoc)
- [Versioning](docs/modules/ROOT/pages/versioning.adoc)
- [Support and defect reports](https://github.com/latheiere/shared-mdstorage-client/issues)

Licensed under the terms in [LICENSE](LICENSE).
