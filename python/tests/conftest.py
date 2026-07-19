"""Shared fixtures: paths and SHA-256 pins for the spec conformance files."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parents[2] / "spec" / "v0.1" / "fixtures"

# Pinned in spec/v0.1/fixtures/README.md; a mismatch means a stale checkout.
FIXTURE_SHA256 = {
    "minimal": "1e40278e2f58a597faef56107bd6a31048cabf5b5173619f8112ad787c6f658a",
    "nyc_taxi_3_rows": (
        "d36315647ea15fff8834e090da1a13b3a6124216de25caac93a066b14a7ba90b"
    ),
    "ts_sorted": "8ddd8bf09ce7d341fba9951ef9bab9a2ee0d0f07c3cfd9d78704089582d0a15b",
    "date32": "fb1ff4fe5b7f6d00c54f3240cada2020061982bbff85010896268a513cba2303",
}


def fixture_path(name: str) -> Path:
    return FIXTURES / name / f"{name}.acta"


def fixture_bytes(name: str) -> bytes:
    path = fixture_path(name)
    if not path.exists():
        pytest.skip(f"spec fixture {name} not available outside the repository")
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    assert digest == FIXTURE_SHA256[name], (
        f"fixture {name} does not match its pinned SHA-256; stale checkout?"
    )
    return data


@pytest.fixture(params=sorted(FIXTURE_SHA256))
def any_fixture(request) -> tuple[str, bytes]:
    return request.param, fixture_bytes(request.param)
