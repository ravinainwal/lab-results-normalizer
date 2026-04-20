from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lab_normalizer.api.app import create_app
from lab_normalizer.storage.memory import InMemoryLabResultRepository

SAMPLES = Path(__file__).parent.parent / "samples"


@pytest.fixture
def acme_payload() -> bytes:
    return (SAMPLES / "acme_labs.json").read_bytes()


@pytest.fixture
def client() -> TestClient:
    # Fresh repository per test so state does not leak between cases.
    app = create_app(repository=InMemoryLabResultRepository())
    return TestClient(app)
