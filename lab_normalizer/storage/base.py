"""Repository contract for persisted lab results.

The API layer depends only on this interface so the backing store can be
swapped (in-memory -> Postgres -> whatever) without touching request handlers.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from lab_normalizer.domain.models import LabResult


@dataclass(frozen=True, slots=True)
class ResultQuery:
    """Filter parameters for querying stored results. All fields are optional."""

    patient_id: str | None = None
    test_code: str | None = None
    source_system: str | None = None
    collected_from: datetime | None = None
    collected_to: datetime | None = None
    limit: int = 100
    offset: int = 0


class LabResultRepository(ABC):
    @abstractmethod
    def add_many(self, results: Iterable[LabResult]) -> list[str]:
        """Persist results and return the assigned ``result_id`` values."""

    @abstractmethod
    def get(self, result_id: str) -> LabResult | None: ...

    @abstractmethod
    def query(self, q: ResultQuery) -> list[LabResult]: ...

    @abstractmethod
    def count(self, q: ResultQuery) -> int: ...
