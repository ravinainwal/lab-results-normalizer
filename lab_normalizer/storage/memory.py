"""In-memory repository implementation.

Suitable for tests and local development. **Not** suitable for production:
data is lost on restart and there is no cross-process sharing. Replace with a
database-backed implementation before deploying; the ``LabResultRepository``
interface is the seam for that swap.
"""
from __future__ import annotations

import threading
from collections.abc import Iterable

from lab_normalizer.domain.models import LabResult
from lab_normalizer.storage.base import LabResultRepository, ResultQuery


class InMemoryLabResultRepository(LabResultRepository):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_id: dict[str, LabResult] = {}

    def add_many(self, results: Iterable[LabResult]) -> list[str]:
        ids: list[str] = []
        with self._lock:
            for r in results:
                self._by_id[r.result_id] = r
                ids.append(r.result_id)
        return ids

    def get(self, result_id: str) -> LabResult | None:
        return self._by_id.get(result_id)

    def query(self, q: ResultQuery) -> list[LabResult]:
        matches = [r for r in self._by_id.values() if _matches(r, q)]
        matches.sort(key=lambda r: (r.collected_at or r.ingested_at), reverse=True)
        return matches[q.offset : q.offset + q.limit]

    def count(self, q: ResultQuery) -> int:
        return sum(1 for r in self._by_id.values() if _matches(r, q))


def _matches(r: LabResult, q: ResultQuery) -> bool:
    if q.patient_id is not None and r.patient_id != q.patient_id:
        return False
    if q.test_code is not None and r.test_code != q.test_code:
        return False
    if q.source_system is not None and r.source_system != q.source_system:
        return False
    if q.collected_from is not None:
        if r.collected_at is None or r.collected_at < q.collected_from:
            return False
    if q.collected_to is not None:
        if r.collected_at is None or r.collected_at > q.collected_to:
            return False
    return True
