from __future__ import annotations

import itertools
from datetime import UTC, datetime

from lab_normalizer.domain.models import LabResult
from lab_normalizer.storage.base import ResultQuery
from lab_normalizer.storage.memory import InMemoryLabResultRepository

_id_counter = itertools.count()


def _result(**overrides: object) -> LabResult:
    base: dict[str, object] = dict(
        result_id=f"test:{next(_id_counter)}",
        source_system="test",
        patient_id="P1",
        test_code="GLU",
        test_name="Glucose",
        value_numeric=1.0,
        collected_at=datetime(2026, 4, 1, tzinfo=UTC),
    )
    base.update(overrides)
    return LabResult(**base)  # type: ignore[arg-type]


def test_add_and_get_roundtrip() -> None:
    repo = InMemoryLabResultRepository()
    r = _result()
    [rid] = repo.add_many([r])
    assert rid == r.result_id
    assert repo.get(rid) == r
    assert repo.get("nope") is None


def test_add_many_is_idempotent_on_result_id() -> None:
    """Adding a result with the same result_id must upsert, not duplicate."""
    repo = InMemoryLabResultRepository()
    r1 = _result(result_id="fixed:1", value_numeric=1.0)
    r2 = _result(result_id="fixed:1", value_numeric=2.0)  # same id, new value
    repo.add_many([r1])
    repo.add_many([r2])

    stored = repo.get("fixed:1")
    assert stored is not None
    assert stored.value_numeric == 2.0
    assert repo.count(ResultQuery()) == 1


def test_query_filters_by_patient_and_test() -> None:
    repo = InMemoryLabResultRepository()
    repo.add_many(
        [
            _result(patient_id="P1", test_code="GLU"),
            _result(patient_id="P1", test_code="HGB"),
            _result(patient_id="P2", test_code="GLU"),
        ]
    )
    assert repo.count(ResultQuery(patient_id="P1")) == 2
    assert repo.count(ResultQuery(test_code="GLU")) == 2
    assert repo.count(ResultQuery(patient_id="P1", test_code="GLU")) == 1
    assert repo.count(ResultQuery(patient_id="P9")) == 0


def test_query_filters_by_date_range() -> None:
    repo = InMemoryLabResultRepository()
    repo.add_many(
        [
            _result(collected_at=datetime(2026, 4, 1, tzinfo=UTC)),
            _result(collected_at=datetime(2026, 4, 5, tzinfo=UTC)),
            _result(collected_at=datetime(2026, 4, 10, tzinfo=UTC)),
        ]
    )
    q = ResultQuery(
        collected_from=datetime(2026, 4, 3, tzinfo=UTC),
        collected_to=datetime(2026, 4, 7, tzinfo=UTC),
    )
    assert repo.count(q) == 1


def test_query_pagination_and_ordering() -> None:
    repo = InMemoryLabResultRepository()
    repo.add_many(
        [
            _result(collected_at=datetime(2026, 4, d, tzinfo=UTC))
            for d in (1, 2, 3, 4, 5)
        ]
    )
    page = repo.query(ResultQuery(limit=2, offset=0))
    assert len(page) == 2
    # Newest first.
    assert page[0].collected_at is not None and page[1].collected_at is not None
    assert page[0].collected_at > page[1].collected_at
    oldest_page = repo.query(ResultQuery(limit=2, offset=4))
    assert oldest_page[0].collected_at is not None
    assert oldest_page[0].collected_at.day == 1
