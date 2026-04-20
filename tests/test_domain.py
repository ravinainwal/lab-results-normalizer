"""Domain-layer tests for the canonical model and ID helper."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from lab_normalizer.domain.models import LabResult, canonical_result_id


def _valid_kwargs(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = dict(
        result_id="test:1",
        source_system="test",
        patient_id="P1",
        test_code="GLU",
        test_name="Glucose",
    )
    base.update(overrides)
    return base


# ---------- timezone strictness ----------

def test_naive_collected_at_is_rejected() -> None:
    with pytest.raises(ValidationError) as exc_info:
        LabResult(**_valid_kwargs(collected_at=datetime(2026, 4, 10, 8, 15, 0)))
    assert "timezone-aware" in str(exc_info.value)


def test_naive_reported_at_is_rejected() -> None:
    with pytest.raises(ValidationError):
        LabResult(**_valid_kwargs(reported_at=datetime(2026, 4, 10, 8, 15, 0)))


def test_non_utc_tz_is_normalized_to_utc() -> None:
    est = timezone(timedelta(hours=-5))
    r = LabResult(
        **_valid_kwargs(collected_at=datetime(2026, 4, 10, 8, 15, tzinfo=est))
    )
    assert r.collected_at is not None
    assert r.collected_at.utcoffset() == timedelta(0)
    assert r.collected_at.hour == 13  # 08:15-05:00 -> 13:15 UTC


def test_utc_tz_passes_through() -> None:
    r = LabResult(
        **_valid_kwargs(collected_at=datetime(2026, 4, 10, 8, 15, tzinfo=UTC))
    )
    assert r.collected_at == datetime(2026, 4, 10, 8, 15, tzinfo=UTC)


# ---------- result_id is required ----------

def test_result_id_is_required() -> None:
    kwargs = _valid_kwargs()
    del kwargs["result_id"]
    with pytest.raises(ValidationError):
        LabResult(**kwargs)


def test_result_id_must_be_non_empty() -> None:
    with pytest.raises(ValidationError):
        LabResult(**_valid_kwargs(result_id="   "))


# ---------- canonical_result_id ----------

def test_canonical_id_uses_source_record_id_when_present() -> None:
    rid = canonical_result_id(
        "acme_json",
        "R-1001",
        patient_id="P-42",
        test_code="2345-7",
        collected_at=datetime(2026, 4, 10, 8, 15, tzinfo=UTC),
        value_numeric=5.4,
        value_text=None,
        value_comparator=None,
    )
    assert rid == "acme_json:R-1001"


def test_canonical_id_is_stable_across_calls() -> None:
    args = dict(
        patient_id="P-42",
        test_code="2345-7",
        collected_at=datetime(2026, 4, 10, 8, 15, tzinfo=UTC),
        value_numeric=5.4,
        value_text=None,
        value_comparator=None,
    )
    assert canonical_result_id("acme_json", "R-1001", **args) == canonical_result_id(
        "acme_json", "R-1001", **args
    )


def test_canonical_id_falls_back_to_content_hash_without_source_id() -> None:
    args = dict(
        patient_id="P-42",
        test_code="2345-7",
        collected_at=datetime(2026, 4, 10, 8, 15, tzinfo=UTC),
        value_numeric=5.4,
        value_text=None,
        value_comparator=None,
    )
    rid1 = canonical_result_id("acme_json", None, **args)
    rid2 = canonical_result_id("acme_json", None, **args)
    assert rid1 == rid2
    assert rid1.startswith("acme_json:content:")


def test_canonical_id_differs_by_source_system() -> None:
    args = dict(
        patient_id="P-42",
        test_code="2345-7",
        collected_at=datetime(2026, 4, 10, 8, 15, tzinfo=UTC),
        value_numeric=5.4,
        value_text=None,
        value_comparator=None,
    )
    a = canonical_result_id("acme_json", "R-1001", **args)
    b = canonical_result_id("quest_csv", "R-1001", **args)
    assert a != b


def test_canonical_id_strips_whitespace_in_source_record_id() -> None:
    rid = canonical_result_id(
        "acme_json",
        "  R-1001  ",
        patient_id="P",
        test_code="T",
        collected_at=None,
        value_numeric=None,
        value_text=None,
        value_comparator=None,
    )
    assert rid == "acme_json:R-1001"
