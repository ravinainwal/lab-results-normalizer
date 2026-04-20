"""Unit tests for the Acme JSON parser.

Covers the three behaviors that previously went wrong:
  * deterministic result_id so reingestion upserts instead of duplicating
  * per-record errors collected into ``ParseResult.errors`` (no all-or-nothing)
  * bounded values ("<0.01") keep numeric fidelity via ``value_comparator``
  * naive timestamps are rejected, not silently coerced to UTC
"""
from __future__ import annotations

import pytest

from lab_normalizer.domain.models import ResultStatus
from lab_normalizer.ingestion.acme_json import AcmeJsonParser
from lab_normalizer.ingestion.base import ParseError

# ---------- happy path ----------

def test_parses_sample_payload(acme_payload: bytes) -> None:
    parsed = AcmeJsonParser().parse(acme_payload)

    assert parsed.errors == []
    assert len(parsed.results) == 4
    by_src_id = {r.source_record_id: r for r in parsed.results}

    glucose = by_src_id["R-1001"]
    assert glucose.source_system == "acme_json"
    assert glucose.patient_id == "P-42"
    assert glucose.test_code == "2345-7"  # LOINC preferred over vendor code
    assert glucose.test_name == "Glucose"
    assert glucose.value_numeric == pytest.approx(5.4)
    assert glucose.value_text is None
    assert glucose.value_comparator is None
    assert glucose.unit == "mmol/L"
    assert glucose.reference_range == "3.9-5.5"
    assert glucose.status is ResultStatus.FINAL
    assert glucose.collected_at is not None and glucose.collected_at.tzinfo is not None

    covid = by_src_id["R-1002"]
    assert covid.test_code == "COVID-PCR"  # no LOINC supplied -> vendor code
    assert covid.value_numeric is None
    assert covid.value_text == "NEGATIVE"
    assert covid.value_comparator is None


def test_deterministic_result_id(acme_payload: bytes) -> None:
    """Same payload parsed twice yields the same result_ids (idempotent reingestion)."""
    first = AcmeJsonParser().parse(acme_payload)
    second = AcmeJsonParser().parse(acme_payload)

    ids1 = sorted(r.result_id for r in first.results)
    ids2 = sorted(r.result_id for r in second.results)
    assert ids1 == ids2
    # IDs should also be human-readable, tying back to the source record.
    assert any(rid.startswith("acme_json:R-1001") for rid in ids1)


# ---------- bounded / qualitative values ----------

@pytest.mark.parametrize(
    "raw, expected_numeric, expected_text, expected_cmp",
    [
        ("<0.01", 0.01, None, "<"),
        ("<=0.5", 0.5, None, "<="),
        (">100", 100.0, None, ">"),
        (">= 200", 200.0, None, ">="),
        ("= 5", 5.0, None, "="),
        ("5.4", 5.4, None, None),
        (13.1, 13.1, None, None),
        (42, 42.0, None, None),
        ("POSITIVE", None, "POSITIVE", None),
        ("NOT DETECTED", None, "NOT DETECTED", None),
        (None, None, None, None),
        ("", None, None, None),
    ],
)
def test_value_parsing_preserves_comparator_and_numeric(
    raw: object,
    expected_numeric: float | None,
    expected_text: str | None,
    expected_cmp: str | None,
) -> None:
    from lab_normalizer.ingestion.acme_json import _parse_value

    numeric, text, comparator = _parse_value(raw)
    if expected_numeric is None:
        assert numeric is None
    else:
        assert numeric == pytest.approx(expected_numeric)
    assert text == expected_text
    assert comparator == expected_cmp


def test_bounded_value_in_full_record(acme_payload: bytes) -> None:
    """TSH record in the sample has value '<0.01' — must round-trip as numeric+operator."""
    parsed = AcmeJsonParser().parse(acme_payload)
    tsh = next(r for r in parsed.results if r.source_record_id == "R-2002")
    assert tsh.value_numeric == pytest.approx(0.01)
    assert tsh.value_text is None
    assert tsh.value_comparator == "<"
    assert tsh.unit == "mIU/L"


# ---------- partial success / per-record errors ----------

def test_single_bad_record_does_not_fail_batch() -> None:
    """One record missing patient.mrn must not block the other two."""
    payload = (
        b'{"results": ['
        b' {"id": "ok-1", "patient": {"mrn": "P1"}, "test": {"code": "GLU", "name": "Glucose"},'
        b'  "observation": {"value": 1}, "collected": "2026-04-10T08:00:00Z"},'
        b' {"id": "bad-1", "test": {"code": "GLU", "name": "Glucose"},'
        b'  "observation": {"value": 1}, "collected": "2026-04-10T08:00:00Z"},'
        b' {"id": "ok-2", "patient": {"mrn": "P2"}, "test": {"code": "GLU", "name": "Glucose"},'
        b'  "observation": {"value": 2}, "collected": "2026-04-10T09:00:00Z"}'
        b']}'
    )
    parsed = AcmeJsonParser().parse(payload)

    assert [r.source_record_id for r in parsed.results] == ["ok-1", "ok-2"]
    assert len(parsed.errors) == 1
    err = parsed.errors[0]
    assert err.index == 1
    assert err.source_record_id == "bad-1"
    assert "patient.mrn" in err.message


def test_rejects_non_object_record() -> None:
    payload = b'{"results": ["not a dict"]}'
    parsed = AcmeJsonParser().parse(payload)
    assert parsed.results == []
    assert len(parsed.errors) == 1


def test_unknown_status_maps_to_unknown() -> None:
    payload = (
        b'{"results": [{"id": "x", "patient": {"mrn": "P1"}, '
        b'"test": {"code": "GLU", "name": "Glucose"}, '
        b'"observation": {"value": 1}, "status": "?",'
        b'"collected": "2026-04-10T08:00:00Z"}]}'
    )
    parsed = AcmeJsonParser().parse(payload)
    assert len(parsed.results) == 1
    assert parsed.results[0].status is ResultStatus.UNKNOWN


# ---------- envelope-level failures still raise ----------

def test_rejects_invalid_json() -> None:
    with pytest.raises(ParseError):
        AcmeJsonParser().parse(b"not json")


def test_rejects_missing_results_array() -> None:
    with pytest.raises(ParseError):
        AcmeJsonParser().parse(b'{"labId": "ACME"}')


def test_rejects_non_object_root() -> None:
    with pytest.raises(ParseError):
        AcmeJsonParser().parse(b"[]")


# ---------- timezone strictness ----------

def test_naive_timestamp_becomes_record_error() -> None:
    """A timestamp without an offset/Z must be rejected, not silently UTC'd."""
    payload = (
        b'{"results": [{"id": "x", "patient": {"mrn": "P1"}, '
        b'"test": {"code": "GLU", "name": "Glucose"}, '
        b'"observation": {"value": 1}, '
        b'"collected": "2026-04-10T08:15:00"}]}'
    )
    parsed = AcmeJsonParser().parse(payload)
    assert parsed.results == []
    assert len(parsed.errors) == 1
    assert "timezone" in parsed.errors[0].message.lower()


def test_offset_timestamp_is_preserved_in_utc() -> None:
    """Explicit non-UTC offset must be converted to UTC, not rejected."""
    payload = (
        b'{"results": [{"id": "x", "patient": {"mrn": "P1"}, '
        b'"test": {"code": "GLU", "name": "Glucose"}, '
        b'"observation": {"value": 1}, '
        b'"collected": "2026-04-10T08:15:00-05:00"}]}'
    )
    parsed = AcmeJsonParser().parse(payload)
    assert parsed.errors == []
    [r] = parsed.results
    assert r.collected_at is not None
    assert r.collected_at.isoformat() == "2026-04-10T13:15:00+00:00"


def test_malformed_timestamp_is_record_error() -> None:
    payload = (
        b'{"results": [{"id": "x", "patient": {"mrn": "P1"}, '
        b'"test": {"code": "GLU", "name": "Glucose"}, '
        b'"observation": {"value": 1}, '
        b'"collected": "not-a-date"}]}'
    )
    parsed = AcmeJsonParser().parse(payload)
    assert parsed.results == []
    assert len(parsed.errors) == 1
