"""Parser for the 'Acme Labs' JSON feed.

Acme sends a JSON document shaped roughly like::

    {
      "labId": "ACME",
      "results": [
        {
          "id": "R-1001",
          "patient": {"mrn": "P-42"},
          "test": {"code": "GLU", "name": "Glucose", "loinc": "2345-7"},
          "observation": {"value": "5.4", "units": "mmol/L", "refRange": "3.9-5.5"},
          "status": "F",
          "collected": "2026-04-10T08:15:00Z",
          "reported": "2026-04-10T12:00:00Z"
        }
      ]
    }

This intentionally differs from the canonical model so the normalization step
is meaningful (field renaming, status code mapping, numeric coercion, LOINC
preference over the vendor's own test code).
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from pydantic import ValidationError

from lab_normalizer.domain.models import (
    LabResult,
    ResultStatus,
    ValueComparator,
    canonical_result_id,
)
from lab_normalizer.ingestion.base import (
    LabResultParser,
    ParseError,
    ParseResult,
    RecordError,
)

_STATUS_MAP: dict[str, ResultStatus] = {
    "F": ResultStatus.FINAL,
    "P": ResultStatus.PRELIMINARY,
    "C": ResultStatus.CORRECTED,
    "X": ResultStatus.CANCELLED,
}

# Matches an optional leading relational operator followed by the value body.
# Order matters: two-char operators must be tried before their single-char
# prefixes.
_COMPARATOR_RE = re.compile(r"^\s*(<=|>=|<|>|=)\s*(.+?)\s*$")


class AcmeJsonParser(LabResultParser):
    source_system = "acme_json"

    def parse(self, payload: bytes) -> ParseResult:
        try:
            doc = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ParseError(self.source_system, f"invalid JSON: {exc.msg}") from exc

        if not isinstance(doc, dict):
            raise ParseError(self.source_system, "top-level JSON must be an object")

        raw_results = doc.get("results")
        if not isinstance(raw_results, list):
            raise ParseError(self.source_system, "'results' must be an array")

        results: list[LabResult] = []
        errors: list[RecordError] = []
        for idx, item in enumerate(raw_results):
            source_record_id = _safe_str(item.get("id")) if isinstance(item, dict) else None
            try:
                results.append(self._parse_one(item))
            except (RecordParseError, ValidationError, TypeError, ValueError, KeyError) as exc:
                errors.append(
                    RecordError(
                        index=idx,
                        source_record_id=source_record_id,
                        message=_record_error_message(exc),
                    )
                )
        return ParseResult(results=results, errors=errors)

    def _parse_one(self, item: Any) -> LabResult:
        if not isinstance(item, dict):
            raise RecordParseError("record must be a JSON object")

        patient = item.get("patient") or {}
        test = item.get("test") or {}
        obs = item.get("observation") or {}
        if not isinstance(patient, dict) or not isinstance(test, dict) or not isinstance(obs, dict):
            raise RecordParseError("patient/test/observation must be objects")

        mrn = patient.get("mrn")
        if not mrn:
            raise RecordParseError("missing patient.mrn")

        # Prefer LOINC if the vendor supplies one; fall back to their own code.
        test_code = test.get("loinc") or test.get("code")
        test_name = test.get("name")
        if not test_code or not test_name:
            raise RecordParseError("missing test.code/test.loinc or test.name")

        value_numeric, value_text, comparator = _parse_value(obs.get("value"))
        raw_collected = obs.get("collected") if "collected" in obs else item.get("collected")
        collected_at = _parse_ts(raw_collected)
        reported_at = _parse_ts(item.get("reported"))

        source_record_id = _safe_str(item.get("id"))
        patient_id = str(mrn)
        canonical_test_code = str(test_code)

        return LabResult(
            result_id=canonical_result_id(
                self.source_system,
                source_record_id,
                patient_id=patient_id,
                test_code=canonical_test_code,
                collected_at=collected_at,
                value_numeric=value_numeric,
                value_text=value_text,
                value_comparator=comparator,
            ),
            source_system=self.source_system,
            source_record_id=source_record_id,
            patient_id=patient_id,
            test_code=canonical_test_code,
            test_name=str(test_name),
            value_numeric=value_numeric,
            value_text=value_text,
            value_comparator=comparator,
            unit=obs.get("units"),
            reference_range=obs.get("refRange"),
            status=_STATUS_MAP.get(str(item.get("status", "")).upper(), ResultStatus.UNKNOWN),
            collected_at=collected_at,
            reported_at=reported_at,
            raw=item,
        )


class RecordParseError(ValueError):
    """Internal marker for per-record failures — caught in ``parse`` and
    converted to a ``RecordError`` entry on the ``ParseResult``."""


def _parse_value(
    raw: Any,
) -> tuple[float | None, str | None, ValueComparator | None]:
    """Split a raw value into (numeric, text, comparator).

    Handles bounded results like ``"<0.01"`` and ``">= 100"`` — the comparator
    is retained so analytics doesn't lose the fact that the value is a bound.
    Qualitative results fall back to ``value_text``.
    """
    if raw is None:
        return None, None, None
    if isinstance(raw, bool):
        # bool is a subclass of int; treat as qualitative text rather than 1/0.
        return None, str(raw), None
    if isinstance(raw, (int, float)):
        return float(raw), None, None

    text = str(raw).strip()
    if not text:
        return None, None, None

    comparator: ValueComparator | None = None
    numeric_part = text
    m = _COMPARATOR_RE.match(text)
    if m:
        op, body = m.group(1), m.group(2)
        comparator = op  # type: ignore[assignment]  # Literal narrowing from regex
        numeric_part = body

    try:
        return float(numeric_part), None, comparator
    except ValueError:
        # Either a qualitative value ("POSITIVE") or an operator attached to
        # non-numeric text — preserve the full original string rather than
        # silently dropping the operator.
        return None, text, None


def _parse_ts(raw: Any) -> datetime | None:
    """Parse an Acme timestamp, requiring an explicit timezone.

    Acme's contract says all timestamps are ISO-8601 with a trailing ``Z`` or
    explicit offset. A naive timestamp indicates either a broken feed or a
    format change on the vendor's side; either way we refuse to guess the
    zone, because silently assuming UTC has already caused bugs.
    """
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            raise RecordParseError("timestamp is missing timezone info")
        return raw

    text = str(raw)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise RecordParseError(f"invalid timestamp {raw!r}: {exc}") from exc

    if parsed.tzinfo is None:
        raise RecordParseError(
            f"timestamp {raw!r} has no timezone; Acme feed contract requires an offset or 'Z'"
        )
    return parsed


def _safe_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _record_error_message(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        # Condense Pydantic's multi-line error into something log-friendly.
        errs = exc.errors()
        if errs:
            first = errs[0]
            loc = ".".join(str(p) for p in first.get("loc", ()))
            return f"validation failed at '{loc}': {first.get('msg')}"
    return str(exc)
