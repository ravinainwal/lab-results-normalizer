"""Canonical domain model for lab results.

Every source system is normalized into this shape. Downstream consumers
(the HTTP API, analytics exports, etc.) only ever see this model, never the
raw vendor payloads.
"""
from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ResultStatus(StrEnum):
    FINAL = "final"
    PRELIMINARY = "preliminary"
    CORRECTED = "corrected"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


#: Allowed value comparators. ``None`` means the reported value is exact (no
#: operator) or non-numeric (``value_text``).
ValueComparator = Literal["<", "<=", ">", ">=", "="]


class LabResult(BaseModel):
    """A single normalized lab result.

    Field semantics are intentionally close to FHIR Observation / LOINC so we
    can map to those later without another redesign.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    #: Deterministic identifier — see ``canonical_result_id``. Required: parsers
    #: must supply a stable ID so that reingestion of the same source record
    #: upserts rather than duplicates.
    result_id: str
    source_system: str = Field(
        description="Identifier of the upstream system that produced this result, e.g. 'acme_json'."
    )
    source_record_id: str | None = Field(
        default=None,
        description="The upstream system's own identifier for this result, if provided.",
    )

    patient_id: str
    test_code: str = Field(description="Canonical test code (LOINC where available).")
    test_name: str

    value_numeric: float | None = None
    value_text: str | None = None
    value_comparator: ValueComparator | None = Field(
        default=None,
        description=(
            "Relational operator attached to ``value_numeric`` when the source "
            "reports a bounded result, e.g. '<0.01' -> value_numeric=0.01, "
            "value_comparator='<'. ``None`` means exact or non-numeric."
        ),
    )
    unit: str | None = None
    reference_range: str | None = None
    status: ResultStatus = ResultStatus.UNKNOWN

    collected_at: datetime | None = None
    reported_at: datetime | None = None
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    raw: dict[str, Any] | None = Field(
        default=None,
        description="Original source record for traceability. Not exposed over the public API.",
    )

    @field_validator("patient_id", "test_code", "test_name", "source_system", "result_id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator("collected_at", "reported_at", "ingested_at")
    @classmethod
    def _require_tz_aware(cls, v: datetime | None) -> datetime | None:
        """Require explicit timezone info and normalize to UTC.

        Naive datetimes are rejected. Parsers are responsible for knowing
        their source's timezone convention and attaching it — silently
        assuming UTC can shift timestamps by the local offset and has
        already bitten us.
        """
        if v is None:
            return None
        if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
            raise ValueError(
                "datetime must be timezone-aware; parser must attach "
                "source-appropriate timezone before constructing LabResult"
            )
        return v.astimezone(UTC)

    @property
    def display_value(self) -> str:
        if self.value_numeric is not None:
            cmp = self.value_comparator
            op = cmp if cmp and cmp != "=" else ""
            unit = f" {self.unit}" if self.unit else ""
            return f"{op}{self.value_numeric}{unit}"
        return self.value_text or ""


def canonical_result_id(
    source_system: str,
    source_record_id: str | None,
    *,
    patient_id: str,
    test_code: str,
    collected_at: datetime | None,
    value_numeric: float | None,
    value_text: str | None,
    value_comparator: ValueComparator | None,
) -> str:
    """Compute a deterministic ``result_id`` for a source record.

    Strategy:
      * If the source supplies its own record ID, use ``"{source_system}:{id}"``.
        This makes reingestion idempotent — the same source record always
        hashes to the same ID and upserts in place.
      * Otherwise, fall back to a content hash over the fields that identify
        a logical result. Two records with identical identifying content
        dedupe to one, which is the conservative choice.

    ``source_system`` is always part of the key so that ID collisions between
    different vendors are impossible.
    """
    if source_record_id:
        return f"{source_system}:{source_record_id.strip()}"

    payload = "|".join(
        [
            patient_id,
            test_code,
            collected_at.isoformat() if collected_at else "",
            "" if value_numeric is None else repr(value_numeric),
            value_text or "",
            value_comparator or "",
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"{source_system}:content:{digest}"
