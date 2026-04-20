"""HTTP request/response schemas.

Kept separate from the domain model so the wire format can evolve (versioning,
field hiding) independently of internal representation.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from lab_normalizer.domain.models import LabResult, ResultStatus, ValueComparator


class LabResultOut(BaseModel):
    """Public representation of a lab result. ``raw`` is intentionally omitted."""

    model_config = ConfigDict(from_attributes=True)

    result_id: str
    source_system: str
    source_record_id: str | None
    patient_id: str
    test_code: str
    test_name: str
    value_numeric: float | None
    value_text: str | None
    value_comparator: ValueComparator | None
    unit: str | None
    reference_range: str | None
    status: ResultStatus
    collected_at: datetime | None
    reported_at: datetime | None
    ingested_at: datetime

    @classmethod
    def from_domain(cls, r: LabResult) -> LabResultOut:
        return cls.model_validate(r, from_attributes=True)


class RejectedRecord(BaseModel):
    """A single record that failed to parse — reported alongside accepted results."""

    index: int
    source_record_id: str | None
    error: str


class IngestResponse(BaseModel):
    """Ingest outcome. Partial success is expected: ``ingested`` may be smaller
    than the number of records in the payload, with the difference explained
    in ``rejected``."""

    source_system: str
    ingested: int
    rejected_count: int
    result_ids: list[str]
    rejected: list[RejectedRecord]


class ResultsPage(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[LabResultOut]
