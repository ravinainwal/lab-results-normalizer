"""Parser contract that every source-system adapter must satisfy.

Adding a new input format (CSV, HL7, XML, ...) means:
  1. Implement a subclass of ``LabResultParser``.
  2. Register it in ``registry.py``.
No changes to the storage layer or HTTP API are required.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from lab_normalizer.domain.models import LabResult


class ParseError(Exception):
    """Raised for *envelope-level* parse failures (malformed JSON, missing
    top-level structure, etc.).

    Per-record errors do **not** raise — they are collected into
    ``ParseResult.errors`` so that a single bad row does not reject an entire
    batch. This is deliberate: upstream systems occasionally send one bad
    record among many valid ones, and losing the whole batch is worse than
    losing the one row.
    """

    def __init__(self, source_system: str, message: str) -> None:
        super().__init__(f"[{source_system}] {message}")
        self.source_system = source_system


@dataclass(frozen=True, slots=True)
class RecordError:
    """A single record that could not be parsed."""

    index: int
    source_record_id: str | None
    message: str


@dataclass(frozen=True, slots=True)
class ParseResult:
    """Outcome of parsing a payload: successful records plus per-record errors."""

    results: list[LabResult] = field(default_factory=list)
    errors: list[RecordError] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results) + len(self.errors)


class LabResultParser(ABC):
    """Translates a raw payload from one source system into canonical ``LabResult`` objects."""

    #: Stable identifier for the source system this parser handles. Used as the
    #: registry key and stamped onto every produced ``LabResult.source_system``.
    source_system: str

    @abstractmethod
    def parse(self, payload: bytes) -> ParseResult:
        """Parse a raw payload into a ``ParseResult``.

        Implementations must:
          * never mutate global state,
          * raise ``ParseError`` only for envelope-level failures (malformed
            outer document),
          * catch per-record errors and report them via ``ParseResult.errors``
            so partial success is preserved.
        """
        raise NotImplementedError
