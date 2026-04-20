"""Registry mapping source-system identifiers to parser instances.

The HTTP layer and any batch-ingest jobs look parsers up here by name; they do
not import concrete parser classes directly. Adding a new format therefore only
touches this module plus the new parser file.
"""
from __future__ import annotations

from lab_normalizer.ingestion.acme_json import AcmeJsonParser
from lab_normalizer.ingestion.base import LabResultParser


class UnknownSourceSystemError(KeyError):
    pass


class ParserRegistry:
    def __init__(self) -> None:
        self._parsers: dict[str, LabResultParser] = {}

    def register(self, parser: LabResultParser) -> None:
        key = parser.source_system
        if key in self._parsers:
            raise ValueError(f"parser already registered for source '{key}'")
        self._parsers[key] = parser

    def get(self, source_system: str) -> LabResultParser:
        try:
            return self._parsers[source_system]
        except KeyError as exc:
            raise UnknownSourceSystemError(source_system) from exc

    def sources(self) -> list[str]:
        return sorted(self._parsers)


def build_default_registry() -> ParserRegistry:
    registry = ParserRegistry()
    registry.register(AcmeJsonParser())
    # Future: registry.register(QuestCsvParser()), registry.register(Hl7Parser()), ...
    return registry
