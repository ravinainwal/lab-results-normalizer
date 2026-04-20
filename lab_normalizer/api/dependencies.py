from __future__ import annotations

from fastapi import Request

from lab_normalizer.ingestion.registry import ParserRegistry
from lab_normalizer.storage.base import LabResultRepository


def get_repository(request: Request) -> LabResultRepository:
    return request.app.state.repository  # type: ignore[no-any-return]


def get_parser_registry(request: Request) -> ParserRegistry:
    return request.app.state.parser_registry  # type: ignore[no-any-return]
