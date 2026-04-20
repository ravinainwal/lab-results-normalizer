from __future__ import annotations

import logging
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status

from lab_normalizer.api.dependencies import get_parser_registry, get_repository
from lab_normalizer.api.schemas import IngestResponse, LabResultOut, RejectedRecord, ResultsPage
from lab_normalizer.config import settings
from lab_normalizer.ingestion.base import ParseError
from lab_normalizer.ingestion.registry import ParserRegistry, UnknownSourceSystemError
from lab_normalizer.storage.base import LabResultRepository, ResultQuery

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/sources", tags=["meta"])
def list_sources(
    registry: Annotated[ParserRegistry, Depends(get_parser_registry)],
) -> list[str]:
    return registry.sources()


@router.post(
    "/ingest/{source_system}",
    response_model=IngestResponse,
    status_code=status.HTTP_200_OK,
    tags=["ingest"],
)
async def ingest(
    source_system: str,
    file: Annotated[UploadFile, File(description="Raw payload from the source system")],
    registry: Annotated[ParserRegistry, Depends(get_parser_registry)],
    repo: Annotated[LabResultRepository, Depends(get_repository)],
) -> IngestResponse:
    try:
        parser = registry.get(source_system)
    except UnknownSourceSystemError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown source system '{source_system}'; known: {registry.sources()}",
        ) from exc

    payload = await file.read()
    if len(payload) > settings.max_upload_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"payload exceeds {settings.max_upload_bytes} bytes",
        )

    try:
        parsed = parser.parse(payload)
    except ParseError as exc:
        # Envelope-level failure: nothing was parseable at all.
        logger.warning("envelope parse failed for source=%s: %s", source_system, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    ids = repo.add_many(parsed.results)
    if parsed.errors:
        logger.warning(
            "ingest source=%s accepted=%d rejected=%d",
            source_system,
            len(ids),
            len(parsed.errors),
        )
    else:
        logger.info("ingest source=%s accepted=%d", source_system, len(ids))

    return IngestResponse(
        source_system=source_system,
        ingested=len(ids),
        rejected_count=len(parsed.errors),
        result_ids=ids,
        rejected=[
            RejectedRecord(
                index=e.index,
                source_record_id=e.source_record_id,
                error=e.message,
            )
            for e in parsed.errors
        ],
    )


@router.get("/results", response_model=ResultsPage, tags=["results"])
def list_results(
    repo: Annotated[LabResultRepository, Depends(get_repository)],
    patient_id: str | None = None,
    test_code: str | None = None,
    source_system: str | None = None,
    collected_from: datetime | None = None,
    collected_to: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ResultsPage:
    q = ResultQuery(
        patient_id=patient_id,
        test_code=test_code,
        source_system=source_system,
        collected_from=collected_from,
        collected_to=collected_to,
        limit=limit,
        offset=offset,
    )
    items = [LabResultOut.from_domain(r) for r in repo.query(q)]
    return ResultsPage(total=repo.count(q), limit=limit, offset=offset, items=items)


@router.get("/results/{result_id}", response_model=LabResultOut, tags=["results"])
def get_result(
    result_id: str,
    repo: Annotated[LabResultRepository, Depends(get_repository)],
) -> LabResultOut:
    result = repo.get(result_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="result not found")
    return LabResultOut.from_domain(result)
