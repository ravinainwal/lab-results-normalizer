from __future__ import annotations

import logging

from fastapi import FastAPI

from lab_normalizer.api.routes import router
from lab_normalizer.config import settings
from lab_normalizer.ingestion.registry import ParserRegistry, build_default_registry
from lab_normalizer.storage.base import LabResultRepository
from lab_normalizer.storage.memory import InMemoryLabResultRepository


def create_app(
    repository: LabResultRepository | None = None,
    parser_registry: ParserRegistry | None = None,
) -> FastAPI:
    """Application factory.

    Dependencies are injectable so tests (and future production wiring, e.g. a
    Postgres-backed repository) can supply their own implementations.
    """
    logging.basicConfig(level=settings.log_level)

    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        description="Normalized lab results API",
    )
    app.state.repository = repository or InMemoryLabResultRepository()
    app.state.parser_registry = parser_registry or build_default_registry()
    app.include_router(router, prefix="/v1")
    return app


# ASGI entrypoint for `uvicorn lab_normalizer.api.app:app`
app = create_app()
