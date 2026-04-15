"""Main FastAPI application for temp-doc service."""

import logging

from fastapi import FastAPI

from app.config.logging_config import setup_logging
from app.api.routes import router

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """Create and configure FastAPI application."""
    application = FastAPI(
        title="Temp-Doc Service",
        description="Temporary document extraction and generation service",
        version="1.0.0",
    )

    # Include routes
    application.include_router(router)

    logger.info("Temp-Doc Service initialized successfully")

    return application


app = create_app()
