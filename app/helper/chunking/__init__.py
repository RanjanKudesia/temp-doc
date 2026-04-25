"""Self-contained extract-then-chunk service.

Takes an uploaded file, extracts its content using the appropriate pipeline
(DOCX, PDF, PPTX, HTML, Markdown, TXT), then immediately produces text chunks.

Does NOT import from helper.extract or helper.chunks service APIs.
Pipelines are imported directly and chunking logic is copied inline.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import HTTPException, UploadFile, status

from .extract_engine import extract_bytes
from .chunk_engine import ChunkEngine
from app.schemas.temp_doc_schema import (
    ChunkItem,
    ChunkingResponse,
    ExtractedData,
    ExtractedPptData,
)

logger = logging.getLogger(__name__)

_chunk_engine = ChunkEngine()

SUPPORTED_EXTENSIONS = {
    "docx", "pdf",
    "pptx", "ppt",
    "html", "htm",
    "md", "markdown",
    "txt",
}


async def chunk_document(
    file: UploadFile,
    include_media: bool = False,
) -> ChunkingResponse:
    """Extract an uploaded document and return text chunks.

    Supported formats: docx, pdf, pptx, html, htm, md, markdown, txt.

    Args:
        file: Uploaded file from a FastAPI route.
        include_media: Include media during extraction (default False for speed).

    Returns:
        ChunkingResponse with filename, extension, chunk_count, and chunks.

    Raises:
        HTTPException 400: Missing filename, empty file, unsupported format.
        HTTPException 422: Extraction failed due to corrupt/invalid file.
        HTTPException 500: Unexpected extraction or chunking error.
    """
    if not file.filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Filename is required.",
        )

    try:
        file_bytes = await file.read()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to read file: {e}",
        ) from e

    if not file_bytes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File is empty.",
        )

    extension = Path(file.filename).suffix.lower().lstrip(".")
    if extension not in SUPPORTED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Unsupported format: .{extension}. "
                f"Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
            ),
        )

    logger.info(
        "Chunking pipeline started for %s (%s bytes, ext=%s)",
        file.filename, len(file_bytes), extension,
    )

    # ── Step 1: Extract ──────────────────────────────────────────────────────
    try:
        extracted, normalized_ext = extract_bytes(
            file_bytes, extension, include_media
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Extraction failed: {e}",
        ) from e
    except Exception as e:
        logger.error("Extraction error for %s: %s", file.filename, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Extraction failed: {e}",
        ) from e

    # ── Step 2: Chunk ────────────────────────────────────────────────────────
    try:
        if isinstance(extracted, ExtractedPptData):
            raw_chunks = _chunk_engine.chunk_pptx(extracted)
        else:
            raw_chunks = _chunk_engine.chunk_docx(extracted)
    except Exception as e:
        logger.error("Chunking error for %s: %s", file.filename, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Chunking failed: {e}",
        ) from e

    chunks = [ChunkItem(text=c) for c in raw_chunks]

    logger.info(
        "Chunking pipeline complete for %s: %d chunks produced",
        file.filename, len(chunks),
    )

    return ChunkingResponse(
        filename=file.filename,
        extension=normalized_ext,
        chunk_count=len(chunks),
        chunks=chunks,
    )
