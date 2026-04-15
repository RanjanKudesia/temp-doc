"""Public chunking entry point.

Usage (in a route):
    from app.helper.chunks import create_chunks

    @router.post("/chunks")
    async def chunks_endpoint(request_body: dict) -> ChunkResponse:
        return await create_chunks(request_body)
"""

from __future__ import annotations

import logging

from fastapi import HTTPException, status

from app.schemas.temp_doc_schema import ChunkResponse, ExtractedData, ExtractedPptData
from app.services.chunking_service import ChunkingService

logger = logging.getLogger(__name__)

# ChunkingService is stateless — one shared instance per process is fine.
_service = ChunkingService()


def create_chunks(request_data: dict) -> ChunkResponse:
    """Create meaningful chunks from extracted document JSON.

    Supports DOCX, PDF, Markdown, TXT, and PPTX extracted JSON.
    Accepts the same payload shape as `/generate`, including direct paste of the
    full `/extract` response.

    Args:
        request_data: Dictionary with keys:
            - extracted_data: Extracted document JSON (required)
            - extension / input_format: Source format optional (defaults to docx)
            - filename / file_name: Optional filename
            - blocks: Optional block list

    Returns:
        ChunkResponse with filename and chunks list.

    Raises:
        HTTPException 400: Missing extracted_data or unsupported format.
        HTTPException 422: Invalid chunking payload.
    """
    extracted_data_payload = request_data.get("extracted_data")
    if extracted_data_payload is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing 'extracted_data' in request body.",
        )

    source_extension = (
        request_data.get("extension") or request_data.get(
            "input_format") or "docx"
    )
    normalized_extension = _normalize_extension(str(source_extension))

    _SUPPORTED_CHUNK_FORMATS = {"docx", "pptx", "pdf", "markdown", "text"}
    if normalized_extension not in _SUPPORTED_CHUNK_FORMATS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The /chunks endpoint supports docx, pdf, pptx, markdown, and txt extracted JSON only.",
        )

    try:
        if normalized_extension == "pptx":
            extracted_data: ExtractedData | ExtractedPptData = ExtractedPptData.model_validate(
                extracted_data_payload
            )
        else:
            extracted_data = ExtractedData.model_validate(
                extracted_data_payload)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid chunking payload: {str(e)}",
        ) from e

    try:
        filename = _normalize_chunk_filename(
            request_data.get("filename") or request_data.get("file_name"),
            normalized_extension,
        )

        if isinstance(extracted_data, ExtractedPptData):
            chunks = _service.chunk_pptx(extracted_data)
        else:
            chunks = _service.chunk_docx(extracted_data)

        return ChunkResponse(
            filename=filename,
            chunks=[{"text": chunk} for chunk in chunks],
        )
    except Exception as e:
        logger.error("Chunking failed: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Chunking failed: {str(e)}",
        ) from e


def _normalize_extension(extension: str | None) -> str:
    """Normalize extension to standard format."""
    if not extension:
        return "docx"

    normalized = extension.lower().lstrip(".")
    if normalized in {"doc", "dox", "docx"}:
        return "docx"
    if normalized == "pdf":
        return "pdf"
    if normalized in {"ppt", "pptx"}:
        return "pptx"
    if normalized in {"html", "htm"}:
        return "html"
    if normalized in {"md", "markdown"}:
        return "markdown"
    if normalized in {"txt", "text"}:
        return "text"

    return "docx"


def _normalize_chunk_filename(filename: str | None, extension: str) -> str:
    """Normalize chunk filename with extension."""
    normalized_name = (filename or "document").strip() or "document"
    suffix = f".{extension}"
    if normalized_name.lower().endswith(suffix):
        return normalized_name
    return f"{normalized_name}{suffix}"
