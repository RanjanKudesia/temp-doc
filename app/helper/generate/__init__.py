"""Public generation entry point.

Usage (in a route):
    from app.helper.generate import generate_document

    @router.post("/generate")
    async def generate_file(request_body: dict) -> Response:
        file_bytes, mime_type, filename = generate_document(request_body)
        return Response(
            content=file_bytes,
            media_type=mime_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
"""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import HTTPException, status

from app.helper.generate.adapters import GenerationAdapterFactory
from app.helper.generate.pipelines import (
    DocxGenerationPipeline,
    HtmlGenerationPipeline,
    MarkdownGenerationPipeline,
    PdfGenerationPipeline,
    PptGenerationPipeline,
    TextGenerationPipeline,
)
from app.schemas.temp_doc_schema import ExtractedData, ExtractedPptData, GenerateRequest

logger = logging.getLogger(__name__)

# Factory is stateless — one shared instance per process is fine.
_factory = GenerationAdapterFactory(
    docx_pipeline=DocxGenerationPipeline(),
    pdf_pipeline=PdfGenerationPipeline(),
    pptx_pipeline=PptGenerationPipeline(),
    html_pipeline=HtmlGenerationPipeline(),
    markdown_pipeline=MarkdownGenerationPipeline(),
    text_pipeline=TextGenerationPipeline(),
)

_EXTENSION_MAP = {
    "docx": ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", "docx"),
    "pdf": ("application/pdf", "pdf"),
    "pptx": ("application/vnd.openxmlformats-officedocument.presentationml.presentation", "pptx"),
    "html": ("text/html", "html"),
    "markdown": ("text/markdown", "md"),
    "text": ("text/plain", "txt"),
}

_NON_GENERATION_FORMATS = {"json", "xml"}


def generate_document(
    request_data: dict,
) -> tuple[bytes, str, str]:
    """Generate document from extracted JSON data.

    Args:
        request_data: Dictionary with keys:
            - extracted_data: Extracted document JSON (required)
            - target_format / output_format: Target format optional (defaults inferred)
            - file_name: Base filename optional
            - title: Optional document title
            - extension: Optional source format for context
            - blocks: Optional block list for advanced generation

    Returns:
        Tuple of (file_bytes, mime_type, filename).

    Raises:
        HTTPException 400: Missing extracted_data or unsupported format.
        HTTPException 422: Invalid generation payload.
        HTTPException 500: Generation error.
    """
    extracted_data_payload = request_data.get("extracted_data")
    if extracted_data_payload is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing 'extracted_data' in request body.",
        )

    requested_format = _coerce_generation_format(
        request_data.get("target_format"))
    if requested_format is None:
        requested_format = _coerce_generation_format(
            request_data.get("output_format"))
    if requested_format is None:
        requested_format = _infer_format_from_extracted_payload(
            extracted_data_payload)
    if requested_format is None:
        requested_format = _infer_format_from_extension(
            request_data.get("extension"))

    # Normalize format
    normalized_format = _normalize_format(requested_format)
    if normalized_format not in _EXTENSION_MAP:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported output format: {normalized_format}",
        )

    try:
        # Determine if PPTX based on extracted_data payload shape
        if normalized_format == "pptx":
            extracted_data = ExtractedPptData.model_validate(
                extracted_data_payload)
        else:
            extracted_data = ExtractedData.model_validate(
                extracted_data_payload)

        # Build and validate request
        normalized_request = GenerateRequest(
            output_format=normalized_format,  # type: ignore
            extracted_data=extracted_data,
            file_name=request_data.get("file_name", "document"),
            title=request_data.get("title"),
            blocks=request_data.get("blocks", []),
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid generation payload: {str(e)}",
        ) from e

    try:
        logger.info("Generating %s document", normalized_format)

        file_bytes = _factory.generate(
            output_format=normalized_format,  # type: ignore
            extracted_data=extracted_data,
            title=normalized_request.title,
        )

        mime_type, ext = _EXTENSION_MAP.get(
            normalized_format, ("application/octet-stream", normalized_format)
        )
        file_name = f"{normalized_request.file_name}.{ext}"

        return file_bytes, mime_type, file_name

    except Exception as e:
        logger.error("Generation failed: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Generation failed: {str(e)}",
        ) from e


def _normalize_format(fmt: str | None) -> str:
    """Normalize format names."""
    if not fmt:
        return "docx"
    fmt = str(fmt).lower().lstrip(".")
    if fmt in {"doc", "dox", "docx"}:
        return "docx"
    if fmt == "pdf":
        return "pdf"
    if fmt in {"ppt", "pptx"}:
        return "pptx"
    if fmt in {"html", "htm"}:
        return "html"
    if fmt in {"md", "markdown"}:
        return "markdown"
    if fmt in {"txt", "text"}:
        return "text"
    return "docx"


def _infer_format_from_extension(ext: str | None) -> str:
    """Infer generation format from source extension."""
    if not ext:
        return "docx"
    ext = str(ext).lower().lstrip(".")
    if ext == "pptx" or ext == "ppt":
        return "pptx"
    if ext == "pdf":
        return "pdf"
    if ext in {"html", "htm"}:
        return "html"
    if ext in {"md", "markdown"}:
        return "markdown"
    if ext == "txt":
        return "text"
    return "docx"


def _infer_format_from_extracted_payload(payload: object) -> str | None:
    """Infer generation format from extracted payload metadata when possible."""
    if not isinstance(payload, dict):
        return None

    doc_type = payload.get("document_type")
    if doc_type is None:
        return None

    normalized = _normalize_format(str(doc_type))
    return normalized if normalized in _EXTENSION_MAP else None


def _coerce_generation_format(fmt: object) -> str | None:
    """Return a valid generation format, ignoring non-generation values."""
    if fmt is None:
        return None

    normalized = _normalize_format(str(fmt))
    if str(fmt).lower().strip() in _NON_GENERATION_FORMATS:
        return None
    return normalized if normalized in _EXTENSION_MAP else None
