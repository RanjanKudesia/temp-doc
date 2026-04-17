"""Public extraction entry point.

Usage (in a route):
    from app.helper.extract import extract_document

    @router.post("/extract")
    async def extract_file(file: UploadFile, include_media: bool = True) -> ExtractResponse:
        return await extract_document(file, include_media=include_media)
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import HTTPException, UploadFile, status

from .adapters import (
    DocxJsonExtractionAdapter,
    HtmlJsonExtractionAdapter,
    MarkdownJsonExtractionAdapter,
    PptJsonExtractionAdapter,
    TextJsonExtractionAdapter,
)
from .pipelines import (
    DocxExtractionPipeline,
    HtmlExtractionPipeline,
    MarkdownExtractionPipeline,
    PdfConversionPipeline,
    PptExtractionPipeline,
    TextExtractionPipeline,
)
from ...schemas.temp_doc_schema import (
    ExtractedData,
    ExtractedPptData,
    ExtractResponse,
)

logger = logging.getLogger(__name__)

# Pipelines are stateless — one shared instance per process is fine.
_docx_adapter = DocxJsonExtractionAdapter(DocxExtractionPipeline())
_html_adapter = HtmlJsonExtractionAdapter(HtmlExtractionPipeline())
_markdown_adapter = MarkdownJsonExtractionAdapter(MarkdownExtractionPipeline())
_text_adapter = TextJsonExtractionAdapter(TextExtractionPipeline())
_ppt_adapter = PptJsonExtractionAdapter(PptExtractionPipeline())
_pdf = PdfConversionPipeline()


async def extract_document(
    file: UploadFile,
    include_media: bool = True,
) -> ExtractResponse:
    """Extract any supported document file and return structured JSON.

    Supported formats: docx, pdf, pptx, html, htm, md, markdown, txt.

    Args:
        file: Uploaded file from a FastAPI route.
        include_media: Include media payload (images/assets) when true.

    Returns:
        ExtractResponse with extension, output_format, and extracted_data.

    Raises:
        HTTPException 400: missing filename, empty file, unsupported format.
        HTTPException 500: extraction error.
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
    logger.info("Extracting %s file: %s", extension, file.filename)

    try:
        if extension == "docx":
            payload = ExtractedData.model_validate(
                _docx_adapter.run(file_bytes, include_media=include_media))

        elif extension == "pdf":
            payload = ExtractedData.model_validate(
                _pdf.run(file_bytes, include_media=include_media)
            )

        elif extension in ("ppt", "pptx"):
            payload = ExtractedPptData.model_validate(
                _ppt_adapter.run(file_bytes, include_media=include_media))

        elif extension in ("html", "htm"):
            payload = ExtractedData.model_validate(
                _html_adapter.run(file_bytes, include_media=include_media))

        elif extension in ("md", "markdown"):
            payload = ExtractedData.model_validate(
                _markdown_adapter.run(file_bytes, include_media=include_media))

        elif extension == "txt":
            payload = ExtractedData.model_validate(
                _text_adapter.run(file_bytes, include_media=include_media))

        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file format: .{extension}",
            )

        return ExtractResponse(
            extension=extension,
            output_format="json",
            extracted_data=payload,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Extraction failed for %s: %s", file.filename, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Extraction failed: {e}",
        ) from e
