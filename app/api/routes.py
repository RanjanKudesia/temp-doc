"""API routes for temp-doc service."""

import logging
from typing import Annotated

from fastapi import APIRouter, UploadFile, Body, Query, Response

from ..helper.extract import extract_document
from ..helper.generate import generate_document
from ..helper.chunks import create_chunks
from ..schemas.temp_doc_schema import (
    ChunkResponse,
    ExtractResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/extract")
async def extract_file(
    file: UploadFile,
    include_media: Annotated[
        bool,
        Query(description="Include extracted media objects in the response."),
    ] = True,
) -> ExtractResponse:
    """Extract document to JSON format.

    - **file**: Document file (DOCX, PDF, PPTX, HTML, MD, TXT)
    - **include_media**: Include media payload (images/assets) when true

    Returns extracted content in JSON format.
    """
    return await extract_document(file, include_media=include_media)


@router.post("/generate")
async def generate_file(request_body: Annotated[dict, Body(...)]) -> Response:
    """Generate document from extracted JSON data.

    - **output_format**: Target format (docx, pdf, pptx, html, markdown, text)
    - **extracted_data**: Extracted document data in JSON format
    - **file_name**: Optional base filename (without extension)
    - **title**: Optional document title

    This endpoint also accepts direct JSON copied from `/extract` response.
    In that case, `extracted_data` is read from the payload and format defaults to
    the extracted `document_type` (for example `pptx`) unless `output_format` or
    `target_format` is provided.

    Returns the generated document file as binary data.
    """
    file_bytes, mime_type, file_name = generate_document(request_body)

    return Response(
        content=file_bytes,
        media_type=mime_type,
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"'
        },
    )


@router.post("/chunks")
async def chunks_endpoint(request_body: Annotated[dict, Body(...)]) -> ChunkResponse:
    """Create meaningful chunks from extracted JSON.

    Supports DOCX, PDF, Markdown, TXT, and PPTX extracted JSON.
    Accepts the same payload shape as `/generate`, including direct paste of the
    full `/extract` response.
    """
    return create_chunks(request_body)
