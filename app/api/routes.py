"""API routes for temp-doc service."""

import logging
from typing import Annotated

from fastapi import APIRouter, UploadFile, Body, Query, Response

from ..helper.extract import extract_document
from ..helper.generate import generate_document
from ..helper.chunks import create_chunks
from ..helper.edit import edit_document
from ..helper.chunking import chunk_document
from ..schemas.temp_doc_schema import (
    ChunkResponse,
    ChunkingResponse,
    EditResponse,
    ExtractResponse,
    PptEditResponse,
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


@router.post("/edit")
async def edit_endpoint(
    request_body: Annotated[
        dict,
        Body(
            openapi_examples={
                "pasteExtractedJsonAndInstructions": {
                    "summary": "Paste extracted_data and instructions",
                    "description": "Only two params are needed: extracted_data and instructions.",
                    "value": {
                        "extracted_data": {
                            "extension": "docx",
                            "output_format": "json",
                            "extracted_data": {
                                "document_order": [],
                                "document_defaults": None,
                                "styles": [],
                                "paragraphs": [],
                                "tables": [],
                                "media": [],
                            },
                        },
                        "instructions": [
                            {
                                "op": "replace",
                                "path": "/paragraphs/0/text",
                                "value": "Updated text",
                            }
                        ],
                    },
                },
                "advancedParagraphOps": {
                    "summary": "Advanced paragraph operations",
                    "description": "Examples of supported high-level edit operations.",
                    "value": {
                        "extracted_data": {
                            "extension": "docx",
                            "output_format": "json",
                            "extracted_data": {
                                "document_order": [],
                                "document_defaults": None,
                                "styles": [],
                                "paragraphs": [],
                                "tables": [],
                                "media": [],
                            },
                        },
                        "instructions": [
                            {
                                "op": "replace_text",
                                "path": "/paragraphs/0/text",
                                "old_value": "Old",
                                "new_value": "New",
                                "count": 1,
                            },
                            {
                                "op": "insert_paragraph_after",
                                "index": 0,
                                "value": "Inserted paragraph after paragraph 0"
                            },
                            {
                                "op": "remove_empty_paragraphs"
                            }
                        ],
                    },
                },
                "tableAndListOps": {
                    "summary": "DOCX table and list operations",
                    "description": "Examples covering list edits, table row/column edits, and nested collections.",
                    "value": {
                        "extracted_data": {
                            "extension": "docx",
                            "output_format": "json",
                            "extracted_data": {
                                "document_order": [],
                                "document_defaults": None,
                                "styles": [],
                                "paragraphs": [],
                                "tables": [],
                                "media": [],
                            },
                        },
                        "instructions": [
                            {
                                "op": "insert_paragraph_after",
                                "path": "/paragraphs",
                                "index": 4,
                                "value": {
                                    "text": "New bullet item",
                                    "style": "List Paragraph",
                                    "is_bullet": True,
                                    "is_numbered": False,
                                    "list_level": 1
                                }
                            },
                            {
                                "op": "insert_table_row",
                                "path": "/tables/0",
                                "index": 0,
                                "value": {
                                    "cells": [
                                        {"text": "Header A"},
                                        {"text": "Header B"}
                                    ]
                                }
                            },
                            {
                                "op": "insert_table_column",
                                "path": "/tables/0",
                                "index": 0,
                                "value": {"text": "Notes"}
                            }
                        ]
                    }
                }
            }
        ),
    ]
) -> EditResponse | PptEditResponse:
    """Apply patch instructions to extracted JSON using a single endpoint.

    - Request body has only two fields: `extracted_data` and `instructions`.
    - `extracted_data` accepts either raw extracted JSON or full `/extract` response.
    - Supported extensions: DOCX, HTML, Markdown, TXT, and PPTX.
    - **instructions** uses JSON-Pointer style paths.

    Supported operations:
    - add: create/update value at path (supports list append with /-)
    - replace: replace existing value at path
    - remove: remove existing value at path
    - replace_text: replace substring inside a string field
    - insert_paragraph_after: insert a paragraph in top-level or nested paragraph collections
    - remove_paragraph: remove a paragraph from top-level or nested paragraph collections
    - remove_empty_paragraphs: remove blank paragraphs from top-level or nested paragraph collections
    - insert_table_after: insert a table in top-level or nested table collections
    - remove_table: remove a table from top-level or nested table collections
    - insert_table_row: insert a row into an existing table
    - remove_table_row: remove a row from an existing table
    - insert_table_column: insert a column into an existing table
    - remove_table_column: remove a column from an existing table
    """
    return edit_document(request_body)


@router.post("/chunking")
async def chunking_endpoint(
    file: UploadFile,
    include_media: Annotated[
        bool,
        Query(description="Include media during extraction (default False for speed)."),
    ] = False,
) -> ChunkingResponse:
    """Extract a document and return text chunks in one step.

    - **file**: Document file (DOCX, PDF, PPTX, HTML, MD, TXT)
    - **include_media**: Include media objects during extraction (default False)

    Extracts the file content, then produces meaningful text chunks.
    Does not require a separate /extract call first.
    """
    return await chunk_document(file, include_media=include_media)
