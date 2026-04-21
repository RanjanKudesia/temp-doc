"""Common edit entry point with per-extension service dispatch."""

from __future__ import annotations

from fastapi import HTTPException, status

from ...schemas.temp_doc_schema import (
    EditRequest,
    EditResponse,
    PptEditRequest,
    PptEditResponse,
)
from .docx import edit_docx_json
from .html import edit_html_json
from .markdown import edit_markdown_json
from .pptx import edit_pptx_json
from .text import edit_text_json


def edit_document(request_body: dict) -> EditResponse | PptEditResponse:
    """Dispatch /edit payload to the extension-specific edit service."""
    extension = detect_edit_extension(request_body)

    if extension in {"ppt", "pptx"}:
        return edit_pptx_json(PptEditRequest.model_validate(request_body))

    if extension in {"html", "htm"}:
        return edit_html_json(EditRequest.model_validate(request_body))

    if extension in {"md", "markdown"}:
        return edit_markdown_json(EditRequest.model_validate(request_body))

    if extension in {"txt", "text"}:
        return edit_text_json(EditRequest.model_validate(request_body))

    if extension in {"doc", "docx", ""}:
        return edit_docx_json(EditRequest.model_validate(request_body))

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
            "Unsupported extension for /edit: "
            f"{extension}. Supported extensions: docx, html, md, txt, pptx"
        ),
    )


def detect_edit_extension(request_body: dict) -> str:
    """Best-effort extension detection from edit payload."""
    top_level_extension = request_body.get("extension")
    if isinstance(top_level_extension, str):
        return top_level_extension.strip().lower()

    extracted_payload = request_body.get("extracted_data")
    if not isinstance(extracted_payload, dict):
        return ""

    direct_extension = _detect_extension_from_dict(extracted_payload)
    if direct_extension:
        return direct_extension

    nested = extracted_payload.get("extracted_data")
    if isinstance(nested, dict):
        nested_extension = _detect_extension_from_dict(nested)
        if nested_extension:
            return nested_extension

    return ""


def _detect_extension_from_dict(payload: dict) -> str:
    """Extract extension hint from one payload dictionary."""
    for key in ("extension", "document_type"):
        value = payload.get(key)
        if isinstance(value, str):
            return value.strip().lower()

    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        source_type = metadata.get("source_type")
        if isinstance(source_type, str):
            return source_type.strip().lower()

    return ""


__all__ = [
    "edit_document",
    "detect_edit_extension",
    "edit_docx_json",
    "edit_html_json",
    "edit_markdown_json",
    "edit_text_json",
    "edit_pptx_json",
]
