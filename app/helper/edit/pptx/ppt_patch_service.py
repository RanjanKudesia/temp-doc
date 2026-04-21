"""Apply patch instructions to extracted PPT JSON payloads."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, status

from ...schemas.temp_doc_schema import (
    ExtractResponse,
    ExtractedPptData,
    PptEditRequest,
    PptEditResponse,
)

from .ppt_context import _PptContext
from .ppt_specific_ops import apply_ppt_instruction

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def edit_ppt_json(
    request_data: PptEditRequest | dict[str, Any],
) -> PptEditResponse:
    """Apply instructions to PPT extracted_data and return updated payload."""
    if isinstance(request_data, dict):
        try:
            request_data = PptEditRequest.model_validate(request_data)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid PPT edit payload: {str(exc)}",
            ) from exc

    source_data = _coerce_ppt_extracted_data(request_data.extracted_data)
    document = source_data.model_dump()

    ctx = _PptContext(document)

    try:
        for index, instruction in enumerate(request_data.instructions):
            apply_ppt_instruction(
                document, instruction.model_dump(), index, ctx)

        validated = ExtractedPptData.model_validate(document)
        return PptEditResponse(
            extension="pptx",
            output_format="json",
            extracted_data=validated,
            applied_instructions=len(request_data.instructions),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("PPT edit operation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to apply PPT edit instructions: {str(exc)}",
        ) from exc


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _coerce_ppt_extracted_data(
    payload: ExtractedPptData | ExtractResponse,
) -> ExtractedPptData:
    """Accept raw ExtractedPptData or a full /extract response."""
    if isinstance(payload, ExtractedPptData):
        return payload

    if payload.extension not in ("pptx", "ppt"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only PPTX extract payloads are supported for /edit/ppt.",
        )
    if payload.output_format != "json":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="/edit/ppt supports JSON extract payloads only.",
        )
    if not isinstance(payload.extracted_data, ExtractedPptData):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="/edit/ppt currently supports pptx extracted_data payloads only.",
        )
    return payload.extracted_data
