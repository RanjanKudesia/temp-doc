"""PPT edit service wrapper."""

from __future__ import annotations

from typing import Any

from ....schemas.temp_doc_schema import PptEditRequest, PptEditResponse
from .ppt_patch_service import edit_ppt_json


def edit_pptx_json(request_data: PptEditRequest | dict[str, Any]) -> PptEditResponse:
    """Apply edit instructions for PPTX payloads."""
    return edit_ppt_json(request_data)
