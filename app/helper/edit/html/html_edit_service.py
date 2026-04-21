"""HTML edit service wrapper."""

from __future__ import annotations

from typing import Any

from ....schemas.temp_doc_schema import EditRequest, EditResponse
from ..shared.json_patch_service import edit_extracted_json


def edit_html_json(request_data: EditRequest | dict[str, Any]) -> EditResponse:
    """Apply edit instructions for HTML payloads."""
    return edit_extracted_json(request_data, extension_hint="html")
