"""Extraction engine for the chunking service.

Imports pipeline classes DIRECTLY from their module files (not via
helper.extract service API) and dispatches extraction based on file extension.
Each pipeline is a stateless class; one instance per process is reused.
"""

from __future__ import annotations

import logging
from typing import Any

from app.schemas.temp_doc_schema import ExtractedData, ExtractedPptData

# Import pipeline classes directly — standalone parsers, not service APIs
from app.helper.chunking.pipelines.docx_extraction_pipeline import (
    DocxExtractionPipeline,
)
from app.helper.chunking.pipelines.pdf_extraction_pipeline import (
    PdfExtractionPipeline,
)
from app.helper.chunking.pipelines.ppt_extraction_pipeline import (
    PptExtractionPipeline,
)
from app.helper.chunking.pipelines.html_extraction_pipeline import (
    HtmlExtractionPipeline,
)
from app.helper.chunking.pipelines.markdown_extraction_pipeline import (
    MarkdownExtractionPipeline,
)
from app.helper.chunking.pipelines.text_extraction_pipeline import (
    TextExtractionPipeline,
)

logger = logging.getLogger(__name__)

# Stateless pipeline instances — shared per process
_docx = DocxExtractionPipeline()
_pdf = PdfExtractionPipeline()
_ppt = PptExtractionPipeline()
_html = HtmlExtractionPipeline()
_markdown = MarkdownExtractionPipeline()
_text = TextExtractionPipeline()


def extract_bytes(
    file_bytes: bytes,
    extension: str,
) -> tuple[ExtractedData | ExtractedPptData, str]:
    """Extract raw bytes using the appropriate pipeline.

    Args:
        file_bytes: Raw file content.
        extension:  Lowercase file extension without dot (e.g. "docx").

    Returns:
        (validated_extracted_data, normalized_extension)

    Raises:
        ValueError: If the file is invalid or the extension is unsupported.
    """
    ext = extension.lower().lstrip(".")

    if ext == "docx":
        raw: dict[str, Any] = _docx.run(file_bytes)
        return ExtractedData.model_validate(raw), "docx"

    if ext == "pdf":
        raw = _pdf.run(file_bytes)
        return ExtractedData.model_validate(raw), "pdf"

    if ext in ("ppt", "pptx"):
        raw = _ppt.run(file_bytes)
        return ExtractedPptData.model_validate(raw), "pptx"

    if ext in ("html", "htm"):
        raw = _html.run(file_bytes)
        return ExtractedData.model_validate(raw), "html"

    if ext in ("md", "markdown"):
        raw = _markdown.run(file_bytes)
        return ExtractedData.model_validate(raw), "markdown"

    if ext == "txt":
        raw = _text.run(file_bytes)
        return ExtractedData.model_validate(raw), "text"

    raise ValueError(f"Unsupported extension: .{ext}")
