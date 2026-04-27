"""Extraction engine for the chunking service.

Imports pipeline classes DIRECTLY from their module files (not via
helper.extract service API) and dispatches extraction based on file extension.
Each pipeline is a stateless class; one instance per process is reused.
"""

from __future__ import annotations

import logging
import time
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
    logger.info(
        "[extract_engine] Starting extraction | ext=.%s | file_size=%d bytes", ext, len(file_bytes))
    t0 = time.perf_counter()

    if ext == "docx":
        logger.info("[extract_engine] Running DOCX pipeline ...")
        raw: dict[str, Any] = _docx.run(file_bytes)
        raw["media"] = []
        result = ExtractedData.model_validate(raw), "docx"

    elif ext == "pdf":
        logger.info("[extract_engine] Running PDF pipeline ...")
        raw = _pdf.run(file_bytes, include_media=False)
        raw["media"] = []
        result = ExtractedData.model_validate(raw), "pdf"

    elif ext in ("ppt", "pptx"):
        logger.info("[extract_engine] Running PPT pipeline ...")
        raw = _ppt.run(file_bytes, include_media=False)
        raw["media"] = []
        result = ExtractedPptData.model_validate(raw), "pptx"

    elif ext in ("html", "htm"):
        logger.info("[extract_engine] Running HTML pipeline ...")
        raw = _html.run(file_bytes, include_media=False)
        raw["media"] = []
        result = ExtractedData.model_validate(raw), "html"

    elif ext in ("md", "markdown"):
        logger.info("[extract_engine] Running Markdown pipeline ...")
        raw = _markdown.run(file_bytes, include_media=False)
        raw["media"] = []
        result = ExtractedData.model_validate(raw), "markdown"

    elif ext == "txt":
        logger.info("[extract_engine] Running TXT pipeline ...")
        raw = _text.run(file_bytes, include_media=False)
        raw["media"] = []
        result = ExtractedData.model_validate(raw), "text"

    else:
        raise ValueError(f"Unsupported extension: .{ext}")

    elapsed_ms = round((time.perf_counter() - t0) * 1000)
    extracted_data, norm_ext = result
    para_count = len(getattr(extracted_data, "paragraphs", []))
    table_count = len(getattr(extracted_data, "tables", []))
    logger.info(
        "[extract_engine] Extraction done | ext=.%s | elapsed=%dms | paragraphs=%d | tables=%d",
        norm_ext, elapsed_ms, para_count, table_count,
    )
    return result
