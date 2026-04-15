"""Extraction adapters re-exported for easy discovery."""

from .extraction_adapters import (
    DocxJsonExtractionAdapter,
    HtmlJsonExtractionAdapter,
    MarkdownJsonExtractionAdapter,
    TextJsonExtractionAdapter,
    PptJsonExtractionAdapter,
)

__all__ = [
    "DocxJsonExtractionAdapter",
    "HtmlJsonExtractionAdapter",
    "MarkdownJsonExtractionAdapter",
    "TextJsonExtractionAdapter",
    "PptJsonExtractionAdapter",
]
