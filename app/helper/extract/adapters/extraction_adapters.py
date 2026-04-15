"""Extraction adapters for temp-doc service - no storage, media ignored."""

from typing import Any

from app.helper.extract.pipelines import (
    DocxExtractionPipeline,
    HtmlExtractionPipeline,
    MarkdownExtractionPipeline,
    TextExtractionPipeline,
    PptExtractionPipeline,
)


class DocxJsonExtractionAdapter:
    """Extract DOCX files to JSON format."""

    def __init__(self, pipeline: DocxExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes) -> dict[str, Any]:
        """Extract DOCX and return JSON data."""
        return self.pipeline.run(file_bytes=file_bytes)


class HtmlJsonExtractionAdapter:
    """Extract HTML files to JSON format."""

    def __init__(self, pipeline: HtmlExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes) -> dict[str, Any]:
        """Extract HTML and return JSON data."""
        return self.pipeline.run(file_bytes=file_bytes)


class MarkdownJsonExtractionAdapter:
    """Extract Markdown files to JSON format."""

    def __init__(self, pipeline: MarkdownExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes) -> dict[str, Any]:
        """Extract Markdown and return JSON data."""
        return self.pipeline.run(file_bytes=file_bytes)


class TextJsonExtractionAdapter:
    """Extract Text files to JSON format."""

    def __init__(self, pipeline: TextExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes) -> dict[str, Any]:
        """Extract Text and return JSON data."""
        return self.pipeline.run(file_bytes=file_bytes)


class PptJsonExtractionAdapter:
    """Extract PPTX files to JSON format."""

    def __init__(self, pipeline: PptExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes) -> dict[str, Any]:
        """Extract PPTX and return JSON data."""
        return self.pipeline.run(file_bytes=file_bytes)
