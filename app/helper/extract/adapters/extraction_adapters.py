"""Extraction adapters for temp-doc service."""

from typing import Any

from ..pipelines import (
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

    def run(self, file_bytes: bytes, include_media: bool = True) -> dict[str, Any]:
        """Extract DOCX and return JSON data."""
        return self.pipeline.run(file_bytes=file_bytes, include_media=include_media)


class HtmlJsonExtractionAdapter:
    """Extract HTML files to JSON format."""

    def __init__(self, pipeline: HtmlExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes, include_media: bool = True) -> dict[str, Any]:
        """Extract HTML and return JSON data."""
        return self.pipeline.run(file_bytes=file_bytes, include_media=include_media)


class MarkdownJsonExtractionAdapter:
    """Extract Markdown files to JSON format."""

    def __init__(self, pipeline: MarkdownExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes, include_media: bool = True) -> dict[str, Any]:
        """Extract Markdown and return JSON data."""
        return self.pipeline.run(file_bytes=file_bytes, include_media=include_media)


class TextJsonExtractionAdapter:
    """Extract Text files to JSON format."""

    def __init__(self, pipeline: TextExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes, include_media: bool = True) -> dict[str, Any]:
        """Extract Text and return JSON data."""
        return self.pipeline.run(file_bytes=file_bytes, include_media=include_media)


class PptJsonExtractionAdapter:
    """Extract PPTX files to JSON format."""

    def __init__(self, pipeline: PptExtractionPipeline) -> None:
        self.pipeline = pipeline

    def run(self, file_bytes: bytes, include_media: bool = True) -> dict[str, Any]:
        """Extract PPTX and return JSON data."""
        return self.pipeline.run(file_bytes=file_bytes, include_media=include_media)
