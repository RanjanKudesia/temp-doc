"""Generation adapters for temp-doc service."""

from typing import Literal

from app.helper.generate.pipelines import (
    DocxGenerationPipeline,
    PdfGenerationPipeline,
    PptGenerationPipeline,
    HtmlGenerationPipeline,
    MarkdownGenerationPipeline,
    TextGenerationPipeline,
)
from ...schemas.temp_doc_schema import ExtractedData, ExtractedPptData


class GenerationAdapterFactory:
    """Factory for getting the appropriate generation pipeline."""

    def __init__(
        self,
        docx_pipeline: DocxGenerationPipeline,
        pdf_pipeline: PdfGenerationPipeline,
        pptx_pipeline: PptGenerationPipeline,
        html_pipeline: HtmlGenerationPipeline,
        markdown_pipeline: MarkdownGenerationPipeline,
        text_pipeline: TextGenerationPipeline,
    ):
        self.pipelines = {
            "docx": docx_pipeline,
            "pdf": pdf_pipeline,
            "pptx": pptx_pipeline,
            "html": html_pipeline,
            "markdown": markdown_pipeline,
            "text": text_pipeline,
        }

    def generate(
        self,
        output_format: Literal["docx", "pdf", "pptx", "html", "markdown", "text"],
        extracted_data: ExtractedData | ExtractedPptData,
        title: str | None = None,
    ) -> bytes:
        """Generate document in specified format."""
        pipeline = self.pipelines.get(output_format)
        if not pipeline:
            raise ValueError(f"Unsupported output format: {output_format}")

        return pipeline.run(extracted_data, title)
