"""PDF conversion pipeline for temp-doc service."""

import logging
import tempfile
from pathlib import Path
from typing import Any

from pdf2docx import Converter

from app.helper.extract.pipelines.docx_extraction_pipeline import DocxExtractionPipeline


class PdfConversionPipeline:
    """Convert PDF to DOCX and then extract."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.docx_pipeline = DocxExtractionPipeline()

    def run(self, file_bytes: bytes) -> dict[str, Any]:
        """Convert PDF to DOCX and extract."""
        try:
            # pdf2docx expects file paths, so use a temp workspace.
            with tempfile.TemporaryDirectory(prefix="temp-doc-") as temp_dir:
                temp_path = Path(temp_dir)
                pdf_path = temp_path / "input.pdf"
                docx_path = temp_path / "output.docx"

                pdf_path.write_bytes(file_bytes)

                converter = Converter(str(pdf_path))
                try:
                    converter.convert(str(docx_path))
                finally:
                    converter.close()

                if not docx_path.exists() or not docx_path.is_file():
                    raise ValueError(
                        "PDF conversion failed: DOCX output was not created.")

                docx_bytes = docx_path.read_bytes()

            # Extract from DOCX
            return self.docx_pipeline.run(docx_bytes)

        except Exception as e:
            self.logger.error("PDF conversion failed: %s", e)
            raise ValueError(f"Failed to convert PDF: {str(e)}") from e
