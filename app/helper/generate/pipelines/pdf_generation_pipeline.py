"""PDF generation pipeline for temp-doc service."""

import logging
from io import BytesIO

from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.lib import colors

from ...schemas.temp_doc_schema import ExtractedData


class PdfGenerationPipeline:
    """Generate PDF from extracted data."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, extracted_data: ExtractedData, title: str | None = None) -> bytes:
        """Generate PDF from extracted data."""
        try:
            output = BytesIO()
            doc = SimpleDocTemplate(output, pagesize=letter)
            story: list = []
            styles = getSampleStyleSheet()

            if title:
                self._append_title(story, styles, title)

            self._append_content(story, styles, extracted_data)

            doc.build(story)
            return output.getvalue()

        except Exception as e:
            self.logger.error("PDF generation failed: %s", e)
            raise

    def _append_title(self, story: list, styles, title: str) -> None:
        """Append document title block to story."""
        story.append(Paragraph(title, styles["Heading1"]))
        story.append(Spacer(1, 0.3 * inch))

    def _append_content(self, story: list, styles, extracted_data: ExtractedData) -> None:
        """Append paragraphs and tables according to document order."""
        paragraph_by_index = {p.index: p for p in extracted_data.paragraphs}
        table_by_index = {t.index: t for t in extracted_data.tables}

        for order_item in extracted_data.document_order:
            if order_item.type == "paragraph":
                paragraph = paragraph_by_index.get(order_item.index)
                self._append_paragraph(story, styles, paragraph)
            elif order_item.type == "table":
                table = table_by_index.get(order_item.index)
                self._append_table(story, table)

    def _append_paragraph(self, story: list, styles, paragraph) -> None:
        """Append a paragraph if it has non-empty text."""
        if paragraph is None or not paragraph.text:
            return

        style = self._resolve_paragraph_style(styles, paragraph.style)
        story.append(Paragraph(paragraph.text, style))
        story.append(Spacer(1, 0.1 * inch))

    def _resolve_paragraph_style(self, styles, paragraph_style: str | None):
        """Resolve reportlab style for a paragraph."""
        style = styles.get("Normal")
        if not paragraph_style or "h" not in paragraph_style.lower():
            return style

        heading_num = paragraph_style[-1] if paragraph_style[-1].isdigit() else "1"
        style_key = f"Heading{heading_num}"
        return styles.get(style_key, styles["Normal"])

    def _append_table(self, story: list, table) -> None:
        """Append a styled table block."""
        if table is None or not table.rows:
            return

        table_data = [[cell.text or "" for cell in row.cells]
                      for row in table.rows]
        if not table_data:
            return

        doc_table = Table(table_data)
        doc_table.setStyle(self._default_table_style())
        story.append(doc_table)
        story.append(Spacer(1, 0.2 * inch))

    def _default_table_style(self) -> TableStyle:
        """Return default style used for generated PDF tables."""
        return TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 12),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
            ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
            ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ])
