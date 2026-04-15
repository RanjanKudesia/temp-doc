"""PDF generation pipeline for temp-doc service."""

import logging
from io import BytesIO
from typing import Any

from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors

from app.schemas.temp_doc_schema import ExtractedData


class PdfGenerationPipeline:
    """Generate PDF from extracted data."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, extracted_data: ExtractedData, title: str | None = None) -> bytes:
        """Generate PDF from extracted data."""
        try:
            output = BytesIO()
            doc = SimpleDocTemplate(output, pagesize=letter)
            story = []
            styles = getSampleStyleSheet()

            # Add title
            if title:
                story.append(Paragraph(title, styles["Heading1"]))
                story.append(Spacer(1, 0.3 * inch))

            # Add content
            for order_item in extracted_data.document_order:
                if order_item.type == "paragraph":
                    para = next(
                        (p for p in extracted_data.paragraphs if p.index == order_item.index), None)
                    if para and para.text:
                        # Use appropriate style based on paragraph style
                        style = styles.get("Normal")
                        if para.style and "h" in para.style.lower():
                            heading_num = para.style[-1] if para.style[-1].isdigit() else "1"
                            style_key = f"Heading{heading_num}"
                            style = styles.get(style_key, styles["Normal"])

                        story.append(Paragraph(para.text, style))
                        story.append(Spacer(1, 0.1 * inch))

                elif order_item.type == "table":
                    table = next(
                        (t for t in extracted_data.tables if t.index == order_item.index), None)
                    if table and table.rows:
                        table_data = []
                        for row in table.rows:
                            table_data.append(
                                [cell.text or "" for cell in row.cells])

                        if table_data:
                            doc_table = Table(table_data)
                            doc_table.setStyle(TableStyle([
                                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                                ("FONTSIZE", (0, 0), (-1, 0), 12),
                                ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                                ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                                ("GRID", (0, 0), (-1, -1), 1, colors.black),
                            ]))
                            story.append(doc_table)
                            story.append(Spacer(1, 0.2 * inch))

            doc.build(story)
            return output.getvalue()

        except Exception as e:
            self.logger.error(f"PDF generation failed: {str(e)}")
            raise
