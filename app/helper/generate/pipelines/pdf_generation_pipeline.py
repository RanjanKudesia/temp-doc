"""PDF generation pipeline for temp-doc service."""

import base64
import logging
from io import BytesIO
from typing import Any
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

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
            rendered_media_indices: set[int] = set()

            if title:
                self._append_title(story, styles, title)

            self._append_content(
                story,
                styles,
                extracted_data,
                rendered_media_indices,
            )

            # Ensure media are not dropped even if no explicit media order items exist.
            self._append_unordered_media(
                story,
                extracted_data,
                rendered_media_indices,
            )

            doc.build(story)
            return output.getvalue()

        except Exception as e:
            self.logger.error("PDF generation failed: %s", e)
            raise

    def _append_title(self, story: list, styles, title: str) -> None:
        """Append document title block to story."""
        story.append(Paragraph(title, styles["Heading1"]))
        story.append(Spacer(1, 0.3 * inch))

    def _append_content(
        self,
        story: list,
        styles,
        extracted_data: ExtractedData,
        rendered_media_indices: set[int],
    ) -> None:
        """Append paragraphs and tables according to document order."""
        paragraph_by_index = {p.index: p for p in extracted_data.paragraphs}
        table_by_index = {t.index: t for t in extracted_data.tables}
        media_by_index = dict(enumerate(extracted_data.media))
        current_page_index: int | None = None

        for order_item in extracted_data.document_order:
            if order_item.type == "paragraph":
                paragraph = paragraph_by_index.get(order_item.index)
                current_page_index = self._append_page_break_if_needed(
                    story,
                    current_page_index,
                    self._coerce_page_index(paragraph),
                )
                self._append_paragraph(story, styles, paragraph)
            elif order_item.type == "table":
                table = table_by_index.get(order_item.index)
                current_page_index = self._append_page_break_if_needed(
                    story,
                    current_page_index,
                    self._coerce_page_index(table),
                )
                self._append_table(story, table)
            elif order_item.type == "media":
                media = media_by_index.get(order_item.index)
                current_page_index = self._append_page_break_if_needed(
                    story,
                    current_page_index,
                    self._coerce_page_index(media),
                )
                if media is not None and self._append_media(story, media):
                    rendered_media_indices.add(order_item.index)

    def _append_page_break_if_needed(
        self,
        story: list,
        current_page_index: int | None,
        next_page_index: int | None,
    ) -> int | None:
        """Insert page break when source page index advances."""
        if next_page_index is None:
            return current_page_index
        if current_page_index is None:
            return next_page_index
        if next_page_index > current_page_index:
            story.append(PageBreak())
            return next_page_index
        return current_page_index

    def _coerce_page_index(self, item: Any) -> int | None:
        """Safely parse page_index from extracted items."""
        if item is None:
            return None
        value = getattr(item, "page_index", None)
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _append_paragraph(self, story: list, styles, paragraph) -> None:
        """Append a paragraph if it has non-empty text."""
        if paragraph is None:
            return

        paragraph_markup = self._build_paragraph_markup(paragraph)
        if not paragraph_markup:
            return

        style = self._resolve_paragraph_style(styles, paragraph)
        story.append(Paragraph(paragraph_markup, style))
        story.append(Spacer(1, 0.1 * inch))

    def _resolve_paragraph_style(self, styles, paragraph):
        """Resolve reportlab style for a paragraph."""
        paragraph_style = (getattr(paragraph, "style", None) or "").strip()
        style = styles["Normal"]

        if paragraph_style:
            normalized = paragraph_style.lower().replace("_", " ")
            if normalized.startswith("heading"):
                tokens = normalized.split()
                heading_num = tokens[-1] if tokens and tokens[-1].isdigit() else "1"
                style = styles.get(f"Heading{heading_num}", styles["Normal"])

        list_level = getattr(paragraph, "list_level", None) or 0
        left_indent = max(0, int(list_level)) * 0.25 * inch
        if left_indent:
            return ParagraphStyle(
                f"{style.name}_list_{list_level}",
                parent=style,
                leftIndent=style.leftIndent + left_indent,
            )
        return style

    def _build_paragraph_markup(self, paragraph) -> str:
        """Build ReportLab paragraph markup from runs and list metadata."""
        paragraph_text = str(getattr(paragraph, "text", "") or "")
        runs = list(getattr(paragraph, "runs", []) or [])
        if runs:
            rendered = "".join(self._render_run_markup(run) for run in runs)
            source_from_runs = "".join(
                str(getattr(run, "text", "") or "") for run in runs
            )
            if self._normalized_text(source_from_runs) != self._normalized_text(paragraph_text):
                rendered = escape(paragraph_text)
        else:
            rendered = escape(paragraph_text)

        rendered = rendered.strip()

        if not rendered:
            return ""

        prefix = ""
        if bool(getattr(paragraph, "is_bullet", False)):
            prefix = "&#8226; "
        elif bool(getattr(paragraph, "is_numbered", False)):
            numbering_format = str(
                getattr(paragraph, "numbering_format", "") or "1."
            ).split(":")[-1]
            prefix = f"{escape(numbering_format)} "

        return f"{prefix}{rendered}"

    def _render_run_markup(self, run) -> str:
        """Render a single text run into ReportLab-compatible XML markup."""
        text = escape(str(getattr(run, "text", "") or ""))
        if not text:
            return ""

        if bool(getattr(run, "bold", False)):
            text = f"<b>{text}</b>"
        if bool(getattr(run, "italic", False)):
            text = f"<i>{text}</i>"

        font_name = self._normalize_font_name(getattr(run, "font_name", None))
        font_size_pt = getattr(run, "font_size_pt", None)
        color_rgb = self._normalize_color(getattr(run, "color_rgb", None))

        font_attrs: list[str] = []
        if font_name:
            font_attrs.append(f'name="{escape(str(font_name))}"')
        if font_size_pt:
            font_attrs.append(f'size="{int(round(float(font_size_pt)))}"')
        if color_rgb:
            font_attrs.append(f'color="#{color_rgb}"')

        if font_attrs:
            text = f"<font {' '.join(font_attrs)}>{text}</font>"
        return text

    def _normalized_text(self, text: str) -> str:
        """Normalize text for robust equivalence checks."""
        return " ".join(text.split())

    def _normalize_font_name(self, font_name: Any) -> str | None:
        """Map arbitrary source font names to ReportLab-safe names."""
        if not font_name:
            return None

        raw = str(font_name).strip()
        if not raw:
            return None

        registered = set(pdfmetrics.getRegisteredFontNames())
        if raw in registered:
            return raw

        normalized = raw.replace("-", " ").replace("_", " ").lower()
        if "courier" in normalized or "mono" in normalized:
            return "Courier"
        if "times" in normalized or "serif" in normalized:
            return "Times-Roman"
        if "helvetica" in normalized or "arial" in normalized or "inter" in normalized:
            return "Helvetica"

        # Unknown fonts are ignored to avoid ReportLab family mapping errors.
        return None

    def _normalize_color(self, color_value: Any) -> str | None:
        """Normalize RGB-like values to 6-char uppercase hex."""
        if color_value is None:
            return None
        value = str(color_value).strip().lstrip("#")
        if len(value) == 3 and all(c in "0123456789abcdefABCDEF" for c in value):
            return "".join(ch * 2 for ch in value).upper()
        if len(value) == 6 and all(c in "0123456789abcdefABCDEF" for c in value):
            return value.upper()
        return None

    def _append_table(self, story: list, table) -> None:
        """Append a styled table block."""
        if table is None or not table.rows:
            return

        table_data = [[cell.text or "" for cell in row.cells]
                      for row in table.rows]
        if not table_data:
            return

        doc_table = Table(table_data)
        has_header = bool(
            table.rows and any(bool(getattr(cell, "is_header", False))
                               for cell in table.rows[0].cells)
        )
        doc_table.setStyle(self._default_table_style(has_header=has_header))
        story.append(doc_table)
        story.append(Spacer(1, 0.2 * inch))

    def _default_table_style(self, has_header: bool = False) -> TableStyle:
        """Return default style used for generated PDF tables."""
        commands: list[tuple[Any, ...]] = [
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]
        if has_header:
            commands.extend([
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ])
        return TableStyle(commands)

    def _append_media(self, story: list, media_item) -> bool:
        """Append a media image flowable when payload contains valid base64."""
        base64_payload = (
            getattr(media_item, "base64", None)
            or getattr(media_item, "base64_data", None)
        )
        if not base64_payload:
            return False

        try:
            image_bytes = base64.b64decode(str(base64_payload), validate=True)
        except (ValueError, TypeError):
            return False

        if not image_bytes:
            return False

        max_width = 6.5 * inch
        max_height = 8.5 * inch
        img = Image(BytesIO(image_bytes))
        self._fit_image(img, max_width=max_width, max_height=max_height)
        story.append(img)
        story.append(Spacer(1, 0.15 * inch))
        return True

    def _fit_image(self, img: Image, max_width: float, max_height: float) -> None:
        """Scale image dimensions to fit within max bounds while preserving ratio."""
        width = float(getattr(img, "imageWidth", 0) or 0)
        height = float(getattr(img, "imageHeight", 0) or 0)
        if width <= 0 or height <= 0:
            return

        scale = min(max_width / width, max_height / height, 1.0)
        img.drawWidth = width * scale
        img.drawHeight = height * scale

    def _append_unordered_media(
        self,
        story: list,
        extracted_data: ExtractedData,
        rendered_media_indices: set[int],
    ) -> None:
        """Append media not referenced in document_order to avoid data loss."""
        for idx, media_item in enumerate(extracted_data.media):
            if idx in rendered_media_indices:
                continue
            self._append_media(story, media_item)
