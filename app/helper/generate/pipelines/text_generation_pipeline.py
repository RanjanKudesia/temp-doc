"""Text generation pipeline for temp-doc service."""
import logging
from ...schemas.temp_doc_schema import ExtractedData


class TextGenerationPipeline:
    """Generate plain text from extracted data."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, extracted_data: ExtractedData, title: str | None = None) -> bytes:
        """Generate plain text from extracted data."""
        try:
            lines = self._build_lines(extracted_data, title)
            content = "\n".join(lines).rstrip() + "\n"
            return content.encode("utf-8")
        except Exception as e:
            self.logger.error("Text generation failed: %s", str(e))
            raise

    def _build_lines(self, data: ExtractedData, title: str | None) -> list[str]:
        paragraph_by_index = {p.index: p for p in data.paragraphs}
        table_by_index = {t.index: t for t in data.tables}
        lines: list[str] = []

        if title:
            lines.append(title)
            lines.append("")

        if data.document_order:
            self._append_lines_from_order(
                data,
                paragraph_by_index,
                table_by_index,
                lines,
            )
            return lines

        self._append_lines_from_sorted(data, lines)

        return lines

    def _append_lines_from_order(
        self,
        data: ExtractedData,
        paragraph_by_index: dict,
        table_by_index: dict,
        lines: list[str],
    ) -> None:
        """Append lines using explicit document order."""
        for item in data.document_order:
            if item.type == "paragraph":
                paragraph = paragraph_by_index.get(item.index)
                self._append_paragraph_line(paragraph, lines)
            elif item.type == "table":
                table = table_by_index.get(item.index)
                self._append_table_lines(table, lines)

    def _append_lines_from_sorted(self, data: ExtractedData, lines: list[str]) -> None:
        """Append lines using sorted paragraphs/tables by index."""
        for paragraph in sorted(data.paragraphs, key=lambda item: item.index):
            self._append_paragraph_line(paragraph, lines)
        for table in sorted(data.tables, key=lambda item: item.index):
            self._append_table_lines(table, lines)

    def _append_paragraph_line(self, paragraph, lines: list[str]) -> None:
        """Append a single paragraph and blank separator line."""
        if paragraph is None:
            return
        lines.append((paragraph.text or "").strip())
        lines.append("")

    def _append_table_lines(self, table, lines: list[str]) -> None:
        """Append a table as text rows and blank separator line."""
        if table is None:
            return
        rows = [[(cell.text or "") for cell in row.cells]
                for row in table.rows]
        lines.extend(self._table_to_text(rows))
        lines.append("")

    def _table_to_text(self, rows: list[list[str]]) -> list[str]:
        if not rows:
            return []
        return [" | ".join(cell.strip() for cell in row) for row in rows]
