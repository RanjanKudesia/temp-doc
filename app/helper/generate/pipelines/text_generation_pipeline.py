"""Text generation pipeline for temp-doc service."""
import logging
from app.schemas.temp_doc_schema import ExtractedData


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
            for item in data.document_order:
                if item.type == "paragraph":
                    p = paragraph_by_index.get(item.index)
                    if p is not None:
                        lines.append((p.text or "").strip())
                        lines.append("")
                elif item.type == "table":
                    t = table_by_index.get(item.index)
                    if t is not None:
                        rows = [[(cell.text or "") for cell in row.cells]
                                for row in t.rows]
                        lines.extend(self._table_to_text(rows))
                        lines.append("")
        else:
            for p in sorted(data.paragraphs, key=lambda x: x.index):
                lines.append((p.text or "").strip())
                lines.append("")
            for t in sorted(data.tables, key=lambda x: x.index):
                rows = [[(cell.text or "") for cell in row.cells]
                        for row in t.rows]
                lines.extend(self._table_to_text(rows))
                lines.append("")

        return lines

    def _table_to_text(self, rows: list[list[str]]) -> list[str]:
        if not rows:
            return []
        return [" | ".join(cell.strip() for cell in row) for row in rows]
