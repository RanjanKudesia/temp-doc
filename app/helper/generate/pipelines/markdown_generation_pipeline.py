"""Markdown generation pipeline for temp-doc service."""

import logging
import re

from ...schemas.temp_doc_schema import ExtractedData


class MarkdownGenerationPipeline:
    """Generate Markdown from extracted data."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, extracted_data: ExtractedData, title: str | None = None) -> bytes:
        """Generate Markdown from extracted data."""
        try:
            parts = self._build_parts(extracted_data, title)
            content = "\n\n".join(
                part for part in parts if part.strip()).rstrip() + "\n"
            return content.encode("utf-8")
        except Exception as e:
            self.logger.error("Markdown generation failed: %s", str(e))
            raise

    def _build_parts(self, data: ExtractedData, title: str | None) -> list[str]:
        paragraph_by_index = {p.index: p for p in data.paragraphs}
        table_by_index = {t.index: t for t in data.tables}
        parts: list[str] = []

        if title:
            parts.append(f"# {title}")

        if data.document_order:
            self._append_parts_in_order(
                data,
                parts,
                paragraph_by_index,
                table_by_index,
            )
            return parts

        self._append_parts_sorted(data, parts)

        return parts

    def _append_parts_in_order(
        self,
        data: ExtractedData,
        parts: list[str],
        paragraph_by_index: dict,
        table_by_index: dict,
    ) -> None:
        """Append markdown parts following explicit document order."""
        for item in data.document_order:
            if item.type == "paragraph":
                paragraph = paragraph_by_index.get(item.index)
                if paragraph is not None:
                    parts.append(self._paragraph_to_md(paragraph))
            elif item.type == "table":
                table = table_by_index.get(item.index)
                if table is not None:
                    parts.append(self._table_to_md(table))

    def _append_parts_sorted(self, data: ExtractedData, parts: list[str]) -> None:
        """Append markdown parts in index order when no document_order is provided."""
        for paragraph in sorted(data.paragraphs, key=lambda item: item.index):
            parts.append(self._paragraph_to_md(paragraph))
        for table in sorted(data.tables, key=lambda item: item.index):
            parts.append(self._table_to_md(table))

    def _paragraph_to_md(self, paragraph) -> str:
        style = paragraph.style or ""
        heading_level = self._heading_level(style)

        # Fenced code block — emit with language tag and fences
        code_lang = getattr(paragraph, "code_fence_language", None)
        if code_lang is not None or style == "CodeBlock":
            lang = code_lang if code_lang is not None else ""
            body = paragraph.text or ""
            return f"```{lang}\n{body}\n```"

        text = self._runs_to_md(paragraph.runs) if paragraph.runs else (
            paragraph.text or "")
        text = text.strip()

        if heading_level:
            # Strip any leading '#' that may be present in replaced text
            # to prevent "# # Heading" double-prefix on round-trip
            text = re.sub(r"^#+\s*", "", text)
            return f"{'#' * heading_level} {text}"
        if getattr(paragraph, "is_bullet", False):
            indent_level = self._list_indent_level(paragraph)
            prefix = "  " * indent_level
            return f"{prefix}- {text}"
        if getattr(paragraph, "is_numbered", False):
            indent_level = self._list_indent_level(paragraph)
            prefix = "  " * indent_level
            marker = getattr(paragraph, "numbering_format", None) or "1."
            if not re.match(r"^\d+[.)]$", marker):
                marker = "1."
            return f"{prefix}{marker} {text}"
        return text

    def _list_indent_level(self, paragraph) -> int:
        """Return the nesting depth stored in list_info, or 0."""
        list_info = getattr(paragraph, "list_info", None)
        if isinstance(list_info, dict):
            return int(list_info.get("indent_level") or 0)
        if list_info is not None:
            return int(getattr(list_info, "indent_level", 0) or 0)
        return 0

    def _runs_to_md(self, runs: list) -> str:
        return "".join(
            self._apply_inline(
                r.text or "",
                r.bold,
                r.italic,
                r.underline,
                getattr(r, "hyperlink_url", None),
                getattr(r, "code", None),
            )
            for r in runs
        ).replace("\n", "  \n")

    def _apply_inline(
        self,
        text: str,
        bold: bool | None,
        italic: bool | None,
        underline: bool | None,
        link: str | None = None,
        code: bool | None = None,
    ) -> str:
        if code:
            return f"`{text}`"
        result = text
        if bold and italic:
            result = f"***{result}***"
        elif bold:
            result = f"**{result}**"
        elif italic:
            result = f"*{result}*"
        if underline:
            result = f"<u>{result}</u>"
        if link:
            result = f"[{result}]({link})"
        return result

    def _table_to_md(self, table) -> str:
        rows: list[list[str]] = []
        for row in table.rows:
            rows.append([cell.text or "" for cell in row.cells])
        return self._rows_to_md(rows)

    def _rows_to_md(self, rows: list[list[str]]) -> str:
        if not rows:
            return ""
        max_cols = max((len(r) for r in rows), default=0)
        if max_cols == 0:
            return ""
        # Normalize row lengths
        normalized = [row + [""] * (max_cols - len(row)) for row in rows]
        header = normalized[0]
        separator = ["---"] * max_cols
        body = normalized[1:]

        lines = [self._pipe_row(header), self._pipe_row(separator)]
        lines.extend(self._pipe_row(r) for r in body)
        return "\n".join(lines)

    def _pipe_row(self, row: list[str]) -> str:
        return "| " + " | ".join((cell or "").replace("\n", " ").strip() for cell in row) + " |"

    def _escape_markdown(self, text: str) -> str:
        return text.replace("|", "\\|").replace("\n", " ")

    def _heading_level(self, style: str | None) -> int | None:
        if not style:
            return None
        m = re.search(r"heading\s*([1-6])", style, re.IGNORECASE)
        if m:
            return int(m.group(1))
        m2 = re.match(r"^h([1-6])$", style.lower())
        if m2:
            return int(m2.group(1))
        return None
