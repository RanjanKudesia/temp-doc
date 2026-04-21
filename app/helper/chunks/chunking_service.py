"""Chunking service for extracted document payloads."""

from __future__ import annotations

import re
from dataclasses import dataclass

from ..schemas.temp_doc_schema import (
    ExtractedData,
    ExtractedParagraph,
    ExtractedPptData,
    ExtractedTable,
)

_HEADING_RE = re.compile(r"heading\s*([1-6])", re.IGNORECASE)
_SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+")


@dataclass
class _ChunkUnit:
    heading: str | None
    parts: list[str]


class ChunkingService:
    """Create meaningful text chunks from extracted document JSON."""

    def __init__(self, max_chunk_chars: int = 1200, min_chunk_chars: int = 350) -> None:
        self.max_chunk_chars = max(300, max_chunk_chars)
        self.min_chunk_chars = max(
            120, min(min_chunk_chars, self.max_chunk_chars))

    def chunk_docx(self, extracted_data: ExtractedData) -> list[str]:
        """Chunk DOCX extracted data into section-aware text chunks."""
        units = self._build_docx_units(extracted_data)
        has_heading_units = any(unit.heading for unit in units)

        if has_heading_units:
            units = self._absorb_heading_only_units(units)

        chunks: list[str] = []
        for unit in units:
            chunks.extend(self._split_unit(unit))

        if has_heading_units:
            return chunks

        return self._merge_short_chunks(chunks)

    def _absorb_heading_only_units(self, units: list[_ChunkUnit]) -> list[_ChunkUnit]:
        """Merge sections that contain only a heading into the following section.

        Example: a "Tables" Heading 2 with no body text immediately before the
        "Simple Tables" Heading 3 would produce a useless single-line chunk.
        Instead, the parent heading text is prepended into the next unit so that
        "Tables > Simple Tables" context is preserved without a standalone stub.
        """
        result: list[_ChunkUnit] = []
        pending_prefix: str | None = None

        for unit in units:
            is_heading_only = (
                unit.heading is not None
                and len(unit.parts) == 1
                and unit.parts[0] == unit.heading
            )

            if is_heading_only:
                pending_prefix = unit.heading
                continue

            if pending_prefix is not None:
                unit = _ChunkUnit(
                    heading=unit.heading,
                    parts=[pending_prefix] + unit.parts,
                )
                pending_prefix = None

            result.append(unit)

        if pending_prefix is not None:
            result.append(_ChunkUnit(
                heading=pending_prefix, parts=[pending_prefix]))

        return result

    def chunk_pptx(self, extracted_data: ExtractedPptData) -> list[str]:
        """Chunk PPTX extracted data: one chunk per slide, split if oversized."""
        paragraph_map = {p.index: p for p in extracted_data.paragraphs}
        table_map = {t.index: t for t in extracted_data.tables}

        chunks: list[str] = []
        for slide in extracted_data.slides:
            parts = self._build_slide_parts(
                slide, paragraph_map, table_map
            )
            slide_num = slide.get("slide_number") or (
                (slide.get("index") or 0) + 1)
            title = self._clean_text(slide.get("title"))
            header = f"Slide {slide_num}: {title}" if title else f"Slide {slide_num}"

            unit = _ChunkUnit(heading=header, parts=parts)
            chunks.extend(self._split_unit(unit))

        return chunks

    def _build_slide_parts(
        self,
        slide: dict,
        paragraph_map: dict,
        table_map: dict,
    ) -> list[str]:
        """Build parts list for a single slide."""
        parts: list[str] = []

        for idx in slide.get("paragraph_indices") or []:
            para = paragraph_map.get(idx)
            if para is None:
                continue
            if self._is_heading(para):
                continue
            text = self._format_paragraph(para)
            if text:
                parts.append(text)

        for idx in slide.get("table_indices") or []:
            table = table_map.get(idx)
            if table is None:
                continue
            table_text = self._format_table(table)
            if table_text:
                parts.append(table_text)

        notes = self._clean_text(slide.get("notes_text"))
        if notes:
            parts.append(f"Notes: {notes}")

        return parts

    def _build_docx_units(self, extracted_data: ExtractedData) -> list[_ChunkUnit]:
        """Build units from DOCX extracted data by processing document order."""
        paragraph_map = {
            paragraph.index: paragraph for paragraph in extracted_data.paragraphs}
        table_map = {table.index: table for table in extracted_data.tables}

        units: list[_ChunkUnit] = []
        context: dict = {
            "heading": None,
            "parts": [],
        }

        for item in extracted_data.document_order:
            self._process_docx_item(
                item, paragraph_map, table_map, units, context
            )

        self._flush_docx_context(units, context)

        if units:
            return units

        return self._build_docx_fallback_units(extracted_data)

    def _process_docx_item(
        self,
        item: any,
        paragraph_map: dict,
        table_map: dict,
        units: list[_ChunkUnit],
        context: dict,
    ) -> None:
        """Process a single item from document order."""
        if item.type == "paragraph":
            self._process_docx_paragraph(
                item, paragraph_map, units, context
            )
        elif item.type == "table":
            self._process_docx_table(item, table_map, context)

    def _process_docx_paragraph(
        self,
        item: any,
        paragraph_map: dict,
        units: list[_ChunkUnit],
        context: dict,
    ) -> None:
        """Process a paragraph item from document order."""
        paragraph = paragraph_map.get(item.index)
        if paragraph is None:
            return

        paragraph_text = self._format_paragraph(paragraph)
        if not paragraph_text:
            return

        if self._is_heading(paragraph):
            self._flush_docx_context(units, context)
            context["heading"] = paragraph_text
            context["parts"] = [paragraph_text]
            return

        context["parts"].append(paragraph_text)

    def _process_docx_table(
        self,
        item: any,
        table_map: dict,
        context: dict,
    ) -> None:
        """Process a table item from document order."""
        table = table_map.get(item.index)
        if table is None:
            return
        table_text = self._format_table(table)
        if table_text:
            context["parts"].append(table_text)

    def _flush_docx_context(
        self,
        units: list[_ChunkUnit],
        context: dict,
    ) -> None:
        """Flush accumulated context into units."""
        parts = [part for part in context["parts"] if part]
        if parts:
            units.append(_ChunkUnit(heading=context["heading"], parts=parts))
        context["heading"] = None
        context["parts"] = []

    def _build_docx_fallback_units(
        self, extracted_data: ExtractedData
    ) -> list[_ChunkUnit]:
        """Build fallback units when document order is empty."""
        fallback_parts = [
            self._format_paragraph(paragraph)
            for paragraph in extracted_data.paragraphs
        ]
        fallback_parts = [part for part in fallback_parts if part]
        if fallback_parts:
            return [_ChunkUnit(heading=None, parts=fallback_parts)]

        fallback_tables = [
            self._format_table(table)
            for table in extracted_data.tables
        ]
        fallback_tables = [table for table in fallback_tables if table]
        if fallback_tables:
            return [_ChunkUnit(heading=None, parts=fallback_tables)]

        return []

    def _format_paragraph(self, paragraph: ExtractedParagraph) -> str:
        text = self._clean_text(paragraph.text)
        if not text:
            return ""

        if paragraph.is_bullet:
            indent = "  " * max(paragraph.list_level or 0, 0)
            return f"{indent}- {text}".strip()

        if paragraph.is_numbered:
            indent = "  " * max(paragraph.list_level or 0, 0)
            marker = paragraph.numbering_format or "1."
            return f"{indent}{marker} {text}".strip()

        return text

    def _format_table(self, table: ExtractedTable) -> str:
        row_texts: list[str] = []
        for row in table.rows:
            cells = [
                self._clean_text(cell.text)
                for cell in row.cells
                if self._clean_text(cell.text)
            ]
            if cells:
                row_texts.append(" | ".join(cells))

        if not row_texts:
            return ""

        return "Table:\n" + "\n".join(row_texts)

    def _is_heading(self, paragraph: ExtractedParagraph) -> bool:
        style_name = (paragraph.style or "").strip()
        if not style_name:
            return False
        if style_name.lower() == "title":
            return True
        match = _HEADING_RE.search(style_name)
        return bool(match and int(match.group(1)) <= 4)

    def _split_unit(self, unit: _ChunkUnit) -> list[str]:
        """Split unit into chunks respecting size constraints."""
        chunks: list[str] = []
        current_parts: list[str] = []
        current_length = 0

        for part in unit.parts:
            self._process_unit_part(
                part, chunks, current_parts, current_length
            )

        if current_parts:
            chunks.append("\n".join(current_parts).strip())

        return [chunk for chunk in chunks if chunk]

    def _process_unit_part(
        self,
        part: str,
        chunks: list[str],
        current_parts: list[str],
        current_length: int,
    ) -> None:
        """Process a single part for unit splitting."""
        if len(part) > self.max_chunk_chars:
            if current_parts:
                chunks.append("\n".join(current_parts).strip())
                current_parts.clear()
            chunks.extend(self._split_large_text(part))
            return

        separator_length = 1 if current_parts else 0
        candidate_length = current_length + separator_length + len(part)
        if candidate_length <= self.max_chunk_chars:
            current_parts.append(part)
            return

        if current_parts:
            chunks.append("\n".join(current_parts).strip())
            current_parts.clear()
        current_parts.append(part)

    def _split_large_text(self, text: str) -> list[str]:
        sentences = [sentence.strip() for sentence in _SENTENCE_BOUNDARY_RE.split(
            text) if sentence.strip()]
        if not sentences:
            return [text[: self.max_chunk_chars].strip()]

        chunks: list[str] = []
        current = ""
        for sentence in sentences:
            candidate = f"{current} {sentence}".strip(
            ) if current else sentence
            if len(candidate) <= self.max_chunk_chars:
                current = candidate
                continue

            if current:
                chunks.append(current)
            current = sentence

            while len(current) > self.max_chunk_chars:
                split_at = current.rfind(" ", 0, self.max_chunk_chars)
                if split_at <= self.max_chunk_chars // 2:
                    split_at = self.max_chunk_chars
                chunks.append(current[:split_at].strip())
                current = current[split_at:].strip()

        if current:
            chunks.append(current)

        return [chunk for chunk in chunks if chunk]

    def _merge_short_chunks(self, chunks: list[str]) -> list[str]:
        if not chunks:
            return []

        merged: list[str] = []
        for chunk in chunks:
            if merged and len(chunk) < self.min_chunk_chars:
                candidate = f"{merged[-1]}\n{chunk}".strip()
                if len(candidate) <= self.max_chunk_chars:
                    merged[-1] = candidate
                    continue
            merged.append(chunk)
        return merged

    def _clean_text(self, text: str | None) -> str:
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()
