"""Chunking engine for the chunking service.

This is a self-contained copy of the chunking logic.
Does not import from helper.chunks service API.
"""

from __future__ import annotations
from app.schemas.temp_doc_schema import (
    ExtractedData,
    ExtractedPptData,
    ExtractedTable,
)

import logging
import re
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


_HEADING_RE = re.compile(r"heading\s*([1-6])", re.IGNORECASE)
_SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+")

# Fields ChunkEngine actually reads from each paragraph — used to build lean dicts
_PARAGRAPH_FIELDS = ("text", "style", "is_bullet",
                     "is_numbered", "list_level", "numbering_format")


@dataclass
class _ChunkUnit:
    heading: str | None
    parts: list[str]


class ChunkEngine:
    """Create meaningful text chunks from extracted document JSON.

    Copied inline from ChunkingService — no import from helper.chunks.
    """

    def __init__(self, max_chunk_chars: int = 1200, min_chunk_chars: int = 350) -> None:
        self.max_chunk_chars = max(300, max_chunk_chars)
        self.min_chunk_chars = max(
            120, min(min_chunk_chars, self.max_chunk_chars))

    # ── Public ────────────────────────────────────────────────────────────────

    def chunk_docx(self, extracted_data: ExtractedData) -> list[str]:
        """Chunk DOCX/PDF/HTML/Markdown/TXT extracted data into section-aware chunks."""
        t0 = time.perf_counter()
        logger.info(
            "[chunk_engine] chunk_docx started | paragraphs=%d | tables=%d",
            len(extracted_data.paragraphs),
            len(extracted_data.tables),
        )

        t1 = time.perf_counter()
        units = self._build_docx_units(extracted_data)
        logger.info(
            "[chunk_engine] _build_docx_units done | units=%d | elapsed=%dms",
            len(units), round((time.perf_counter() - t1) * 1000),
        )

        has_heading_units = any(unit.heading for unit in units)
        if has_heading_units:
            t2 = time.perf_counter()
            units = self._absorb_heading_only_units(units)
            logger.info(
                "[chunk_engine] _absorb_heading_only_units done | units=%d | elapsed=%dms",
                len(units), round((time.perf_counter() - t2) * 1000),
            )

        t3 = time.perf_counter()
        chunks: list[str] = []
        for unit in units:
            chunks.extend(self._split_unit(unit))
        logger.info(
            "[chunk_engine] _split_unit (all units) done | raw_chunks=%d | elapsed=%dms",
            len(chunks), round((time.perf_counter() - t3) * 1000),
        )

        result = self._merge_short_chunks(chunks)
        logger.info(
            "[chunk_engine] chunk_docx done | final_chunks=%d | total_elapsed=%dms",
            len(result), round((time.perf_counter() - t0) * 1000),
        )
        return result

    def chunk_pptx(self, extracted_data: ExtractedPptData) -> list[str]:
        """Chunk PPTX extracted data: one chunk per slide, split if oversized."""
        t0 = time.perf_counter()
        slide_count = len(extracted_data.slides)
        logger.info(
            "[chunk_engine] chunk_pptx started | slides=%d | paragraphs=%d",
            slide_count, len(extracted_data.paragraphs),
        )

        paragraph_map: dict[int, dict] = {
            p.index: {f: getattr(p, f, None) for f in _PARAGRAPH_FIELDS}
            for p in extracted_data.paragraphs
        }
        table_map = {t.index: t for t in extracted_data.tables}

        chunks: list[str] = []
        for slide in extracted_data.slides:
            parts = self._build_slide_parts(slide, paragraph_map, table_map)
            slide_num = slide.get("slide_number") or (
                (slide.get("index") or 0) + 1)
            title = self._clean_text(slide.get("title"))
            header = f"Slide {slide_num}: {title}" if title else f"Slide {slide_num}"
            logger.debug(
                "[chunk_engine] Slide %s | parts=%d", slide_num, len(parts)
            )

            unit = _ChunkUnit(heading=header, parts=parts)
            chunks.extend(self._split_unit(unit))

        result = self._merge_short_chunks(chunks)
        logger.info(
            "[chunk_engine] chunk_pptx done | final_chunks=%d | total_elapsed=%dms",
            len(result), round((time.perf_counter() - t0) * 1000),
        )
        return result

    # ── Slide helpers ─────────────────────────────────────────────────────────

    def _build_slide_parts(
        self,
        slide: dict,
        paragraph_map: dict,
        table_map: dict,
    ) -> list[str]:
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

    # ── DOCX unit builders ────────────────────────────────────────────────────

    def _build_docx_units(self, extracted_data: ExtractedData) -> list[_ChunkUnit]:
        # Build lean dicts with only the 6 fields ChunkEngine reads — avoids
        # attribute lookups on Pydantic model objects for every paragraph.
        paragraph_map: dict[int, dict] = {
            p.index: {f: getattr(p, f, None) for f in _PARAGRAPH_FIELDS}
            for p in extracted_data.paragraphs
        }
        table_map = {t.index: t for t in extracted_data.tables}

        units: list[_ChunkUnit] = []
        context: dict = {"heading": None, "parts": []}

        for item in extracted_data.document_order:
            self._process_docx_item(
                item, paragraph_map, table_map, units, context)

        self._flush_docx_context(units, context)

        if units:
            return units

        return self._build_docx_fallback_units(extracted_data)

    def _process_docx_item(self, item, paragraph_map, table_map, units, context) -> None:
        if item.type == "paragraph":
            self._process_docx_paragraph(item, paragraph_map, units, context)
        elif item.type == "table":
            self._process_docx_table(item, table_map, context)

    def _process_docx_paragraph(self, item, paragraph_map, units, context) -> None:
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

    def _process_docx_table(self, item, table_map, context) -> None:
        table = table_map.get(item.index)
        if table is None:
            return
        table_text = self._format_table(table)
        if table_text:
            context["parts"].append(table_text)

    def _flush_docx_context(self, units: list[_ChunkUnit], context: dict) -> None:
        parts = [part for part in context["parts"] if part]
        if parts:
            units.append(_ChunkUnit(heading=context["heading"], parts=parts))
        context["heading"] = None
        context["parts"] = []

    def _build_docx_fallback_units(
        self, extracted_data: ExtractedData
    ) -> list[_ChunkUnit]:
        fallback_parts = [
            self._format_paragraph({f: getattr(p, f, None)
                                   for f in _PARAGRAPH_FIELDS})
            for p in extracted_data.paragraphs
        ]
        fallback_parts = [p for p in fallback_parts if p]
        if fallback_parts:
            return [_ChunkUnit(heading=None, parts=fallback_parts)]

        fallback_tables = [
            self._format_table(t) for t in extracted_data.tables
        ]
        fallback_tables = [t for t in fallback_tables if t]
        if fallback_tables:
            return [_ChunkUnit(heading=None, parts=fallback_tables)]

        return []

    def _absorb_heading_only_units(self, units: list[_ChunkUnit]) -> list[_ChunkUnit]:
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

    # ── Formatting ────────────────────────────────────────────────────────────

    def _format_paragraph(self, paragraph: dict) -> str:
        text = self._clean_text(paragraph["text"])
        if not text:
            return ""
        if paragraph["is_bullet"]:
            indent = "  " * max(paragraph["list_level"] or 0, 0)
            return f"{indent}- {text}".strip()
        if paragraph["is_numbered"]:
            indent = "  " * max(paragraph["list_level"] or 0, 0)
            marker = paragraph["numbering_format"] or "1."
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

    def _is_heading(self, paragraph: dict) -> bool:
        style_name = (paragraph["style"] or "").strip()
        if not style_name:
            return False
        if style_name.lower() == "title":
            return True
        match = _HEADING_RE.search(style_name)
        return bool(match and int(match.group(1)) <= 4)

    # ── Splitting ─────────────────────────────────────────────────────────────

    def _split_unit(self, unit: _ChunkUnit) -> list[str]:
        chunks: list[str] = []
        current_parts: list[str] = []
        current_length = 0

        for part in unit.parts:
            if len(part) > self.max_chunk_chars:
                if current_parts:
                    chunks.append("\n".join(current_parts).strip())
                    current_parts.clear()
                    current_length = 0
                chunks.extend(self._split_large_text(part))
                continue

            separator_length = 1 if current_parts else 0
            candidate_length = current_length + separator_length + len(part)
            if candidate_length <= self.max_chunk_chars:
                current_parts.append(part)
                current_length = candidate_length
            else:
                if current_parts:
                    chunks.append("\n".join(current_parts).strip())
                current_parts = [part]
                current_length = len(part)

        if current_parts:
            chunks.append("\n".join(current_parts).strip())

        return [chunk for chunk in chunks if chunk]

    def _split_large_text(self, text: str) -> list[str]:
        sentences = [
            s.strip() for s in _SENTENCE_BOUNDARY_RE.split(text) if s.strip()
        ]
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
        # Soft ceiling: allow merging tiny chunks even if the result slightly
        # exceeds max_chunk_chars (prevents orphan footer/straggler chunks).
        soft_max = self.max_chunk_chars + self.min_chunk_chars
        for chunk in chunks:
            if merged and len(chunk) < self.min_chunk_chars:
                candidate = f"{merged[-1]}\n{chunk}".strip()
                if len(candidate) <= soft_max:
                    merged[-1] = candidate
                    continue
            merged.append(chunk)
        return merged

    def _clean_text(self, text: str | None) -> str:
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()
