"""Simple PyMuPDF-only PDF chunker.

Strategy:
- Extract text blocks page-by-page with fitz (no pdfplumber, no extraction schema)
- Detect tables via page.find_tables() and render them as plain text rows
- Accumulate text in a buffer; flush a chunk at a sentence boundary when the
  buffer exceeds max_chars
- Carry overlap_chars of the previous chunk into the next to preserve context

This pipeline bypasses ExtractedData entirely and returns plain text chunks
directly, making it much faster than the structured pipeline for large PDFs.
"""

from __future__ import annotations

import logging
import re

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)

# Default chunking parameters
_MAX_CHARS: int = 800
_OVERLAP_CHARS: int = 80

_SENTENCE_END = re.compile(r"(?<=[.!?])\s+")
_MULTI_SPACE = re.compile(r"\s+")


# ── Public entry point ────────────────────────────────────────────────────────


def chunk_pdf_simple(
    file_bytes: bytes,
    max_chars: int = _MAX_CHARS,
    overlap_chars: int = _OVERLAP_CHARS,
) -> list[str]:
    """Extract and chunk a PDF using PyMuPDF only.

    Args:
        file_bytes: Raw PDF bytes.
        max_chars:  Target maximum characters per chunk.
        overlap_chars: Characters from the end of the previous chunk carried
                       into the start of the next for context continuity.

    Returns:
        List of text chunk strings. Never empty for non-trivial PDFs.
    """
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    try:
        page_count = len(doc)
        chunks = _extract_and_chunk(doc, max_chars, overlap_chars)
    finally:
        doc.close()

    logger.info("[pdf_simple_pipeline] %d chunks produced from %d pages", len(
        chunks), page_count)
    return chunks


# ── Core extraction ───────────────────────────────────────────────────────────


def _extract_and_chunk(
    doc: fitz.Document,
    max_chars: int,
    overlap_chars: int,
) -> list[str]:
    """Iterate all pages, collect text+tables, emit chunks."""
    chunks: list[str] = []
    buffer: str = ""
    tables_supported = _check_tables_support(doc)

    for page_num in range(len(doc)):
        page = doc[page_num]
        table_bboxes, table_texts = (
            _extract_page_tables(page) if tables_supported
            else ([], [])
        )
        buffer, chunks = _process_page_blocks(
            page, table_bboxes, buffer, chunks, max_chars, overlap_chars
        )
        buffer, chunks = _flush_table_texts(
            table_texts, buffer, chunks, max_chars, overlap_chars
        )

    if buffer.strip():
        chunks.append(buffer.strip())

    return chunks


def _flush_table_texts(
    table_texts: list[str],
    buffer: str,
    chunks: list[str],
    max_chars: int,
    overlap_chars: int,
) -> tuple[str, list[str]]:
    """Append each table's plain-text render to the buffer and flush as needed."""
    for ttext in table_texts:
        if not ttext:
            continue
        buffer = (buffer + "\n" + ttext).lstrip() if buffer else ttext
        buffer, chunks = _flush_if_full(
            buffer, chunks, max_chars, overlap_chars)
    return buffer, chunks


def _extract_page_tables(
    page: fitz.Page,
) -> tuple[list[fitz.Rect], list[str]]:
    """Return (bounding_boxes, plain_text_renders) for all tables on *page*."""
    bboxes: list[fitz.Rect] = []
    texts: list[str] = []
    try:
        for table in page.find_tables():
            bboxes.append(table.bbox)
            texts.append(_table_to_text(table))
    except (AttributeError, RuntimeError, ValueError):
        pass  # gracefully degrade — treat table regions as plain text
    return bboxes, texts


def _process_page_blocks(
    page: fitz.Page,
    table_bboxes: list[fitz.Rect],
    buffer: str,
    chunks: list[str],
    max_chars: int,
    overlap_chars: int,
) -> tuple[str, list[str]]:
    """Process all text blocks on *page*, skipping table regions."""
    for block in page.get_text("blocks", sort=True):
        if block[6] != 0:  # skip image blocks (type 1)
            continue
        if any(_rect_overlap_ratio(fitz.Rect(block[:4]), tb) > 0.5 for tb in table_bboxes):
            continue  # will be rendered as table text
        text = _normalise(block[4])
        if not text:
            continue
        buffer = (buffer + " " + text).lstrip() if buffer else text
        buffer, chunks = _flush_if_full(
            buffer, chunks, max_chars, overlap_chars)
    return buffer, chunks


def _flush_if_full(
    buffer: str,
    chunks: list[str],
    max_chars: int,
    overlap_chars: int,
) -> tuple[str, list[str]]:
    """Emit chunks from buffer while it exceeds max_chars."""
    while len(buffer) >= max_chars:
        chunk, buffer = _split_at_boundary(buffer, max_chars, overlap_chars)
        if chunk:
            chunks.append(chunk)
        else:
            # Safety: avoid infinite loop if split returns empty chunk
            chunks.append(buffer[:max_chars].strip())
            buffer = buffer[max_chars:].strip()
    return buffer, chunks


def _split_at_boundary(
    text: str,
    max_chars: int,
    overlap_chars: int,
) -> tuple[str, str]:
    """Split *text* at the last sentence boundary before *max_chars*.

    Returns (chunk, remainder_with_overlap).
    """
    candidate = text[:max_chars]

    # Prefer the last sentence boundary in the second half of the candidate
    split_pos: int = -1
    for m in _SENTENCE_END.finditer(candidate):
        if m.start() > max_chars // 3:
            split_pos = m.start() + 1  # include the trailing space

    if split_pos == -1:
        # Fall back to last word boundary
        last_space = candidate.rfind(" ")
        split_pos = last_space if last_space > max_chars // 3 else max_chars

    chunk = text[:split_pos].strip()
    remainder = text[split_pos:].strip()

    # Prepend overlap from the end of chunk into remainder
    if overlap_chars > 0 and len(chunk) > overlap_chars:
        overlap_text = chunk[-overlap_chars:]
        # Start overlap at a word boundary
        first_space = overlap_text.find(" ")
        if first_space != -1:
            overlap_text = overlap_text[first_space + 1:]
        remainder = (overlap_text + " " +
                     remainder).strip() if remainder else overlap_text

    return chunk, remainder


# ── Table rendering ───────────────────────────────────────────────────────────


def _table_to_text(table: object) -> str:
    """Render a PyMuPDF Table object as pipe-separated plain text rows."""
    try:
        rows = table.extract()  # list[list[str|None]]
    except (AttributeError, RuntimeError, ValueError):
        return ""

    lines: list[str] = []
    for row in rows:
        cells = [_normalise(str(c)) if c is not None else "" for c in row]
        line = " | ".join(cells)
        if line.strip():
            lines.append(line)

    return "\n".join(lines)


# ── Utilities ─────────────────────────────────────────────────────────────────


def _check_tables_support(doc: fitz.Document) -> bool:
    """Return True if page.find_tables() is available (PyMuPDF >= 1.23)."""
    if len(doc) == 0:
        return False
    try:
        doc[0].find_tables()
        return True
    except AttributeError:
        return False


def _rect_overlap_ratio(a: fitz.Rect, b: fitz.Rect) -> float:
    """Return the fraction of *a* that is covered by *b* (0.0–1.0)."""
    inter = a & b
    if inter.is_empty:
        return 0.0
    area_a = a.width * a.height
    if area_a == 0:
        return 0.0
    return (inter.width * inter.height) / area_a


def _normalise(text: str) -> str:
    """Collapse whitespace and strip."""
    return _MULTI_SPACE.sub(" ", text).strip()
