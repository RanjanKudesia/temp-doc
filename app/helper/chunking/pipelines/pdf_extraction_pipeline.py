"""PDF extraction pipeline using PyMuPDF (fitz) + pdfplumber.

Strategy
--------
1. Open with fitz; if encrypted → ValueError immediately.
2. Sample total text chars across all pages. If < MIN_TEXT_CHARS the PDF is
   likely scanned/image-only → fall back to PdfConversionPipeline (pdf2docx).
3. Collect all font sizes across the document to determine the modal (body)
   font size and map larger sizes to heading levels.
4. For each page:
   a. pdfplumber detects tables and returns their bounding boxes + cell text.
   b. fitz returns text blocks with full span-level formatting.
   c. fitz blocks that overlap a pdfplumber table bbox (>= 40%) are discarded
      — pdfplumber owns those regions.
   d. Remaining fitz blocks become paragraphs.
   e. All page items (paragraphs + tables) are sorted by y-position for
      correct reading order.
5. Images are extracted xref-by-xref from fitz, deduplicated across pages.
6. Output dict matches the ExtractedData schema exactly.
"""

from __future__ import annotations

import base64
import logging
import os
import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from typing import Any, Iterator

import fitz  # PyMuPDF
import pdfplumber

from app.helper.chunking.pipelines.pdf_conversion_pipeline import (
    PdfConversionPipeline,
)

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

# fitz span flag bits
_FLAG_ITALIC: int = 1   # bit 0
_FLAG_BOLD: int = 16    # bit 4

# A PDF with fewer than this many text chars is treated as scanned/image-only
_MIN_TEXT_CHARS: int = 100

# Fraction of a fitz block's area that must overlap a table bbox for it to be
# considered "inside" the table (and therefore excluded from paragraphs)
_TABLE_OVERLAP_THRESHOLD: float = 0.40

# Bullet characters that may appear at the start of list items
_BULLET_CHAR_SET: frozenset[str] = frozenset("•·▪▸▹►◦‣⁃◉○●∙")

# Regex: leading bullet chars or - / * / + followed by whitespace
_BULLET_RE: re.Pattern[str] = re.compile(
    r"^[-*+•·▪▸▹►◦‣⁃◉○●∙]\s+"
)

# Regex: leading ordered-list marker  (1.  1)  a.  a)  i.  i) …)
_NUMBERED_RE: re.Pattern[str] = re.compile(
    r"^\s*(?:\d+|[a-zA-Z]|[ivxlcdmIVXLCDM]+)[.)]\s+"
)

# Heading levels assigned to sizes larger than body size
_HEADING_LEVELS: list[str] = [
    "Heading 1", "Heading 2", "Heading 3",
    "Heading 4", "Heading 5", "Heading 6",
]

# Number of pages sampled for font-size / heading heuristics (Pass 1).
# Sampling the first N pages is statistically sufficient for uniform docs and
# avoids a full document scan on large PDFs.
_FONT_SAMPLE_PAGES: int = 50

# Worker threads for parallel page extraction (Pass 3).
# Each thread opens its own fitz + pdfplumber instance (fitz is NOT thread-safe
# with shared Document objects).
_MAX_WORKERS: int = min(8, (os.cpu_count() or 4))


# ── Pipeline class ────────────────────────────────────────────────────────────

class PdfExtractionPipeline:
    """Extract PDF content natively using PyMuPDF + pdfplumber.

    Falls back to PdfConversionPipeline (pdf2docx) for scanned/image PDFs.
    """

    def __init__(self) -> None:
        self._fallback = PdfConversionPipeline()

    # ------------------------------------------------------------------ public

    def run(
        self, file_bytes: bytes, include_media: bool = True
    ) -> dict[str, Any]:
        """Extract a PDF and return a dict matching ExtractedData schema."""
        t0 = time.perf_counter()
        logger.info(
            "PDF extraction started",
            extra={"file_size_bytes": len(file_bytes)},
        )

        # ── Open with fitz ──────────────────────────────────────────────────
        try:
            doc: fitz.Document = fitz.open(
                stream=file_bytes, filetype="pdf"
            )
        except fitz.FileDataError as exc:
            raise ValueError(f"Corrupted or invalid PDF: {exc}") from exc
        except Exception as exc:
            raise ValueError(f"Failed to open PDF: {exc}") from exc

        # ── Encryption check ────────────────────────────────────────────────
        if _is_pdf_encrypted(doc):
            doc.close()
            raise ValueError(
                "PDF is password-protected. Provide an unlocked PDF."
            )

        # ── Scanned PDF detection is merged into Pass 1 (see _extract_native) ──
        # NOTE: Fallback disabled — native extraction only (for testing).
        # if total_chars < _MIN_TEXT_CHARS:
        #     doc.close()
        #     logger.info(
        #         "PDF has very little text (%d chars). "
        #         "Falling back to pdf2docx conversion.",
        #         total_chars,
        #     )
        #     return self._fallback.run(file_bytes, include_media=include_media)

        # ── Native extraction ────────────────────────────────────────────────
        try:
            result = self._extract_native(doc, file_bytes, include_media)
        finally:
            doc.close()

        elapsed_ms = round((time.perf_counter() - t0) * 1000)
        logger.info(
            "PDF extraction complete",
            extra={
                "elapsed_ms": elapsed_ms,
                "elapsed_s": round(elapsed_ms / 1000, 3),
                "paragraphs_extracted": len(result.get("paragraphs", [])),
                "tables_extracted": len(result.get("tables", [])),
                "media_extracted": len(result.get("media", [])),
                "method": "native_fitz_pdfplumber",
            },
        )
        return result

    # ----------------------------------------------------------------- private

    def _extract_native(
        self,
        doc: fitz.Document,
        file_bytes: bytes,
        include_media: bool,
    ) -> dict[str, Any]:
        """Core extraction: per-page paragraphs + tables + images."""
        total_pages = doc.page_count
        logger.info(
            "[pdf_pipeline] _extract_native started | pages=%d", total_pages)

        # ── Pass 1: font sizes + heading map + scanned-PDF check ────────────
        # Single pass over first _FONT_SAMPLE_PAGES pages — collects font sizes
        # for heading detection and counts span text chars as a scanned-PDF signal.
        t1 = time.perf_counter()
        sample_pages = min(_FONT_SAMPLE_PAGES, doc.page_count)
        all_font_sizes: list[float] = []
        sampled_chars: int = 0
        for span in _iter_document_spans(doc, fitz.TEXT_PRESERVE_WHITESPACE, max_pages=sample_pages):
            size = span.get("size", 0.0)
            if size and size > 0:
                all_font_sizes.append(round(size, 1))
            sampled_chars += len(span.get("text", ""))
        body_font_size = _detect_body_font_size(all_font_sizes)
        heading_size_map = _build_heading_size_map(
            all_font_sizes, body_font_size)
        logger.info(
            "[pdf_pipeline] Pass 1 done | sampled_pages=%d | unique_sizes=%d | "
            "body_size=%.1f | heading_levels=%d | sampled_chars=%d | elapsed=%dms",
            sample_pages, len(set(all_font_sizes)), body_font_size,
            len(heading_size_map), sampled_chars,
            round((time.perf_counter() - t1) * 1000),
        )

        # ── Pass 2: images ───────────────────────────────────────────────────
        media: list[dict[str, Any]] = []
        if include_media:
            t2 = time.perf_counter()
            media = _extract_media_items(doc)
            logger.info(
                "[pdf_pipeline] Pass 2 (media extraction) done | images=%d | elapsed=%dms",
                len(media), round((time.perf_counter() - t2) * 1000),
            )
        else:
            logger.info(
                "[pdf_pipeline] Pass 2 (media) skipped (include_media=False)")

        # ── Pass 3: per-page paragraph + table extraction ───────────────────
        t3 = time.perf_counter()
        all_paragraphs, all_tables, document_order = _extract_pages_content(
            doc=doc,
            file_bytes=file_bytes,
            heading_size_map=heading_size_map,
        )
        logger.info(
            "[pdf_pipeline] Pass 3 (page content) done | "
            "paragraphs=%d | tables=%d | elapsed=%dms",
            len(all_paragraphs), len(all_tables),
            round((time.perf_counter() - t3) * 1000),
        )

        document_defaults = _extract_document_defaults(all_font_sizes)

        return {
            "document_order": document_order,
            "document_defaults": document_defaults,
            "styles": [],
            "paragraphs": all_paragraphs,
            "tables": all_tables,
            "media": media,
        }


# ── Module-level helpers (stateless, no self needed) ─────────────────────────

def _detect_body_font_size(sizes: list[float]) -> float:
    """Return the modal (most-common) font size as the body text size."""
    if not sizes:
        return 12.0
    return Counter(sizes).most_common(1)[0][0]


def _build_heading_size_map(
    sizes: list[float], body_size: float
) -> dict[float, str]:
    """Map font sizes >5% larger than body size to heading level names.

    Largest unique size → Heading 1, next → Heading 2, etc.
    """
    threshold = body_size * 1.05
    larger = sorted({s for s in sizes if s > threshold}, reverse=True)
    return {
        sz: _HEADING_LEVELS[i]
        for i, sz in enumerate(larger[: len(_HEADING_LEVELS)])
    }


def _overlaps_any_table(
    block_bbox: tuple[float, float, float, float],
    table_bboxes: list[tuple[float, float, float, float]],
    threshold: float,
) -> bool:
    """Return True if block_bbox overlaps any table bbox by >= threshold fraction."""
    bx0, by0, bx1, by1 = block_bbox
    b_area = max((bx1 - bx0) * (by1 - by0), 1.0)
    for tx0, ty0, tx1, ty1 in table_bboxes:
        ix0 = max(bx0, tx0)
        iy0 = max(by0, ty0)
        ix1 = min(bx1, tx1)
        iy1 = min(by1, ty1)
        if ix1 <= ix0 or iy1 <= iy0:
            continue
        if (ix1 - ix0) * (iy1 - iy0) / b_area >= threshold:
            return True
    return False


def _is_pdf_encrypted(doc: fitz.Document) -> bool:
    """Handle API differences across PyMuPDF versions."""
    encrypted = bool(getattr(doc, "is_encrypted", False))
    needs_pass = bool(getattr(doc, "needs_pass", False))
    return encrypted or needs_pass


def _extract_media_items(doc: fitz.Document) -> list[dict[str, Any]]:
    """Extract and deduplicate PDF images across all pages by xref."""
    media: list[dict[str, Any]] = []
    seen_xrefs: set[int] = set()
    for page_num, page in enumerate(doc):
        for img_info in page.get_images(full=True):
            xref: int = img_info[0]
            if xref in seen_xrefs:
                continue
            seen_xrefs.add(xref)
            item = _extract_image(doc, xref, page_num)
            if item:
                media.append(item)
    return media


def _process_page_batch(
    file_bytes: bytes,
    page_nums: list[int],
    heading_size_map: dict[float, str],
) -> list[tuple[int, list[tuple[float, str, dict[str, Any]]]]]:
    """Process a batch of pages in a worker thread.

    Opens its own fitz.Document and pdfplumber instance — fitz is NOT
    thread-safe with a shared Document, so each worker must own its doc.
    Returns list of (page_num, page_items).
    """
    batch_results: list[tuple[int,
                              list[tuple[float, str, dict[str, Any]]]]] = []
    thread_doc = fitz.open(stream=file_bytes, filetype="pdf")
    try:
        with pdfplumber.open(BytesIO(file_bytes)) as plumber_doc:
            for page_num in page_nums:
                fitz_page = thread_doc[page_num]
                plumber_page = plumber_doc.pages[page_num]
                plumber_tables = _safe_find_tables(plumber_page, page_num)
                table_bboxes: list[tuple[float, float, float, float]] = [
                    t.bbox for t in plumber_tables
                ]
                page_items = _build_page_items(
                    fitz_page=fitz_page,
                    page_num=page_num,
                    heading_size_map=heading_size_map,
                    plumber_tables=plumber_tables,
                    table_bboxes=table_bboxes,
                    table_idx_start=0,  # indices are reassigned during assembly
                )
                batch_results.append((page_num, page_items))
    finally:
        thread_doc.close()
    return batch_results


def _extract_pages_content(
    doc: fitz.Document,
    file_bytes: bytes,
    heading_size_map: dict[float, str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Extract paragraph/table content and document order — parallel page processing."""
    total_pages = doc.page_count
    log_every = max(1, total_pages // 10)

    # Distribute pages evenly across workers
    all_page_nums = list(range(total_pages))
    batch_size = max(1, (total_pages + _MAX_WORKERS - 1) // _MAX_WORKERS)
    batches = [
        all_page_nums[i: i + batch_size]
        for i in range(0, total_pages, batch_size)
    ]
    actual_workers = min(_MAX_WORKERS, len(batches))
    logger.info(
        "[pdf_pipeline] Parallel page extraction | workers=%d | batches=%d | pages_per_batch~=%d",
        actual_workers, len(batches), batch_size,
    )

    # Run workers — each opens its own fitz + pdfplumber instance
    page_results: dict[int, list[tuple[float, str, dict[str, Any]]]] = {}
    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        futures = {
            executor.submit(_process_page_batch, file_bytes, batch, heading_size_map): batch
            for batch in batches
        }
        for future in as_completed(futures):
            for page_num, page_items in future.result():
                page_results[page_num] = page_items

    # Reassemble in page order and assign final sequential indices
    all_paragraphs: list[dict[str, Any]] = []
    all_tables: list[dict[str, Any]] = []
    document_order: list[dict[str, Any]] = []
    para_idx = 0
    table_idx = 0

    for page_num in range(total_pages):
        page_items = page_results.get(page_num, [])
        para_idx, table_idx = _append_page_items(
            page_items=page_items,
            para_idx=para_idx,
            table_idx=table_idx,
            all_paragraphs=all_paragraphs,
            all_tables=all_tables,
            document_order=document_order,
        )
        if (page_num + 1) % log_every == 0 or page_num == total_pages - 1:
            logger.info(
                "[pdf_pipeline] Page %d/%d assembled | "
                "total_paragraphs=%d | total_tables=%d",
                page_num + 1, total_pages, para_idx, table_idx,
            )

    return all_paragraphs, all_tables, document_order


def _safe_find_tables(plumber_page: Any, page_num: int) -> list[Any]:
    """Run table detection with bounded parser-related exception handling."""
    try:
        return plumber_page.find_tables()
    except (ValueError, TypeError, AttributeError, KeyError) as exc:
        logger.debug(
            "pdfplumber table detection failed on page %d: %s",
            page_num,
            exc,
        )
        return []


def _build_page_items(
    fitz_page: fitz.Page,
    page_num: int,
    heading_size_map: dict[float, str],
    plumber_tables: list[Any],
    table_bboxes: list[tuple[float, float, float, float]],
    table_idx_start: int,
) -> list[tuple[float, str, dict[str, Any]]]:
    """Build sorted page items in reading order: paragraphs and tables."""
    page_items: list[tuple[float, str, dict[str, Any]]] = []

    raw = fitz_page.get_text(
        "dict",
        flags=(fitz.TEXT_PRESERVE_WHITESPACE | fitz.TEXT_PRESERVE_LIGATURES),
    )
    fitz_text_blocks = [
        b for b in raw.get("blocks", []) if b.get("type") == 0
    ]

    for block in fitz_text_blocks:
        bx0, by0, bx1, by1 = block["bbox"]
        if _overlaps_any_table(
            (bx0, by0, bx1, by1),
            table_bboxes,
            _TABLE_OVERLAP_THRESHOLD,
        ):
            continue
        para = _block_to_paragraph(
            block,
            index=0,
            heading_size_map=heading_size_map,
            page_index=page_num,
        )
        if para is None:
            continue
        page_items.append((by0, "paragraph", para))

    for offset, table in enumerate(plumber_tables):
        tx0, ty0, tx1, ty1 = table.bbox
        tbl = _plumber_table_to_dict(
            table,
            table_idx_start + offset,
            page_index=page_num,
            bbox=(tx0, ty0, tx1, ty1),
        )
        page_items.append((ty0, "table", tbl))

    page_items.sort(key=lambda item: item[0])
    return page_items


def _append_page_items(
    page_items: list[tuple[float, str, dict[str, Any]]],
    para_idx: int,
    table_idx: int,
    all_paragraphs: list[dict[str, Any]],
    all_tables: list[dict[str, Any]],
    document_order: list[dict[str, Any]],
) -> tuple[int, int]:
    """Append sorted page items into global lists and advance indices."""
    for _, kind, data in page_items:
        if kind == "paragraph":
            data["index"] = para_idx
            all_paragraphs.append(data)
            document_order.append({"type": "paragraph", "index": para_idx})
            para_idx += 1
            continue
        data["index"] = table_idx
        all_tables.append(data)
        document_order.append({"type": "table", "index": table_idx})
        table_idx += 1
    return para_idx, table_idx


def _block_to_paragraph(
    block: dict[str, Any],
    index: int,
    heading_size_map: dict[float, str],
    page_index: int,
) -> dict[str, Any] | None:
    """Convert a fitz text block to a paragraph dict.

    Returns None if the block contains no non-empty text.
    """
    lines: list[dict] = block.get("lines", [])
    if not lines:
        return None

    # Build full text: join lines with space (word-wrap model)
    line_texts: list[str] = []
    all_spans: list[dict] = []
    for line in lines:
        spans = line.get("spans", [])
        all_spans.extend(spans)
        line_text = "".join(s.get("text", "") for s in spans)
        stripped = line_text.strip()
        if stripped:
            line_texts.append(stripped)

    full_text = " ".join(line_texts)
    if not full_text:
        return None

    # Detect heading style from the dominant (largest) font size in the block
    dominant_size = round(
        max((s.get("size", 0.0) for s in all_spans), default=0.0), 1
    )
    style = heading_size_map.get(dominant_size, "Normal")

    # List type detection
    is_bullet, is_numbered, numbering_format = _detect_list_type(full_text)

    runs = _build_runs(all_spans)

    return {
        "index": index,          # will be re-assigned by caller after sort
        "text": full_text,
        "style": style,
        "alignment": None,       # fitz doesn't expose paragraph alignment
        "direction": "ltr",
        "is_bullet": is_bullet,
        "is_numbered": is_numbered,
        "list_info": None,
        "numbering_format": numbering_format,
        "list_level": None,
        "page_index": page_index,
        "bbox": block.get("bbox"),
        "runs": runs,
    }


def _detect_list_type(text: str) -> tuple[bool, bool, str | None]:
    """Return (is_bullet, is_numbered, numbering_format) for a paragraph text."""
    if _BULLET_RE.match(text):
        return True, False, "bullet:•"
    m = _NUMBERED_RE.match(text)
    if m:
        marker = text.split()[0] if text.split() else ""
        return False, True, f"decimal:{marker}"
    return False, False, None


def _build_runs(spans: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert fitz spans to run dicts, merging consecutive same-style spans."""
    if not spans:
        return []

    def _key(s: dict) -> tuple:
        flags = s.get("flags", 0)
        return (
            round(s.get("size", 0.0), 1),
            s.get("font", ""),
            bool(flags & _FLAG_BOLD),
            bool(flags & _FLAG_ITALIC),
            s.get("color", 0),
        )

    runs: list[dict[str, Any]] = []
    run_idx = 0
    i = 0
    while i < len(spans):
        key = _key(spans[i])
        j = i + 1
        while j < len(spans) and _key(spans[j]) == key:
            j += 1

        # Merge spans[i:j] into one run
        merged_text = " ".join(
            s.get("text", "").strip() for s in spans[i:j]
            if s.get("text", "").strip()
        )
        if not merged_text:
            i = j
            continue

        s0 = spans[i]
        flags = s0.get("flags", 0)
        color_int = s0.get("color", 0)

        runs.append({
            "index": run_idx,
            "text": merged_text,
            "bold": bool(flags & _FLAG_BOLD),
            "italic": bool(flags & _FLAG_ITALIC),
            "underline": None,          # fitz doesn't expose underline in spans
            "strikethrough": None,
            "font_name": s0.get("font") or None,
            "font_size_pt": round(s0.get("size", 0.0), 2) or None,
            "color_rgb": _int_to_rgb_hex(color_int) if color_int else None,
            "embedded_media": [],
        })
        run_idx += 1
        i = j

    return runs


def _int_to_rgb_hex(color_int: int) -> str:
    """Convert fitz integer color (0xRRGGBB) to 'RRGGBB' hex string."""
    r = (color_int >> 16) & 0xFF
    g = (color_int >> 8) & 0xFF
    b = color_int & 0xFF
    return f"{r:02X}{g:02X}{b:02X}"


def _plumber_table_to_dict(
    table: Any,
    index: int,
    page_index: int,
    bbox: tuple[float, float, float, float] | None = None,
) -> dict[str, Any]:
    """Convert a pdfplumber Table to an ExtractedTable-compatible dict."""
    try:
        raw_rows: list[list[str | None]] = table.extract()
    except (ValueError, TypeError, AttributeError, KeyError):
        raw_rows = []

    if not raw_rows:
        return {
            "index": index,
            "row_count": 0,
            "column_count": 0,
            "style": None,
            "rows": [],
        }

    rows: list[dict[str, Any]] = []
    for r_idx, row in enumerate(raw_rows):
        cells: list[dict[str, Any]] = []
        for cell_val in row:
            cell_text = (cell_val or "").strip()
            para: dict[str, Any] = {
                "index": 0,
                "text": cell_text,
                "style": "Normal",
                "alignment": None,
                "direction": "ltr",
                "is_bullet": False,
                "is_numbered": False,
                "list_info": None,
                "numbering_format": None,
                "list_level": None,
                "runs": (
                    [{
                        "index": 0,
                        "text": cell_text,
                        "bold": None,
                        "italic": None,
                        "underline": None,
                        "strikethrough": None,
                        "font_name": None,
                        "font_size_pt": None,
                        "color_rgb": None,
                        "embedded_media": [],
                    }]
                    if cell_text else []
                ),
            }
            cells.append({
                "text": cell_text,
                "paragraphs": [para] if cell_text else [],
            })
        rows.append({"cells": cells, "row_index": r_idx})

    col_count = max((len(r) for r in raw_rows), default=0)
    return {
        "index": index,
        "row_count": len(raw_rows),
        "column_count": col_count,
        "style": None,
        "page_index": page_index,
        "bbox": bbox,
        "rows": rows,
    }


def _extract_image(
    doc: fitz.Document, xref: int, page_num: int
) -> dict[str, Any] | None:
    """Extract a single image by xref.  Returns None on any failure."""
    try:
        img = doc.extract_image(xref)
        if not img or not img.get("image"):
            return None
        ext = img.get("ext", "png")
        blob: bytes = img["image"]
        return {
            "relationship_id": f"pdf_xref_{xref}",
            "content_type": f"image/{ext}",
            "file_name": f"pdf_image_p{page_num + 1}_{xref}.{ext}",
            "page_index": page_num,
            "local_file_path": None,
            "local_url": None,
            "width_emu": img.get("width"),
            "height_emu": img.get("height"),
            "alt_text": None,
            "base64": base64.b64encode(blob).decode("ascii"),
        }
    except (RuntimeError, ValueError, TypeError, KeyError) as exc:
        logger.debug("Failed to extract image xref=%d: %s", xref, exc)
        return None


def _extract_document_defaults(all_font_sizes: list[float]) -> dict[str, Any]:
    """Build document_defaults from font analysis."""
    body_size = _detect_body_font_size(all_font_sizes)
    return {
        "font_name": None,
        "font_size_pt": body_size,
        "color_rgb": None,
    }


def _iter_document_spans(
    doc: fitz.Document,
    flags: int | None = None,
    max_pages: int | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield text spans across pages, skipping unreadable pages.

    If max_pages is set, only the first N pages are iterated.
    """
    pages = list(doc)[:max_pages] if max_pages is not None else doc
    for page in pages:
        raw = _safe_get_page_text_dict(page, flags)
        yield from _iter_spans_from_page_dict(raw)


def _safe_get_page_text_dict(
    page: fitz.Page,
    flags: int | None = None,
) -> dict[str, Any]:
    """Safely read a page text dictionary; returns empty dict on parser errors."""
    try:
        if flags is None:
            return page.get_text("dict")
        return page.get_text("dict", flags=flags)
    except (RuntimeError, ValueError, TypeError, KeyError):
        return {}


def _iter_spans_from_page_dict(raw: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield spans from text blocks in a fitz page dict."""
    for block in raw.get("blocks", []):
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                yield span
