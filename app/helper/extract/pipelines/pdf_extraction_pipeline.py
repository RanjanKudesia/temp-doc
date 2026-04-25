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
import json
import logging
import re
import time
from collections import Counter
from io import BytesIO
from typing import Any

import fitz  # PyMuPDF
import pdfplumber

from app.helper.extract.pipelines.pdf_conversion_pipeline import (
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
        if doc.is_encrypted:
            doc.close()
            raise ValueError(
                "PDF is password-protected. Provide an unlocked PDF."
            )

        # ── Scanned PDF detection ────────────────────────────────────────────
        total_chars = sum(
            len(doc[i].get_text("text").strip())
            for i in range(len(doc))
        )
        if total_chars < _MIN_TEXT_CHARS:
            doc.close()
            logger.info(
                "PDF has very little text (%d chars). "
                "Falling back to pdf2docx conversion.",
                total_chars,
            )
            return self._fallback.run(file_bytes, include_media=include_media)

        # ── Native extraction ────────────────────────────────────────────────
        try:
            result = self._extract_native(doc, file_bytes, include_media)
        finally:
            doc.close()

        elapsed_ms = round((time.perf_counter() - t0) * 1000)
        response_size_bytes = len(json.dumps(result).encode("utf-8"))
        logger.info(
            "PDF extraction complete",
            extra={
                "elapsed_ms": elapsed_ms,
                "elapsed_s": round(elapsed_ms / 1000, 3),
                "response_size_bytes": response_size_bytes,
                "response_size_kb": round(response_size_bytes / 1024, 2),
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

        # ── Pass 1: collect font sizes for heading heuristic ────────────────
        all_font_sizes: list[float] = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            raw = page.get_text(
                "dict",
                flags=fitz.TEXT_PRESERVE_WHITESPACE,
            )
            for block in raw.get("blocks", []):
                if block.get("type") != 0:
                    continue
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        sz = span.get("size", 0.0)
                        if sz and sz > 0:
                            all_font_sizes.append(round(sz, 1))

        body_font_size = _detect_body_font_size(all_font_sizes)
        heading_size_map = _build_heading_size_map(
            all_font_sizes, body_font_size)

        # ── Pass 2: extract images (xref-based, cross-page dedup) ───────────
        media: list[dict[str, Any]] = []
        seen_xrefs: set[int] = set()
        if include_media:
            for page_num in range(len(doc)):
                page = doc[page_num]
                for img_info in page.get_images(full=True):
                    xref: int = img_info[0]
                    if xref in seen_xrefs:
                        continue
                    seen_xrefs.add(xref)
                    item = _extract_image(doc, xref, page_num)
                    if item:
                        media.append(item)

        # ── Pass 3: per-page paragraph + table extraction ───────────────────
        all_paragraphs: list[dict[str, Any]] = []
        all_tables: list[dict[str, Any]] = []
        document_order: list[dict[str, Any]] = []
        para_idx = 0
        table_idx = 0

        with pdfplumber.open(BytesIO(file_bytes)) as pdf_reader:
            plumber_pages = pdf_reader.pages
            for page_num in range(min(len(doc), len(plumber_pages))):
                fitz_page = doc[page_num]
                plumber_page = plumber_pages[page_num]

                # pdfplumber table bboxes: (x0, top, x1, bottom)
                # both fitz and pdfplumber use top-left origin on the page
                try:
                    plumber_tables = plumber_page.find_tables()
                except Exception as exc:
                    logger.debug(
                        "pdfplumber table detection failed on page %d: %s",
                        page_num, exc,
                    )
                    plumber_tables = []

                table_bboxes: list[tuple[float, float, float, float]] = [
                    t.bbox for t in plumber_tables
                ]

                # fitz text blocks
                raw = fitz_page.get_text(
                    "dict",
                    flags=(
                        fitz.TEXT_PRESERVE_WHITESPACE
                        | fitz.TEXT_PRESERVE_LIGATURES
                    ),
                )
                fitz_text_blocks = [
                    b for b in raw.get("blocks", []) if b.get("type") == 0
                ]

                # Collect (y_top, kind, data) for this page
                page_items: list[tuple[float, str, dict[str, Any]]] = []

                # Paragraphs from fitz (exclude blocks overlapping tables)
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
                        para_idx,
                        heading_size_map,
                        body_font_size,
                        page_index=page_num,
                    )
                    if para is None:
                        continue
                    page_items.append((by0, "paragraph", para))

                # Tables from pdfplumber
                for t in plumber_tables:
                    tx0, ty0, tx1, ty1 = t.bbox
                    tbl = _plumber_table_to_dict(
                        t,
                        table_idx,
                        page_index=page_num,
                        bbox=(tx0, ty0, tx1, ty1),
                    )
                    page_items.append((ty0, "table", tbl))

                # Sort by top-y for correct reading order
                page_items.sort(key=lambda x: x[0])

                for _, kind, data in page_items:
                    if kind == "paragraph":
                        data["index"] = para_idx
                        all_paragraphs.append(data)
                        document_order.append(
                            {"type": "paragraph", "index": para_idx}
                        )
                        para_idx += 1
                    else:
                        data["index"] = table_idx
                        all_tables.append(data)
                        document_order.append(
                            {"type": "table", "index": table_idx}
                        )
                        table_idx += 1

        document_defaults = _extract_document_defaults(doc, all_font_sizes)

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


def _block_to_paragraph(
    block: dict[str, Any],
    index: int,
    heading_size_map: dict[float, str],
    body_font_size: float,
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
    except Exception:
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
    except Exception as exc:
        logger.debug("Failed to extract image xref=%d: %s", xref, exc)
        return None


def _extract_document_defaults(
    doc: fitz.Document, all_font_sizes: list[float]
) -> dict[str, Any]:
    """Build document_defaults from PDF metadata + font analysis."""
    body_size = _detect_body_font_size(all_font_sizes)

    # Find the most common font name across all pages
    font_counter: Counter[str] = Counter()
    try:
        for page_num in range(len(doc)):
            page = doc[page_num]
            raw = page.get_text("dict")
            for block in raw.get("blocks", []):
                if block.get("type") != 0:
                    continue
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        fname = span.get("font")
                        if fname:
                            font_counter[fname] += 1
    except Exception:
        pass

    most_common_font: str | None = (
        font_counter.most_common(1)[0][0] if font_counter else None
    )

    return {
        "font_name": most_common_font,
        "font_size_pt": body_size,
        "color_rgb": None,
    }
