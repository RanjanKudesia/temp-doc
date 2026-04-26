"""DOCX extraction pipeline — chunking-optimised, raw zipfile + lxml implementation.

Uses Python's built-in zipfile module and lxml directly — no python-docx.
A .docx file is a ZIP archive; main content lives at word/document.xml.
Styles are in word/styles.xml; list numbering is in word/numbering.xml.

Advantages over python-docx:
- No eager object-wrapping for every element
- No run/font XML reads (not needed for chunking)
- Generator-friendly single pass over the body
- Style map loaded once from styles.xml (no per-paragraph style tree walk)
- Numbering root loaded once; results cached by (numId, ilvl)
"""

import logging
import time
from io import BytesIO
from typing import Any
from zipfile import ZipFile, is_zipfile

from lxml import etree

# ── WordprocessingML namespace ────────────────────────────────────────────────
_W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
_NS = {"w": _W}


def _w(tag: str) -> str:
    """Return the Clark-notation qualified name for a w: tag."""
    return f"{{{_W}}}{tag}"


class DocxExtractionPipeline:
    """Extract paragraphs and tables from DOCX files (chunking-optimised).

    Uses zipfile + lxml only. Returns a dict compatible with ExtractedData schema.
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    # ── Public ────────────────────────────────────────────────────────────────

    def run(self, file_bytes: bytes, include_media: bool = True) -> dict[str, Any]:
        """Parse a DOCX byte stream and return extracted JSON payload."""
        t0 = time.perf_counter()
        self.logger.info(
            "DOCX extraction started",
            extra={"file_size_bytes": len(file_bytes)},
        )

        if not is_zipfile(BytesIO(file_bytes)):
            raise ValueError(
                "Invalid DOCX file: not a valid ZIP archive. File may be corrupted.")

        try:
            result = self._extract(file_bytes)
        except (KeyError, etree.XMLSyntaxError, ValueError, OSError) as e:
            raise ValueError(f"Failed to parse DOCX document: {e}") from e

        elapsed_ms = round((time.perf_counter() - t0) * 1000)
        self.logger.info(
            "DOCX extraction complete",
            extra={
                "elapsed_ms": elapsed_ms,
                "elapsed_s": round(elapsed_ms / 1000, 3),
                "paragraphs_extracted": len(result["paragraphs"]),
                "tables_extracted": len(result["tables"]),
            },
        )
        return result

    # ── Core extraction ───────────────────────────────────────────────────────

    def _extract(self, file_bytes: bytes) -> dict[str, Any]:
        """Open the ZIP, parse XMLs, walk the body in a single pass."""
        with ZipFile(BytesIO(file_bytes)) as zf:
            doc_xml = zf.read("word/document.xml")
            style_map = self._load_styles(zf)
            numbering_root = self._load_numbering(zf)

        # Numbering cache is local to this extraction — avoids re-walking XML
        # for every list paragraph.
        numbering_cache: dict[tuple[int, int], str | None] = {}

        root_el = etree.fromstring(doc_xml)
        body = root_el.find(f".//{_w('body')}")
        if body is None:
            raise ValueError("No <w:body> element found in word/document.xml")

        paragraphs: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []
        document_order: list[dict[str, Any]] = []
        para_idx = 0
        tbl_idx = 0

        for child in body:
            local = child.tag.rsplit(
                "}", 1)[-1] if "}" in child.tag else child.tag

            if local == "p":
                p = self._extract_paragraph(
                    child, para_idx, style_map, numbering_root, numbering_cache
                )
                if not p["text"].strip():
                    continue
                paragraphs.append(p)
                document_order.append({"type": "paragraph", "index": para_idx})
                para_idx += 1

            elif local == "tbl":
                t = self._extract_table(child, tbl_idx)
                tables.append(t)
                document_order.append({"type": "table", "index": tbl_idx})
                tbl_idx += 1

        self.logger.info(
            "DOCX document structure",
            extra={"paragraphs": para_idx, "tables": tbl_idx},
        )

        return {
            "document_order": document_order,
            "document_defaults": None,   # not needed for chunking
            "styles": [],                # not needed for chunking
            "paragraphs": paragraphs,
            "tables": tables,
            "media": [],                 # not needed for chunking
        }

    # ── ZIP helpers ───────────────────────────────────────────────────────────

    def _load_styles(self, zf: ZipFile) -> dict[str, str]:
        """Return {styleId: displayName} from word/styles.xml (best-effort)."""
        style_map: dict[str, str] = {}
        try:
            styles_xml = zf.read("word/styles.xml")
            root = etree.fromstring(styles_xml)
            for style_el in root.findall(_w("style")):
                style_id = style_el.get(_w("styleId"))
                name_el = style_el.find(_w("name"))
                if style_id and name_el is not None:
                    name_val = name_el.get(_w("val")) or ""
                    if name_val:
                        style_map[style_id] = name_val
        except (KeyError, etree.XMLSyntaxError, AttributeError):
            pass
        return style_map

    def _load_numbering(self, zf: ZipFile) -> etree._Element | None:
        """Parse word/numbering.xml and return its root, or None if absent."""
        try:
            num_xml = zf.read("word/numbering.xml")
            return etree.fromstring(num_xml)
        except (KeyError, etree.XMLSyntaxError):
            return None

    # ── Paragraph ─────────────────────────────────────────────────────────────

    def _extract_paragraph(
        self,
        p_el: etree._Element,
        index: int,
        style_map: dict[str, str],
        numbering_root: etree._Element | None,
        numbering_cache: dict[tuple[int, int], str | None],
    ) -> dict[str, Any]:
        """Extract paragraph text, style, and list metadata. No run/font data."""
        # Concatenate all <w:t> text — preserves spaces correctly
        text = "".join(t_el.text or "" for t_el in p_el.iter(_w("t")))

        # Style and list info live inside <w:pPr>
        style_name: str | None = None
        list_info: dict[str, Any] | None = None

        p_pr = p_el.find(_w("pPr"))
        if p_pr is not None:
            # Style
            p_style_el = p_pr.find(_w("pStyle"))
            if p_style_el is not None:
                style_id = p_style_el.get(_w("val"), "")
                style_name = style_map.get(style_id, style_id) or None

            # List numbering
            num_pr = p_pr.find(_w("numPr"))
            if num_pr is not None:
                num_id_el = num_pr.find(_w("numId"))
                ilvl_el = num_pr.find(_w("ilvl"))
                num_id_val = num_id_el.get(
                    _w("val")) if num_id_el is not None else None
                ilvl_val = ilvl_el.get(
                    _w("val")) if ilvl_el is not None else None
                if num_id_val is not None:
                    list_info = {
                        "num_id": int(num_id_val),
                        "level": int(ilvl_val) if ilvl_val is not None else 0,
                    }

        numbering_format = self._resolve_list_formatting(
            list_info, numbering_root, numbering_cache
        )
        is_bullet, is_numbered = self._get_list_format_flags(
            style_name, numbering_format)

        return {
            "index": index,
            "text": text,
            "style": style_name,
            "is_bullet": is_bullet or list_info is not None,
            "is_numbered": is_numbered,
            "list_info": list_info,
            "numbering_format": numbering_format,
            "list_level": list_info.get("level") if list_info else None,
            "runs": [],  # not needed for chunking
        }

    def _get_list_format_flags(
        self, style_name: str | None, numbering_format: str | None
    ) -> tuple[bool, bool]:
        """Return (is_bullet, is_numbered) from style name and resolved format."""
        is_bullet = bool(style_name and "bullet" in style_name.lower())
        is_numbered = bool(style_name and "number" in style_name.lower())
        if numbering_format:
            fmt = numbering_format.split(":", 1)[0].lower()
            if fmt == "bullet":
                is_bullet = True
            else:
                is_numbered = True
        return is_bullet, is_numbered

    # ── Numbering resolution ──────────────────────────────────────────────────

    def _resolve_list_formatting(
        self,
        list_info: dict[str, Any] | None,
        numbering_root: etree._Element | None,
        cache: dict[tuple[int, int], str | None],
    ) -> str | None:
        """Return abstract list format string, cached by (numId, ilvl)."""
        if not list_info or numbering_root is None:
            return None
        num_id = list_info.get("num_id")
        ilvl = list_info.get("level") or 0
        if num_id is None:
            return None

        cache_key = (num_id, ilvl)
        if cache_key in cache:
            return cache[cache_key]

        try:
            result = self._lookup_list_format(numbering_root, num_id, ilvl)
        except (AttributeError, ValueError, TypeError, KeyError):
            result = None

        cache[cache_key] = result
        return result

    def _lookup_list_format(
        self, root: etree._Element, num_id: int, ilvl: int
    ) -> str | None:
        """Walk numbering.xml to resolve numId + ilvl → format string."""
        num_id_str = str(num_id)
        ilvl_str = str(ilvl)

        # Step 1: numId → abstractNumId
        abstract_id_str: str | None = None
        for num_el in root.findall(_w("num")):
            if num_el.get(_w("numId")) == num_id_str:
                abstract_el = num_el.find(_w("abstractNumId"))
                if abstract_el is not None:
                    abstract_id_str = abstract_el.get(_w("val"))
                break

        if abstract_id_str is None:
            return None

        # Step 2: abstractNumId + ilvl → numFmt + lvlText
        for abs_num in root.findall(_w("abstractNum")):
            if abs_num.get(_w("abstractNumId")) != abstract_id_str:
                continue
            for lvl in abs_num.findall(_w("lvl")):
                if lvl.get(_w("ilvl")) != ilvl_str:
                    continue
                num_fmt_el = lvl.find(_w("numFmt"))
                lvl_text_el = lvl.find(_w("lvlText"))
                if num_fmt_el is not None and lvl_text_el is not None:
                    fmt = num_fmt_el.get(_w("val"), "")
                    text = lvl_text_el.get(_w("val"), "")
                    if fmt and text:
                        return f"{fmt}:{text}"
                return None  # found the level but missing format elements

        return None

    # ── Table ─────────────────────────────────────────────────────────────────

    def _extract_table(self, tbl_el: etree._Element, index: int) -> dict[str, Any]:
        """Extract table cell text — no run data, no cell paragraph objects."""
        rows: list[dict[str, Any]] = []
        max_cols = 0

        for row_idx, row_el in enumerate(tbl_el.findall(_w("tr"))):
            cells: list[dict[str, Any]] = []
            for cell_el in row_el.findall(_w("tc")):
                parts = []
                for p_el in cell_el.findall(_w("p")):
                    cell_text = "".join(
                        t_el.text or "" for t_el in p_el.iter(_w("t"))
                    )
                    if cell_text:
                        parts.append(cell_text)
                cells.append({"text": "\n".join(parts), "paragraphs": []})
            max_cols = max(max_cols, len(cells))
            rows.append({"cells": cells, "row_index": row_idx})

        return {
            "index": index,
            "row_count": len(rows),
            "column_count": max_cols,
            "style": None,  # not needed for chunking
            "rows": rows,
        }
