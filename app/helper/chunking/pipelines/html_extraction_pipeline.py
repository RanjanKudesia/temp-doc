"""HTML extraction pipeline for temp-doc service."""

import logging
import re
from typing import Any

from bs4 import BeautifulSoup, Doctype, NavigableString, Tag

# ── Module-level helpers ──────────────────────────────────────────────────────

_BLOCK_SKIP_TAGS = {"ul", "ol", "table", "img"}


def _normalize_text(value: str) -> str:
    if not value:
        return ""
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for line in value.split("\n"):
        cleaned = re.sub(r"\s+", " ", line).strip()
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


def _default_run(text: str) -> dict[str, Any]:
    return {
        "index": 0,
        "text": text,
        "bold": None,
        "italic": None,
        "underline": None,
        "strikethrough": None,
        "code": None,
        "font_name": None,
        "font_size_pt": None,
        "color_rgb": None,
        "highlight_color": None,
        "hyperlink_url": None,
        "embedded_media": [],
    }


def _same_style(a: dict[str, Any], b: dict[str, Any]) -> bool:
    keys = ("bold", "italic", "underline", "strikethrough", "code",
            "color_rgb", "font_name", "font_size_pt", "hyperlink_url",
            "highlight_color", "semantic_insert", "semantic_delete", "vertical_align")
    return all(a.get(k) == b.get(k) for k in keys)


def _css_color_to_rgb(raw: str) -> str | None:
    raw = raw.strip()
    if raw.startswith("#"):
        return raw
    m_rgb = re.match(
        r"rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", raw, re.I)
    if m_rgb:
        r, g, b = int(m_rgb.group(1)), int(m_rgb.group(2)), int(m_rgb.group(3))
        return f"#{r:02x}{g:02x}{b:02x}"
    if re.match(r"^[a-zA-Z]+$", raw):
        return raw
    return None


def _css_size_to_pt(raw: str) -> float | None:
    """Convert a CSS font-size value to points (best-effort)."""
    raw = raw.strip()
    m = re.match(r"([\d.]+)\s*(pt|px|em|rem)?", raw, re.I)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    unit = (m.group(2) or "pt").lower()
    if unit == "pt":
        return val
    if unit == "px":
        return round(val * 0.75, 2)
    if unit in ("em", "rem"):
        return round(val * 12.0, 2)
    return None


def _apply_tag_style_flags(name: str, style: dict[str, Any]) -> None:
    """Update style dict in-place based on semantic HTML tag name."""
    if name in {"b", "strong"}:
        style["bold"] = True
    if name in {"i", "em", "cite", "dfn", "var"}:
        style["italic"] = True
    if name == "u":
        style["underline"] = True
    if name == "ins":
        style["underline"] = True
        style["semantic_insert"] = True  # preserve ins semantics
    if name in {"s", "strike"}:
        style["strikethrough"] = True
    if name == "del":
        style["strikethrough"] = True
        style["semantic_delete"] = True  # preserve del semantics
    if name in {"code", "kbd", "samp", "tt"}:
        style["code"] = True
    if name == "mark":
        style["highlight_color"] = "yellow"
    if name in {"sub", "sup"}:
        style["vertical_align"] = name  # "sub" or "sup"
    if name == "a":
        href = (style.get("_node_href") or "").strip()
        if href:
            style["hyperlink_url"] = href


def _apply_inline_css(node: Tag, style: dict[str, Any]) -> None:
    """Update style dict in-place from element's inline CSS."""
    inline = node.get("style") or ""
    if not inline:
        return
    m_color = re.search(r"\bcolor\s*:\s*([^;]+)", inline, re.I)
    if m_color:
        style["color_rgb"] = _css_color_to_rgb(m_color.group(1).strip())
    m_font = re.search(r"\bfont-family\s*:\s*([^;]+)", inline, re.I)
    if m_font:
        font_raw = (
            m_font.group(1).strip().strip("'\"").split(
                ",")[0].strip().strip("'\"")
        )
        if font_raw:
            style["font_name"] = font_raw
    m_size = re.search(r"\bfont-size\s*:\s*([^;]+)", inline, re.I)
    if m_size:
        size_pt = _css_size_to_pt(m_size.group(1).strip())
        if size_pt is not None:
            style["font_size_pt"] = size_pt


class _RunCollector:
    """Accumulates inline runs from a parsed HTML element tree."""

    def __init__(self, *, skip_block_children: bool) -> None:
        self.runs: list[dict[str, Any]] = []
        self._skip_block = skip_block_children

    def push_text(self, text: str, style: dict[str, Any]) -> None:
        """Append or merge a text run."""
        normalized = "\n" if text == "\n" else _normalize_text(text)
        if not normalized:
            return
        run = {
            "index": len(self.runs),
            "text": normalized,
            "bold": style.get("bold"),
            "italic": style.get("italic"),
            "underline": style.get("underline"),
            "strikethrough": style.get("strikethrough"),
            "code": style.get("code"),
            "color_rgb": style.get("color_rgb"),
            "font_name": style.get("font_name"),
            "font_size_pt": style.get("font_size_pt"),
            "highlight_color": style.get("highlight_color"),
            "hyperlink_url": style.get("hyperlink_url"),
            "embedded_media": [],
            # Semantic extras — preserved as model_extra in ExtractedRun
            "semantic_insert": style.get("semantic_insert"),
            "semantic_delete": style.get("semantic_delete"),
            "vertical_align": style.get("vertical_align"),
        }
        if self.runs and _same_style(self.runs[-1], run):
            prev_text = str(self.runs[-1].get("text") or "")
            if prev_text.endswith("\n") or run["text"].startswith("\n"):
                self.runs[-1]["text"] = f"{prev_text}{run['text']}"
            else:
                self.runs[-1]["text"] = f"{prev_text} {run['text']}".strip()
        else:
            run["index"] = len(self.runs)
            self.runs.append(run)

    def collect(self, node: Any, style: dict[str, Any]) -> None:
        """Recursively walk a node tree and accumulate runs."""
        if isinstance(node, NavigableString):
            self.push_text(str(node), style)
            return
        if not isinstance(node, Tag):
            return
        name = (node.name or "").lower()
        if name in {"script", "style", "noscript"}:
            return
        if self._skip_block and name in _BLOCK_SKIP_TAGS:
            return
        if name in {"table", "ul", "ol"}:
            return
        if name == "br":
            self.push_text("\n", style)
            return
        if name == "img":
            src = (node.get("src") or "").strip()
            if src:
                w = node.get("width", "")
                h = node.get("height", "")
                media_item = {
                    "relationship_id": None,
                    "content_type": None,
                    "file_name": src.split("/")[-1],
                    "local_file_path": src,
                    "local_url": src,
                    "width_emu": int(w) if str(w).isdigit() else None,
                    "height_emu": int(h) if str(h).isdigit() else None,
                    "alt_text": (node.get("alt") or "").strip() or None,
                }
                if self.runs:
                    self.runs[-1]["embedded_media"].append(media_item)
                else:
                    placeholder = _default_run("")
                    placeholder["embedded_media"] = [media_item]
                    self.runs.append(placeholder)
            return
        next_style = dict(style)
        if name == "a":
            next_style["_node_href"] = (node.get("href") or "").strip()
        _apply_tag_style_flags(name, next_style)
        next_style.pop("_node_href", None)
        _apply_inline_css(node, next_style)
        for sub in node.children:
            self.collect(sub, next_style)


def _collect_runs(element: Tag, *, skip_block_children: bool) -> list[dict[str, Any]]:
    collector = _RunCollector(skip_block_children=skip_block_children)
    collector.collect(element, {
        "bold": None, "italic": None, "underline": None,
        "strikethrough": None, "code": None,
        "color_rgb": None, "font_name": None,
        "font_size_pt": None, "hyperlink_url": None,
    })
    for idx, run in enumerate(collector.runs):
        run["index"] = idx
    return collector.runs


def _extract_runs(element: Tag) -> list[dict[str, Any]]:
    return _collect_runs(element, skip_block_children=False)


def _extract_runs_skip_blocks(element: Tag) -> list[dict[str, Any]]:
    return _collect_runs(element, skip_block_children=True)


# ── Extraction state ──────────────────────────────────────────────────────────

_RTL_BLOCK_SET = {"rtl"}
_ARABIC_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]")
_DIRECTION_RE = re.compile(r"\bdirection\s*:\s*(\w+)", re.I)


def _is_element_rtl(elem: Tag) -> bool:
    """Return True if the element has or implies RTL text direction."""
    elem_dir = (elem.get("dir") or "").lower()
    m_dir = _DIRECTION_RE.search(elem.get("style") or "")
    has_arabic = bool(_ARABIC_RE.search(elem.get_text()))
    return (
        elem_dir in _RTL_BLOCK_SET
        or (m_dir is not None and m_dir.group(1).lower() in _RTL_BLOCK_SET)
        or has_arabic
    )


class _ExtractionState:
    """Holds mutable state for a single HTML extraction run."""

    def __init__(self, include_media: bool = True) -> None:
        self.paragraphs: list[dict[str, Any]] = []
        self.tables: list[dict[str, Any]] = []
        self.media: list[dict[str, Any]] = []
        self.document_order: list[dict[str, Any]] = []
        self.seen_tables: set[int] = set()
        self.paragraph_index: int = 0
        self.table_index: int = 0
        self.media_index: int = 0
        self.include_media: bool = include_media


# ── Pipeline ──────────────────────────────────────────────────────────────────


class HtmlExtractionPipeline:
    """Extract HTML content to JSON format."""

    _BLOCK_TAGS = {
        "address", "article", "aside", "blockquote", "details", "dialog",
        "div", "dl", "fieldset", "figcaption", "figure", "footer", "form",
        "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "li", "main",
        "nav", "ol", "p", "pre", "section", "table", "ul",
    }
    _RTL_VALUES = {"rtl"}

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    # ── Public entry point ────────────────────────────────────────────────────

    def run(self, file_bytes: bytes, include_media: bool = True) -> dict[str, Any]:
        """Extract HTML and return JSON data."""
        try:
            html = file_bytes.decode("utf-8-sig", errors="replace")
        except Exception as e:
            raise ValueError(f"Failed to decode HTML: {str(e)}") from e

        soup = BeautifulSoup(html, "lxml")
        root = soup.body or soup

        state = _ExtractionState(include_media=include_media)
        self._walk(state, root)

        metadata = self._build_metadata(soup, root, html)

        return {
            "metadata": metadata,
            "document_order": state.document_order,
            "document_defaults": None,
            "styles": [],
            "paragraphs": state.paragraphs,
            "tables": state.tables,
            "media": state.media,
        }

    def _build_metadata(self, soup: BeautifulSoup, root: Tag, raw_html: str) -> dict[str, Any]:
        """Capture full-fidelity HTML metadata for downstream generation."""
        title_tag = soup.title
        title_text = (title_tag.string.strip()
                      if title_tag and title_tag.string else None)

        html_tag = soup.html if isinstance(soup.html, Tag) else None
        head_tag = soup.head if isinstance(soup.head, Tag) else None

        doctype = self._extract_doctype(soup)
        style_blocks, link_tags, meta_tags, script_blocks = self._extract_head_assets(
            head_tag)

        return {
            "source_type": "html",
            "extraction_mode": "html",
            "title": title_text,
            "doctype": doctype,
            "full_html": raw_html,
            "head_html": str(head_tag) if head_tag is not None else None,
            "body_html": str(root) if isinstance(root, Tag) else None,
            "html_attributes": self._tag_attrs(html_tag),
            "body_attributes": self._tag_attrs(root),
            "style_blocks": style_blocks,
            "meta_tags": meta_tags,
            "link_tags": link_tags,
            "script_blocks": script_blocks,
        }

    def _extract_head_assets(
        self,
        head_tag: Tag | None,
    ) -> tuple[list[str], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """Extract style/link/meta/script data from the head tag."""
        style_blocks: list[str] = []
        link_tags: list[dict[str, Any]] = []
        meta_tags: list[dict[str, Any]] = []
        script_blocks: list[dict[str, Any]] = []

        if head_tag is None:
            return style_blocks, link_tags, meta_tags, script_blocks

        for child in head_tag.find_all(recursive=False):
            if not isinstance(child, Tag):
                continue
            self._collect_head_child_asset(
                child,
                style_blocks,
                link_tags,
                meta_tags,
                script_blocks,
            )

        return style_blocks, link_tags, meta_tags, script_blocks

    def _collect_head_child_asset(
        self,
        child: Tag,
        style_blocks: list[str],
        link_tags: list[dict[str, Any]],
        meta_tags: list[dict[str, Any]],
        script_blocks: list[dict[str, Any]],
    ) -> None:
        """Add one head child into metadata asset collections."""
        name = child.name.lower()
        if name == "style":
            style_blocks.append(child.get_text() or "")
            return
        if name == "link":
            link_tags.append(self._tag_attrs(child))
            return
        if name == "meta":
            meta_tags.append(self._tag_attrs(child))
            return
        if name == "script":
            script_blocks.append(
                {
                    "attrs": self._tag_attrs(child),
                    "content": child.get_text() or "",
                }
            )

    def _extract_doctype(self, soup: BeautifulSoup) -> str | None:
        """Extract doctype text when present."""
        for item in soup.contents:
            if isinstance(item, Doctype):
                return f"<!DOCTYPE {str(item)}>"
        return None

    def _tag_attrs(self, tag: Tag | None) -> dict[str, Any]:
        """Convert tag attributes to JSON-safe values."""
        if tag is None:
            return {}

        attrs: dict[str, Any] = {}
        for key, value in tag.attrs.items():
            if isinstance(value, list):
                attrs[key] = " ".join(str(v) for v in value)
            else:
                attrs[key] = value
        return attrs

    def _tag_source(self, tag: Tag) -> dict[str, Any]:
        """Capture source snippet and tag metadata for a node."""
        return {
            "format": "html",
            "tag": tag.name.lower() if tag.name else None,
            "attrs": self._tag_attrs(tag),
            "raw_html": str(tag),
        }

    # ── Direction detection ───────────────────────────────────────────────────

    def _detect_direction(
        self,
        elem: Tag,
        text: str,
        direction: str | None,
    ) -> str | None:
        if direction:
            return direction
        elem_dir = (elem.get("dir") or "").lower()
        inline_style = elem.get("style") or ""
        m_dir = re.search(r"\bdirection\s*:\s*(\w+)", inline_style, re.I)
        if (
            elem_dir in self._RTL_VALUES
            or (m_dir and m_dir.group(1).lower() in self._RTL_VALUES)
            or re.search(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", text)
        ):
            return "rtl"
        return None

    # ── Paragraph helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _make_list_info(
        is_bullet: bool,
        is_numbered: bool,
        numbering_format: str | None,
        list_level: int,
        list_start: int | None,
    ) -> dict[str, Any] | None:
        if not (is_bullet or is_numbered):
            return None
        kind = "bullet" if is_bullet else "numbered"
        return {
            "kind": kind,
            "numbering_format": numbering_format,
            "level": list_level,
            "start": list_start,
        }

    def _add_paragraph(
        self,
        state: _ExtractionState,
        *,
        text: str,
        runs: list[dict[str, Any]],
        style: str | None = None,
        is_bullet: bool = False,
        is_numbered: bool = False,
        numbering_format: str | None = None,
        list_level: int = 0,
        list_start: int | None = None,
        alignment: str | None = None,
        direction: str | None = None,
        source: dict[str, Any] | None = None,
    ) -> None:
        list_info = self._make_list_info(
            is_bullet, is_numbered, numbering_format, list_level, list_start
        )
        source_payload = {"format": "html"}
        if isinstance(source, dict):
            source_payload.update(source)

        state.paragraphs.append({
            "index": state.paragraph_index,
            "text": text,
            "style": style,
            "is_bullet": is_bullet,
            "is_numbered": is_numbered,
            "list_info": list_info,
            "numbering_format": numbering_format,
            "list_level": list_level if (is_bullet or is_numbered) else None,
            "alignment": alignment,
            "direction": direction,
            "runs": runs if runs else [_default_run(text)],
            "source": source_payload,
        })
        state.document_order.append(
            {"type": "paragraph", "index": state.paragraph_index}
        )
        state.paragraph_index += 1

    def _add_paragraph_from_element(
        self,
        state: _ExtractionState,
        elem: Tag,
        *,
        heading_level: int | None = None,
        is_bullet: bool = False,
        is_numbered: bool = False,
        numbering_format: str | None = None,
        list_level: int = 0,
        list_start: int | None = None,
        direction: str | None = None,
        override_style: str | None = None,
    ) -> None:
        runs = _extract_runs(elem)
        text = "".join((run.get("text") or "") for run in runs).strip()
        if not text:
            return
        style = override_style if override_style is not None else (
            f"Heading {heading_level}" if heading_level else None
        )
        direction = self._detect_direction(elem, text, direction)
        self._add_paragraph(
            state,
            text=text,
            runs=runs,
            style=style,
            is_bullet=is_bullet,
            is_numbered=is_numbered,
            numbering_format=numbering_format,
            list_level=list_level,
            list_start=list_start,
            direction=direction,
            source=self._tag_source(elem),
        )

    def _add_paragraph_direct(
        self,
        state: _ExtractionState,
        runs: list[dict[str, Any]],
        text: str,
    ) -> None:
        if not text:
            return
        self._add_paragraph(state, text=text, runs=runs)

    def _add_horizontal_rule(self, state: _ExtractionState) -> None:
        self._add_paragraph(
            state,
            text="---",
            runs=[_default_run("---")],
            style="HorizontalRule",
            alignment="CENTER",
        )

    # ── Media ─────────────────────────────────────────────────────────────────

    def _add_media(self, state: _ExtractionState, image_elem: Tag) -> None:
        if not state.include_media:
            return
        src = (image_elem.get("src") or "").strip()
        if not src:
            return
        w = image_elem.get("width", "")
        h = image_elem.get("height", "")
        state.media.append({
            "relationship_id": f"html_img_{state.media_index}",
            "content_type": None,
            "file_name": src.split("/")[-1],
            "local_file_path": src,
            "local_url": src,
            "width_emu": int(w) if str(w).isdigit() else None,
            "height_emu": int(h) if str(h).isdigit() else None,
            "alt_text": (image_elem.get("alt") or "").strip() or None,
        })
        state.document_order.append(
            {"type": "media", "index": state.media_index})
        state.media_index += 1

    # ── Table ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_cell_span(cell: Tag) -> tuple[int, int]:
        """Return (colspan, rowspan) for a table cell."""
        try:
            cs = int(cell.get("colspan", 1) or 1)
        except (ValueError, TypeError):
            cs = 1
        try:
            rs = int(cell.get("rowspan", 1) or 1)
        except (ValueError, TypeError):
            rs = 1
        return cs, rs

    def _build_cell(
        self,
        cell: Tag,
        col_index: int,
        table_elem: Tag,
    ) -> dict[str, Any]:
        cell_text = _normalize_text(cell.get_text(" ", strip=True))
        cell_runs = _extract_runs(cell)
        cs, rs = self._parse_cell_span(cell)
        nested_table_elems = [
            nt for nt in cell.find_all("table")
            if nt.find_parent("table") is table_elem
        ]
        return {
            "text": cell_text,
            "paragraphs": [{
                "index": 0,
                "text": cell_text,
                "style": None,
                "is_bullet": False,
                "is_numbered": False,
                "list_info": None,
                "numbering_format": None,
                "alignment": None,
                "runs": cell_runs if cell_runs else [_default_run(cell_text)],
            }],
            "tables": [],
            "cell_index": col_index,
            "is_header": cell.name == "th",
            "colspan": cs,
            "rowspan": rs,
            "_nested_elems": nested_table_elems,
            "nested_table_indices": [],
            "source": self._tag_source(cell),
        }

    def _add_table(self, state: _ExtractionState, table_elem: Tag) -> None:
        table_id = id(table_elem)
        if table_id in state.seen_tables:
            return
        state.seen_tables.add(table_id)

        # Emit <caption> as a TableCaption paragraph before the table
        caption_tag = next(
            (c for c in table_elem.children
             if isinstance(c, Tag) and c.name.lower() == "caption"),
            None,
        )
        if caption_tag is not None:
            caption_text = _normalize_text(
                caption_tag.get_text(" ", strip=True))
            if caption_text:
                caption_runs = _extract_runs(caption_tag)
                self._add_paragraph(
                    state,
                    text=caption_text,
                    runs=caption_runs if caption_runs else [
                        _default_run(caption_text)],
                    style="TableCaption",
                    source=self._tag_source(caption_tag),
                )

        table_rows: list[dict[str, Any]] = []
        max_cols = 0

        direct_rows = [
            tr for tr in table_elem.find_all("tr")
            if tr.find_parent("table") is table_elem
        ]

        for row_index, tr in enumerate(direct_rows):
            cell_tags = [
                cell for cell in tr.find_all(["th", "td"])
                if cell.find_parent("table") is table_elem
            ]
            if not cell_tags:
                continue
            cells = [
                self._build_cell(cell, col_index, table_elem)
                for col_index, cell in enumerate(cell_tags)
            ]
            max_cols = max(max_cols, len(cells))
            table_rows.append({"row_index": row_index, "cells": cells})

        if not table_rows:
            return

        current_index = state.table_index
        state.tables.append({
            "index": current_index,
            "row_count": len(table_rows),
            "column_count": max_cols,
            "style": None,
            "rows": table_rows,
            "source": self._tag_source(table_elem),
        })
        state.document_order.append({"type": "table", "index": current_index})
        state.table_index += 1

        for row in table_rows:
            for cell in row["cells"]:
                nested_elems = cell.pop("_nested_elems", [])
                for nt in nested_elems:
                    pre_idx = state.table_index
                    self._add_table(state, nt)
                    if state.table_index > pre_idx:
                        cell["nested_table_indices"].append(pre_idx)

    # ── List ──────────────────────────────────────────────────────────────────

    def _walk_list_item_children(
        self, state: _ExtractionState, li: Tag, *, list_level: int
    ) -> None:
        """Process nested block-level children of a list item."""
        for child in li.children:
            if not isinstance(child, Tag):
                continue
            cname = child.name.lower()
            if cname in {"ul", "ol"}:
                self._walk_list(state, child, list_level=list_level + 1)
            elif cname == "table":
                self._add_table(state, child)
            elif cname == "img":
                self._add_media(state, child)
            elif cname == "figcaption":
                self._add_paragraph_from_element(
                    state, child, override_style="Caption")
            elif cname in {
                "div", "section", "article", "main", "header",
                "footer", "aside", "nav", "figure",
            }:
                self._walk_div_like(state, child)

    def _walk_list(
        self,
        state: _ExtractionState,
        list_elem: Tag,
        *,
        list_level: int = 0,
    ) -> None:
        is_bullet = list_elem.name.lower() == "ul"
        try:
            start = int(list_elem.get("start", 1) or 1) if not is_bullet else 1
        except (ValueError, TypeError):
            start = 1
        current_number = start

        for li in list_elem.find_all("li", recursive=False):
            numbering_format = "bullet" if is_bullet else f"{current_number}."
            self._add_paragraph_from_element(
                state,
                li,
                is_bullet=is_bullet,
                is_numbered=not is_bullet,
                numbering_format=numbering_format,
                list_level=list_level,
                list_start=start if not is_bullet else None,
            )
            self._walk_list_item_children(state, li, list_level=list_level)
            current_number += 1

    # ── Definition list walker ────────────────────────────────────────────────

    def _walk_definition_list(self, state: _ExtractionState, dl_elem: Tag) -> None:
        """Extract <dl> as alternating DefinitionTerm / DefinitionData paragraphs."""
        for child in dl_elem.children:
            if not isinstance(child, Tag):
                continue
            cname = child.name.lower()
            if cname == "dt":
                self._add_paragraph_from_element(
                    state, child, override_style="DefinitionTerm")
            elif cname == "dd":
                self._add_paragraph_from_element(
                    state, child, override_style="DefinitionData")
            elif cname in {"ul", "ol"}:
                self._walk_list(state, child, list_level=0)
            elif cname == "table":
                self._add_table(state, child)

    # ── Fieldset walker ───────────────────────────────────────────────────────

    def _walk_fieldset(self, state: _ExtractionState, fieldset_elem: Tag) -> None:
        """Walk a <fieldset> treating <legend> as a heading and form controls as text."""
        for child in fieldset_elem.children:
            if not isinstance(child, Tag):
                continue
            cname = child.name.lower()
            if cname == "legend":
                self._add_paragraph_from_element(
                    state, child, override_style="Legend")
            elif cname in {"label", "button", "textarea"}:
                self._add_paragraph_from_element(state, child)
            elif cname == "input":
                # Use placeholder or value as surrogate text for input fields.
                raw = (
                    child.get("placeholder")
                    or child.get("value")
                    or child.get("type")
                    or ""
                )
                text = _normalize_text(str(raw))
                if text:
                    self._add_paragraph(
                        state, text=text,
                        runs=[_default_run(text)],
                        source=self._tag_source(child),
                    )
            elif cname == "select":
                self._walk_div_like(state, child)
            elif cname in {"ul", "ol"}:
                self._walk_list(state, child, list_level=0)
            elif cname == "table":
                self._add_table(state, child)
            elif cname == "fieldset":
                self._walk_fieldset(state, child)
            else:
                txt = _normalize_text(child.get_text(" ", strip=True))
                if txt:
                    self._add_paragraph_from_element(state, child)

    # ── Walkers ───────────────────────────────────────────────────────────────

    def _walk_paragraph(self, state: _ExtractionState, child: Tag) -> None:
        block_children = [
            c for c in child.children
            if isinstance(c, Tag) and c.name.lower() in {"ul", "ol", "table", "img"}
        ]
        if block_children:
            text_only_runs = _extract_runs_skip_blocks(child)
            text_only = "".join((r.get("text") or "")
                                for r in text_only_runs).strip()
            if text_only:
                self._add_paragraph_direct(state, text_only_runs, text_only)
            for bc in block_children:
                bcname = bc.name.lower()
                if bcname in {"ul", "ol"}:
                    self._walk_list(state, bc, list_level=0)
                elif bcname == "table":
                    self._add_table(state, bc)
                elif bcname == "img":
                    self._add_media(state, bc)
        else:
            self._add_paragraph_from_element(state, child)

    def _walk_div_like(self, state: _ExtractionState, child: Tag) -> None:
        has_block_child = any(
            isinstance(grand, Tag) and grand.name
            and grand.name.lower() in self._BLOCK_TAGS
            for grand in child.children
        )
        if has_block_child:
            self._walk(state, child)
        else:
            self._add_paragraph_from_element(
                state, child, direction="rtl" if _is_element_rtl(
                    child) else None
            )

    def _dispatch_tag_child(
        self, state: _ExtractionState, child: Tag, name: str
    ) -> bool:
        """Dispatch a Tag to the appropriate handler. Return True if handled."""
        if name in {"script", "style", "noscript", "meta", "link", "br"}:
            return True
        if name == "hr":
            self._add_horizontal_rule(state)
            return True
        if name == "img":
            self._add_media(state, child)
            return True
        if name == "table":
            self._add_table(state, child)
            return True
        if name in {"ul", "ol"}:
            self._walk_list(state, child, list_level=0)
            return True
        if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._add_paragraph_from_element(
                state, child, heading_level=int(name[1])
            )
            return True
        if name == "p":
            self._walk_paragraph(state, child)
            return True
        if name in {"blockquote", "pre"}:
            self._add_paragraph_from_element(state, child)
            return True
        if name == "figcaption":
            self._add_paragraph_from_element(
                state, child, override_style="Caption")
            return True
        if name == "details":
            self._walk(state, child)
            return True
        if name == "summary":
            self._add_paragraph_from_element(
                state, child, override_style="Summary")
            return True
        if name in {"div", "section", "article", "main", "header",
                    "footer", "aside", "nav", "figure", "form"}:
            self._walk_div_like(state, child)
            return True
        if name == "fieldset":
            self._walk_fieldset(state, child)
            return True
        if name == "dl":
            self._walk_definition_list(state, child)
            return True
        if name in {"dt", "dd"}:
            style = "DefinitionTerm" if name == "dt" else "DefinitionData"
            self._add_paragraph_from_element(
                state, child, override_style=style)
            return True
        return False

    def _walk(self, state: _ExtractionState, parent: Tag) -> None:
        for child in parent.children:
            if isinstance(child, NavigableString):
                txt = _normalize_text(str(child))
                if txt:
                    self._add_paragraph_direct(state, [_default_run(txt)], txt)
                continue
            if not isinstance(child, Tag):
                continue
            name = child.name.lower()
            if self._dispatch_tag_child(state, child, name):
                continue
            txt = _normalize_text(child.get_text(" ", strip=True))
            if txt:
                self._add_paragraph_from_element(state, child)
