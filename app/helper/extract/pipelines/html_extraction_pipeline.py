"""HTML extraction pipeline for temp-doc service."""

import logging
import re
from typing import Any

from bs4 import BeautifulSoup, NavigableString, Tag


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

    def run(self, file_bytes: bytes) -> dict[str, Any]:
        """Extract HTML and return JSON data."""
        try:
            html = file_bytes.decode("utf-8-sig", errors="replace")
        except Exception as e:
            raise ValueError(f"Failed to decode HTML: {str(e)}") from e

        soup = BeautifulSoup(html, "lxml")
        root = soup.body or soup

        paragraphs: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []
        media: list[dict[str, Any]] = []
        document_order: list[dict[str, Any]] = []
        seen_tables: set[int] = set()

        paragraph_index = 0
        table_index = 0
        media_index_counter = 0

        def add_media(image_elem: Tag) -> None:
            nonlocal media_index_counter
            src = (image_elem.get("src") or "").strip()
            if not src:
                return
            w = image_elem.get("width", "")
            h = image_elem.get("height", "")
            media.append({
                "relationship_id": f"html_img_{media_index_counter}",
                "content_type": None,
                "file_name": src.split("/")[-1],
                "local_file_path": src,
                "local_url": src,
                "width_emu": int(w) if str(w).isdigit() else None,
                "height_emu": int(h) if str(h).isdigit() else None,
                "alt_text": (image_elem.get("alt") or "").strip() or None,
            })
            document_order.append(
                {"type": "media", "index": media_index_counter})
            media_index_counter += 1

        def add_paragraph_from_element(
            elem: Tag,
            *,
            heading_level: int | None = None,
            is_bullet: bool = False,
            is_numbered: bool = False,
            numbering_format: str | None = None,
            list_level: int = 0,
            list_start: int | None = None,
            direction: str | None = None,
        ) -> None:
            nonlocal paragraph_index

            runs = _extract_runs(elem)
            text = "".join((run.get("text") or "") for run in runs).strip()
            if not text:
                return

            style = f"Heading {heading_level}" if heading_level else None

            elem_dir = (elem.get("dir") or "").lower()
            inline_style = elem.get("style") or ""
            m_dir = re.search(r"\bdirection\s*:\s*(\w+)", inline_style, re.I)
            if not direction:
                if elem_dir in self._RTL_VALUES:
                    direction = "rtl"
                elif m_dir and m_dir.group(1).lower() in self._RTL_VALUES:
                    direction = "rtl"
                elif re.search(r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", text):
                    direction = "rtl"

            paragraphs.append({
                "index": paragraph_index,
                "text": text,
                "style": style,
                "is_bullet": is_bullet,
                "is_numbered": is_numbered,
                "list_info": {
                    "kind": "bullet" if is_bullet else ("numbered" if is_numbered else None),
                    "numbering_format": numbering_format,
                    "level": list_level,
                    "start": list_start,
                } if (is_bullet or is_numbered) else None,
                "numbering_format": numbering_format,
                "list_level": list_level if (is_bullet or is_numbered) else None,
                "alignment": None,
                "direction": direction,
                "runs": runs if runs else [_default_run(text)],
                "source": {"format": "html"},
            })
            document_order.append(
                {"type": "paragraph", "index": paragraph_index})
            paragraph_index += 1

        def add_table(table_elem: Tag) -> None:
            nonlocal table_index

            table_id = id(table_elem)
            if table_id in seen_tables:
                return
            seen_tables.add(table_id)

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

                cells = []
                for col_index, cell in enumerate(cell_tags):
                    cell_text = _normalize_text(cell.get_text(" ", strip=True))
                    cell_runs = _extract_runs(cell)
                    try:
                        cs = int(cell.get("colspan", 1) or 1)
                    except (ValueError, TypeError):
                        cs = 1
                    try:
                        rs = int(cell.get("rowspan", 1) or 1)
                    except (ValueError, TypeError):
                        rs = 1
                    is_header = cell.name == "th"

                    nested_table_elems = [
                        nt for nt in cell.find_all("table")
                        if nt.find_parent("table") is table_elem
                    ]

                    cells.append({
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
                        "is_header": is_header,
                        "colspan": cs,
                        "rowspan": rs,
                        "_nested_elems": nested_table_elems,
                        "nested_table_indices": [],
                    })

                max_cols = max(max_cols, len(cells))
                table_rows.append({"row_index": row_index, "cells": cells})

            if not table_rows:
                return

            current_index = table_index
            tables.append({
                "index": current_index,
                "row_count": len(table_rows),
                "column_count": max_cols,
                "style": None,
                "rows": table_rows,
                "source": {"format": "html"},
            })
            document_order.append({"type": "table", "index": current_index})
            table_index += 1

            for row in table_rows:
                for cell in row["cells"]:
                    nested_elems = cell.pop("_nested_elems", [])
                    for nt in nested_elems:
                        pre_idx = table_index
                        add_table(nt)
                        if table_index > pre_idx:
                            cell["nested_table_indices"].append(pre_idx)

        def walk_list(list_elem: Tag, *, list_level: int = 0) -> None:
            is_bullet = list_elem.name.lower() == "ul"
            try:
                start = int(list_elem.get("start", 1)
                            or 1) if not is_bullet else 1
            except (ValueError, TypeError):
                start = 1
            current_number = start

            for li in list_elem.find_all("li", recursive=False):
                numbering_format = "bullet" if is_bullet else f"{current_number}."

                add_paragraph_from_element(
                    li,
                    is_bullet=is_bullet,
                    is_numbered=not is_bullet,
                    numbering_format=numbering_format,
                    list_level=list_level,
                    list_start=start if not is_bullet else None,
                )

                for child in li.children:
                    if isinstance(child, Tag):
                        cname = child.name.lower()
                        if cname in {"ul", "ol"}:
                            walk_list(child, list_level=list_level + 1)
                        elif cname == "table":
                            add_table(child)

                current_number += 1

        def add_paragraph_direct(runs: list[dict], text: str) -> None:
            nonlocal paragraph_index
            if not text:
                return
            paragraphs.append({
                "index": paragraph_index,
                "text": text,
                "style": None,
                "is_bullet": False,
                "is_numbered": False,
                "list_info": None,
                "numbering_format": None,
                "list_level": None,
                "alignment": None,
                "direction": None,
                "runs": runs if runs else [_default_run(text)],
                "source": {"format": "html"},
            })
            document_order.append(
                {"type": "paragraph", "index": paragraph_index})
            paragraph_index += 1

        def walk(parent: Tag) -> None:
            nonlocal paragraph_index
            for child in parent.children:
                if isinstance(child, NavigableString):
                    txt = _normalize_text(str(child))
                    if txt:
                        add_paragraph_direct([_default_run(txt)], txt)
                    continue

                if not isinstance(child, Tag):
                    continue

                name = child.name.lower()

                if name in {"script", "style", "noscript", "meta", "link", "br"}:
                    continue

                if name == "hr":
                    paragraphs.append({
                        "index": paragraph_index,
                        "text": "---",
                        "style": "HorizontalRule",
                        "is_bullet": False,
                        "is_numbered": False,
                        "list_info": None,
                        "numbering_format": None,
                        "list_level": None,
                        "alignment": "CENTER",
                        "direction": None,
                        "runs": [_default_run("---")],
                        "source": {"format": "html"},
                    })
                    document_order.append(
                        {"type": "paragraph", "index": paragraph_index})
                    paragraph_index += 1
                    continue

                if name == "img":
                    add_media(child)
                    continue

                if name == "table":
                    add_table(child)
                    continue

                if name in {"ul", "ol"}:
                    walk_list(child, list_level=0)
                    continue

                if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
                    add_paragraph_from_element(
                        child, heading_level=int(name[1]))
                    continue

                if name == "p":
                    block_children = [
                        c for c in child.children
                        if isinstance(c, Tag) and c.name.lower() in {"ul", "ol", "table", "img"}
                    ]
                    if block_children:
                        text_only_runs = _extract_runs_skip_blocks(child)
                        text_only = "".join((r.get("text") or "")
                                            for r in text_only_runs).strip()
                        if text_only:
                            add_paragraph_direct(text_only_runs, text_only)
                        for bc in block_children:
                            bcname = bc.name.lower()
                            if bcname in {"ul", "ol"}:
                                walk_list(bc, list_level=0)
                            elif bcname == "table":
                                add_table(bc)
                            elif bcname == "img":
                                add_media(bc)
                    else:
                        add_paragraph_from_element(child)
                    continue

                if name in {"blockquote", "pre"}:
                    add_paragraph_from_element(child)
                    continue

                if name in {"div", "section", "article", "main", "header",
                            "footer", "aside", "nav", "figure", "figcaption"}:
                    elem_dir = (child.get("dir") or "").lower()
                    inline_style = child.get("style") or ""
                    m_dir = re.search(
                        r"\bdirection\s*:\s*(\w+)", inline_style, re.I)
                    elem_text = child.get_text()
                    has_arabic = bool(re.search(
                        r"[\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", elem_text))
                    is_rtl = (
                        elem_dir in self._RTL_VALUES
                        or (m_dir and m_dir.group(1).lower() in self._RTL_VALUES)
                        or has_arabic
                    )

                    has_block_child = any(
                        isinstance(grand, Tag) and grand.name and grand.name.lower(
                        ) in self._BLOCK_TAGS
                        for grand in child.children
                    )
                    if has_block_child:
                        walk(child)
                    else:
                        add_paragraph_from_element(
                            child, direction="rtl" if is_rtl else None)
                    continue

                txt = _normalize_text(child.get_text(" ", strip=True))
                if txt:
                    add_paragraph_from_element(child)

        # ── run extraction helpers ────────────────────────────────────────────

        def _extract_runs(element: Tag) -> list[dict[str, Any]]:
            return _collect_runs(element, skip_block_children=False)

        def _extract_runs_skip_blocks(element: Tag) -> list[dict[str, Any]]:
            return _collect_runs(element, skip_block_children=True)

        def _collect_runs(element: Tag, *, skip_block_children: bool) -> list[dict[str, Any]]:
            runs: list[dict[str, Any]] = []
            _BLOCK_SKIP = {"ul", "ol", "table", "img"}

            def push_text(text: str, style: dict[str, Any]) -> None:
                normalized = _normalize_text(text)
                if not normalized:
                    return
                run = {
                    "index": len(runs),
                    "text": normalized,
                    "bold": style.get("bold"),
                    "italic": style.get("italic"),
                    "underline": style.get("underline"),
                    "strikethrough": style.get("strikethrough"),
                    "code": style.get("code"),
                    "color_rgb": style.get("color_rgb"),
                    "font_name": style.get("font_name"),
                    "font_size_pt": None,
                    "highlight_color": None,
                    "hyperlink_url": style.get("hyperlink_url"),
                    "embedded_media": [],
                }
                if runs and _same_style(runs[-1], run):
                    runs[-1]["text"] = f"{runs[-1]['text']} {run['text']}".strip()
                else:
                    run["index"] = len(runs)
                    runs.append(run)

            def rec(node: Any, style: dict[str, Any]) -> None:
                if isinstance(node, NavigableString):
                    push_text(str(node), style)
                    return
                if not isinstance(node, Tag):
                    return

                name = (node.name or "").lower()

                if name in {"script", "style", "noscript"}:
                    return
                if skip_block_children and name in _BLOCK_SKIP:
                    return
                if name in {"table", "ul", "ol"}:
                    return
                if name == "br":
                    push_text("\n", style)
                    return

                next_style = dict(style)

                if name in {"b", "strong"}:
                    next_style["bold"] = True
                if name in {"i", "em"}:
                    next_style["italic"] = True
                if name in {"u", "ins"}:
                    next_style["underline"] = True
                if name in {"s", "del", "strike"}:
                    next_style["strikethrough"] = True
                if name in {"code", "kbd", "samp", "tt"}:
                    next_style["code"] = True
                if name == "a":
                    href = (node.get("href") or "").strip()
                    if href:
                        next_style["hyperlink_url"] = href

                inline = node.get("style") or ""
                if inline:
                    m_color = re.search(r"\bcolor\s*:\s*([^;]+)", inline, re.I)
                    if m_color:
                        next_style["color_rgb"] = _css_color_to_rgb(
                            m_color.group(1).strip())
                    m_font = re.search(
                        r"\bfont-family\s*:\s*([^;]+)", inline, re.I)
                    if m_font:
                        font_raw = m_font.group(1).strip().strip(
                            "'\"").split(",")[0].strip().strip("'\"")
                        if font_raw:
                            next_style["font_name"] = font_raw

                for sub in node.children:
                    rec(sub, next_style)

            rec(element, {
                "bold": None, "italic": None, "underline": None,
                "strikethrough": None, "code": None,
                "color_rgb": None, "font_name": None,
                "hyperlink_url": None,
            })
            for idx, run in enumerate(runs):
                run["index"] = idx
            return runs

        def _same_style(a: dict[str, Any], b: dict[str, Any]) -> bool:
            keys = ("bold", "italic", "underline", "strikethrough", "code",
                    "color_rgb", "font_name", "hyperlink_url")
            return all(a.get(k) == b.get(k) for k in keys)

        def _css_color_to_rgb(raw: str) -> str | None:
            raw = raw.strip()
            if raw.startswith("#"):
                return raw
            m_rgb = re.match(
                r"rgb\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", raw, re.I)
            if m_rgb:
                r, g, b = int(m_rgb.group(1)), int(
                    m_rgb.group(2)), int(m_rgb.group(3))
                return f"#{r:02x}{g:02x}{b:02x}"
            if re.match(r"^[a-zA-Z]+$", raw):
                return raw
            return None

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

        walk(root)

        title_tag = soup.title
        title_text = (title_tag.string.strip()
                      if title_tag and title_tag.string else None)

        return {
            "metadata": {
                "source_type": "html",
                "extraction_mode": "html",
                "title": title_text,
            },
            "document_order": document_order,
            "document_defaults": None,
            "styles": [],
            "paragraphs": paragraphs,
            "tables": tables,
            "media": media,
        }
