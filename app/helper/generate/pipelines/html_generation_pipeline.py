"""HTML generation pipeline for temp-doc service."""

import logging
import re
from html import escape
from itertools import groupby
from typing import Any

try:
    from bs4 import BeautifulSoup, Tag
    _BS4_AVAILABLE = True
except ImportError:  # pragma: no cover
    _BS4_AVAILABLE = False

from ...schemas.temp_doc_schema import ExtractedData


class HtmlGenerationPipeline:
    """Generate HTML from extracted data."""

    _SOURCE_BLOCK_TAGS = {
        "p",
        "blockquote",
        "pre",
        "nav",
        "figcaption",
        "header",
        "footer",
        "section",
        "article",
        "aside",
        "main",
    }

    # Block-level children to preserve when replacing inline content in a node.
    _BLOCK_PRESERVE_TAGS = {"ul", "ol", "table", "figure", "blockquote", "div",
                            "section", "pre", "details", "dl"}

    _CSS = (
        "body{font-family:Arial,Helvetica,sans-serif;line-height:1.6;margin:24px;color:#333;}"
        "table{border-collapse:collapse;width:100%;margin:12px 0;}"
        "th,td{border:1px solid #ccc;padding:8px;vertical-align:top;text-align:left;}"
        "thead th{background:#f0f0f0;font-weight:bold;}"
        "h1,h2,h3,h4,h5,h6{margin:16px 0 8px;}"
        "p{margin:8px 0;}"
        "ul,ol{margin:8px 0 8px 22px;}"
        "ul li{list-style-type:disc;}"
        "ol li{list-style-type:decimal;}"
        "ul ul,ol ul{list-style-type:circle;margin:4px 0 4px 20px;}"
        "ul ol,ol ol{list-style-type:lower-alpha;margin:4px 0 4px 20px;}"
        "code{background:#272822;color:#f8f8f2;padding:2px 5px;"
        "border-radius:3px;font-family:monospace;}"
        ".rtl{direction:rtl;unicode-bidi:bidi-override;}"
        "hr.doc-divider{border:none;border-top:1px solid #ccc;margin:16px 0;}"
        ".nested-table-note{font-size:0.8em;color:#888;font-style:italic;}"
    )

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, extracted_data: ExtractedData, title: str | None = None) -> bytes:
        """Generate HTML from extracted data."""
        try:
            metadata = self._extract_metadata(extracted_data)
            doctype = self._doctype_from_metadata(metadata)
            html_attrs = self._attrs_to_html(
                self._dict_value(metadata, "html_attributes"))
            body_attrs = self._attrs_to_html(
                self._dict_value(metadata, "body_attributes"))
            head_html = self._head_from_metadata(metadata, title)

            body = self._build_body_html(
                extracted_data, metadata, title, body_attrs)

            html = (
                f"{doctype}\n"
                f"<html{html_attrs}>\n"
                f"{head_html}\n"
                f"{body}\n"
                "</html>\n"
            )
            return html.encode("utf-8")
        except Exception as e:
            self.logger.error("HTML generation failed: %s", str(e))
            raise

    def _build_body_html(
        self,
        data: ExtractedData,
        metadata: dict,
        title: str | None,
        body_attrs: str,
    ) -> str:
        """Build body element — prefer source-body patching for HTML source docs."""
        source_body = self._try_build_body_from_source_html(data, metadata)
        if source_body is not None:
            return source_body

        body_parts = self._build_body(data, title)
        body = "\n".join(body_parts)
        return f"<body{body_attrs}>\n{body}\n</body>"

    def _try_build_body_from_source_html(
        self,
        data: ExtractedData,
        metadata: dict,
    ) -> str | None:
        """Reconstruct body by patching paragraph edits into original source body_html.

        Returns None when source body is unavailable or bs4 is not installed.
        Only used when extraction metadata carries the original HTML body.
        """
        if not _BS4_AVAILABLE:
            return None

        body_html = metadata.get("body_html")
        if not isinstance(body_html, str) or not body_html.strip():
            return None

        try:
            return self._patch_source_body(body_html, data)
        except (ValueError, AttributeError, RuntimeError) as exc:
            self.logger.warning(
                "Source-body patching failed, falling back: %s", exc)
            return None

    def _patch_source_body(self, body_html: str, data: ExtractedData) -> str:
        """Apply paragraph text and run edits to the original source body DOM."""
        soup = BeautifulSoup(body_html, "lxml")
        body = soup.body
        if not isinstance(body, Tag):
            body = soup

        self._patch_paragraphs_into_body(body, soup, data)
        self._patch_tables_into_body(body, data)

        body_tag = soup.find("body")
        if isinstance(body_tag, Tag):
            attrs_str = ""
            if body_tag.attrs:
                attrs_str = " " + " ".join(
                    f'{escape(str(k), quote=True)}="{escape(str(v), quote=True)}"'
                    if not isinstance(v, bool)
                    else str(k)
                    for k, v in body_tag.attrs.items()
                )
            inner = body_tag.decode_contents()
            return f"<body{attrs_str}>\n{inner}\n</body>"

        return f"<body>\n{body.decode_contents()}\n</body>"

    def _build_paragraph_node_map(
        self, body: Tag, paragraphs: list
    ) -> "dict[int, Tag]":
        """Build {paragraph.index: dom_node} BEFORE any content is modified.

        This map is used both for patching and as a reliable anchor lookup for
        inserted paragraphs, since the DOM is unmodified at call time.
        """
        node_map: dict[int, Tag] = {}
        for paragraph in paragraphs:
            raw_html = self._paragraph_raw_html(paragraph)
            if not raw_html:
                continue
            node = self._find_body_node_for_paragraph(
                body, raw_html, paragraph)
            if node is not None:
                node_map[paragraph.index] = node
        return node_map

    def _patch_paragraphs_into_body(
        self, body: Tag, soup: BeautifulSoup, data: ExtractedData
    ) -> None:
        """Replace source paragraph nodes with edited text where raw_html maps cleanly."""
        # Build the node map BEFORE any patching so anchor lookup is reliable.
        para_node_map = self._build_paragraph_node_map(body, data.paragraphs)
        inserted_paragraphs = self._collect_inserted_paragraphs(
            data.paragraphs)

        for paragraph in data.paragraphs:
            raw_html = self._paragraph_raw_html(paragraph)
            if not raw_html:
                continue
            node = para_node_map.get(paragraph.index)
            if node is None:
                continue
            new_html = (
                self._runs_to_html(paragraph.runs)
                if paragraph.runs
                else escape(paragraph.text or "")
            )
            self._replace_node_content(node, new_html, paragraph.text or "")

        for paragraph in inserted_paragraphs:
            self._insert_new_paragraph_in_body(
                body, soup, paragraph, data.paragraphs, para_node_map)

    def _collect_inserted_paragraphs(self, paragraphs: list) -> list:
        """Return paragraphs that have no source raw_html (i.e. were inserted via edit)."""
        return [p for p in paragraphs if not self._paragraph_raw_html(p)]

    def _paragraph_raw_html(self, paragraph: Any) -> str | None:
        """Return raw_html from paragraph source metadata if present."""
        source = self._paragraph_source(paragraph)
        raw = source.get("raw_html")
        return raw.strip() if isinstance(raw, str) and raw.strip() else None

    # _apply_paragraph_edit_to_body is superseded by the node-map approach in
    # _patch_paragraphs_into_body.  Kept for any future direct callers.
    def _apply_paragraph_edit_to_body(
        self,
        body: Tag,
        paragraph: Any,
        raw_html: str,
    ) -> None:
        """Find matching source element in body and update its content."""
        new_html = self._runs_to_html(paragraph.runs) if paragraph.runs else escape(
            paragraph.text or ""
        )
        matched = self._find_body_node_for_paragraph(body, raw_html, paragraph)
        if matched is not None:
            self._replace_node_content(matched, new_html, paragraph.text or "")

    def _find_body_node_for_paragraph(
        self, body: Tag, raw_html: str, paragraph: Any
    ) -> "Tag | None":
        """Locate a body node matching the paragraph by raw_html or text."""
        for node in body.find_all(True):
            if not isinstance(node, Tag):
                continue
            if str(node).strip() == raw_html:
                return node
            if self._html_text_matches(node, paragraph):
                return node
        return None

    def _replace_node_content(self, node: Tag, new_html: str, fallback_text: str) -> None:
        """Replace a node's inline content while preserving block-level children.

        Block-level children such as nested ``<ul>``/``<ol>`` or ``<figure>``
        elements are saved before the node is cleared and re-appended afterwards
        so that nested structures (e.g. a list item containing a sub-list) are
        not accidentally destroyed.
        """
        # Save block-level children before clearing.
        block_children = [
            c for c in list(node.children)
            if isinstance(c, Tag) and c.name
            and c.name.lower() in self._BLOCK_PRESERVE_TAGS
        ]

        node.clear()
        frag = BeautifulSoup(new_html, "lxml").body
        if frag:
            for child in frag.children:
                node.append(child.__copy__() if hasattr(
                    child, "__copy__") else child)
        else:
            node.string = fallback_text

        # Re-append the preserved block children at the end.
        for bc in block_children:
            node.append(bc)

    def _html_text_matches(self, node: Tag, paragraph: Any) -> bool:
        """Fallback: check if node text equals paragraph text AND tag name matches.

        Requiring the tag name to match prevents accidentally selecting a parent
        container (e.g. ``<figure>``) whose text content equals a child element's
        text (e.g. ``<figcaption>``).
        """
        node_text = " ".join(node.get_text(separator=" ").split())
        para_text = " ".join((paragraph.text or "").split())
        if not (para_text and node_text == para_text):
            return False
        # Also require the tag name to match the source tag to avoid false positives.
        source_tag = str(self._paragraph_source(
            paragraph).get("tag") or "").lower()
        if source_tag and node.name and node.name.lower() != source_tag:
            return False
        return True

    def _insert_new_paragraph_in_body(
        self,
        body: Tag,
        soup: BeautifulSoup,
        paragraph: Any,
        all_paragraphs: list,
        para_node_map: "dict[int, Tag] | None" = None,
    ) -> None:
        """Insert a newly created paragraph after its predecessor in body.

        Uses *para_node_map* (built before any DOM modifications) as the primary
        source for the anchor element so that patching the anchor paragraph's
        text does not break the lookup.
        """
        new_html = self._runs_to_html(paragraph.runs) if paragraph.runs else escape(
            paragraph.text or ""
        )
        new_tag = soup.new_tag("p")
        frag = BeautifulSoup(new_html, "lxml").body
        if frag:
            for child in frag.children:
                new_tag.append(child.__copy__() if hasattr(
                    child, "__copy__") else child)
        else:
            new_tag.string = paragraph.text or ""

        prev_index = paragraph.index - 1
        prev_paragraph = next(
            (p for p in all_paragraphs if p.index == prev_index), None
        )

        anchor: "Tag | None" = None
        if prev_paragraph is not None:
            # Prefer the pre-built map — it holds the original DOM node even
            # after the node's inner content has been patched.
            if para_node_map is not None:
                anchor = para_node_map.get(prev_paragraph.index)
            if anchor is None:
                anchor = self._find_anchor_for_insert(body, prev_paragraph)

        if anchor is not None:
            anchor.insert_after(new_tag)
        else:
            body.append(new_tag)

    def _find_anchor_for_insert(self, body: Tag, prev_paragraph: Any) -> Tag | None:
        """Find body node that corresponds to prev_paragraph as insertion anchor."""
        if prev_paragraph is None:
            return None
        raw_html = self._paragraph_raw_html(prev_paragraph)
        para_text = " ".join((prev_paragraph.text or "").split())
        for node in body.find_all(True):
            if not isinstance(node, Tag):
                continue
            if raw_html and str(node).strip() == raw_html:
                return node
            node_text = " ".join(node.get_text(separator=" ").split())
            if para_text and node_text == para_text:
                return node
        return None

    def _patch_tables_into_body(
        self, body: Tag, data: ExtractedData
    ) -> None:
        """Apply table cell edits into source body table nodes."""
        source_tables = body.find_all("table")
        for table_idx, source_table in enumerate(source_tables):
            if not isinstance(source_table, Tag):
                continue
            data_table = next(
                (t for t in data.tables if t.index == table_idx), None
            )
            if data_table is None:
                continue
            self._patch_table_cells(source_table, data_table)

    def _patch_table_cells(self, source_table: Tag, data_table: Any) -> None:
        """Update table cell text from edited table data."""
        source_rows = source_table.find_all("tr")
        for row_idx, source_row in enumerate(source_rows):
            if not isinstance(source_row, Tag):
                continue
            data_row = next(
                (r for r in data_table.rows if r.row_index == row_idx), None
            )
            if data_row is None:
                continue
            source_cells = source_row.find_all(["td", "th"])
            for cell_idx, source_cell in enumerate(source_cells):
                if cell_idx >= len(data_row.cells):
                    continue
                data_cell = data_row.cells[cell_idx]
                cell_text = data_cell.text or ""
                if cell_text and source_cell.get_text(strip=True) != cell_text:
                    source_cell.clear()
                    source_cell.string = cell_text

    def _build_body(self, data: ExtractedData, title: str | None) -> list[str]:
        paragraph_by_index = self._index_by_item_index(data.paragraphs)
        table_by_index = self._index_by_item_index(data.tables)
        media_by_index = dict(enumerate(data.media))
        parts: list[str] = []

        if title:
            parts.append(f"<h1>{escape(title)}</h1>")

        list_stack: list[tuple[str, int]] = []

        if data.document_order:
            self._build_body_from_order(
                data,
                parts,
                list_stack,
                paragraph_by_index,
                table_by_index,
                media_by_index,
            )
        else:
            self._build_body_from_sorted(data, parts, list_stack)

        self._close_all_lists(parts, list_stack)
        return parts

    def _extract_metadata(self, data: ExtractedData) -> dict:
        """Read extra metadata captured during extraction when available."""
        extras = getattr(data, "model_extra", None)
        if not isinstance(extras, dict):
            return {}
        metadata = extras.get("metadata")
        return metadata if isinstance(metadata, dict) else {}

    def _doctype_from_metadata(self, metadata: dict) -> str:
        """Build document doctype, defaulting to HTML5."""
        doctype = metadata.get("doctype")
        if isinstance(doctype, str) and doctype.strip():
            return doctype.strip()
        return "<!doctype html>"

    def _dict_value(self, metadata: dict, key: str) -> dict:
        """Return a metadata dict value or empty dict."""
        value = metadata.get(key)
        return value if isinstance(value, dict) else {}

    def _attrs_to_html(self, attrs: dict) -> str:
        """Render HTML attributes from metadata dictionaries."""
        if not attrs:
            return ""

        rendered: list[str] = []
        for key, value in attrs.items():
            if value is None:
                continue
            if value is True:
                rendered.append(str(key))
                continue
            if value is False:
                continue
            rendered.append(
                f'{escape(str(key), quote=True)}="{escape(str(value), quote=True)}"')

        return " " + " ".join(rendered) if rendered else ""

    def _head_from_metadata(self, metadata: dict, title: str | None) -> str:
        """Build head block using preserved HTML metadata when available."""
        preserved_head = metadata.get("head_html")
        if isinstance(preserved_head, str) and "<head" in preserved_head.lower():
            return preserved_head

        page_title = title or metadata.get("title") or "Generated Document"
        meta_lines = self._meta_lines_from_metadata(metadata)
        link_lines = self._link_lines_from_metadata(metadata)
        style_lines = self._style_lines_from_metadata(metadata)
        script_lines = self._script_lines_from_metadata(metadata)

        parts = [
            "<head>",
            '  <meta charset="utf-8">',
            '  <meta name="viewport" content="width=device-width, initial-scale=1">',
            f"  <title>{escape(str(page_title))}</title>",
        ]
        parts.extend(meta_lines)
        parts.extend(link_lines)
        parts.extend(style_lines)
        parts.extend(script_lines)
        parts.append("</head>")
        return "\n".join(parts)

    def _meta_lines_from_metadata(self, metadata: dict) -> list[str]:
        meta_tags = metadata.get("meta_tags")
        if not isinstance(meta_tags, list):
            return []

        lines: list[str] = []
        for tag_attrs in meta_tags:
            if not isinstance(tag_attrs, dict):
                continue
            lines.append(f"  <meta{self._attrs_to_html(tag_attrs)}>")
        return lines

    def _link_lines_from_metadata(self, metadata: dict) -> list[str]:
        link_tags = metadata.get("link_tags")
        if not isinstance(link_tags, list):
            return []

        lines: list[str] = []
        for tag_attrs in link_tags:
            if not isinstance(tag_attrs, dict):
                continue
            lines.append(f"  <link{self._attrs_to_html(tag_attrs)}>")
        return lines

    def _style_lines_from_metadata(self, metadata: dict) -> list[str]:
        style_blocks = metadata.get("style_blocks")
        if isinstance(style_blocks, list) and style_blocks:
            lines: list[str] = []
            for style in style_blocks:
                if not isinstance(style, str):
                    continue
                lines.append("  <style>")
                lines.append(style)
                lines.append("  </style>")
            return lines

        return [f"  <style>{self._CSS}</style>"]

    def _script_lines_from_metadata(self, metadata: dict) -> list[str]:
        script_blocks = metadata.get("script_blocks")
        if not isinstance(script_blocks, list):
            return []

        lines: list[str] = []
        for script in script_blocks:
            if not isinstance(script, dict):
                continue
            attrs = script.get("attrs")
            attrs_str = self._attrs_to_html(
                attrs if isinstance(attrs, dict) else {})
            content = script.get("content")
            if isinstance(content, str) and content.strip():
                lines.append(f"  <script{attrs_str}>{content}</script>")
            else:
                lines.append(f"  <script{attrs_str}></script>")
        return lines

    def _index_by_item_index(self, items: list) -> dict:
        """Index schema objects by their `index` field."""
        indexed: dict = {}
        for item in items:
            indexed[item.index] = item
        return indexed

    def _build_body_from_order(
        self,
        data: ExtractedData,
        parts: list[str],
        list_stack: list[tuple[str, int]],
        paragraph_by_index: dict,
        table_by_index: dict,
        media_by_index: dict,
    ) -> None:
        """Build body following explicit document order."""
        for item in data.document_order:
            if item.type == "paragraph":
                paragraph = paragraph_by_index.get(item.index)
                if paragraph is not None:
                    self._add_paragraph(parts, list_stack, paragraph)
            elif item.type == "table":
                table = table_by_index.get(item.index)
                if table is not None:
                    self._add_table(parts, list_stack, table)
            elif item.type == "media":
                media = media_by_index.get(item.index)
                if media is not None:
                    self._add_media(parts, list_stack, media)

    def _build_body_from_sorted(
        self,
        data: ExtractedData,
        parts: list[str],
        list_stack: list[tuple[str, int]],
    ) -> None:
        """Build body using paragraph/table index ordering."""
        for paragraph in sorted(data.paragraphs, key=lambda item: item.index):
            self._add_paragraph(parts, list_stack, paragraph)
        for table in sorted(data.tables, key=lambda item: item.index):
            self._add_table(parts, list_stack, table)

    def _close_lists_to(
        self,
        parts: list[str],
        list_stack: list[tuple[str, int]],
        target_level: int,
    ) -> None:
        """Close lists down to target nesting level."""
        while list_stack and list_stack[-1][1] > target_level:
            tag, _ = list_stack.pop()
            parts.append(f"</{tag}>")

    def _close_all_lists(
        self,
        parts: list[str],
        list_stack: list[tuple[str, int]],
    ) -> None:
        """Close all currently open lists."""
        while list_stack:
            tag, _ = list_stack.pop()
            parts.append(f"</{tag}>")

    def _open_list(
        self,
        parts: list[str],
        list_stack: list[tuple[str, int]],
        tag: str,
        level: int,
        start: int | None,
    ) -> None:
        """Open a list tag and track nesting."""
        if tag == "ol" and start and start != 1:
            parts.append(f'<ol start="{start}">')
        else:
            parts.append(f"<{tag}>")
        list_stack.append((tag, level))

    def _add_paragraph(
        self,
        parts: list[str],
        list_stack: list[tuple[str, int]],
        paragraph,
    ) -> None:
        """Append a paragraph-like item, including headings/lists/hr."""
        text = self._runs_to_html(
            paragraph.runs) if paragraph.runs else escape(paragraph.text or "")
        if getattr(paragraph, "style", None) == "HorizontalRule":
            self._close_all_lists(parts, list_stack)
            parts.append('<hr class="doc-divider">')
            return

        heading = self._heading_level(paragraph.style)
        if heading:
            self._close_all_lists(parts, list_stack)
            parts.append(
                f"<h{heading}{self._dir_attr(paragraph)}>{text}</h{heading}>"
            )
            return

        if self._is_list_paragraph(paragraph):
            self._add_list_item(parts, list_stack, paragraph, text)
            return

        self._close_all_lists(parts, list_stack)
        tag = self._paragraph_source_block_tag(paragraph)
        attrs = self._paragraph_attrs(paragraph)
        parts.append(f"<{tag}{attrs}>{text}</{tag}>")

    def _paragraph_source_block_tag(self, paragraph) -> str:
        """Choose output tag from extracted source metadata when available."""
        source = self._paragraph_source(paragraph)
        tag = str(source.get("tag") or "").strip().lower()
        if tag in self._SOURCE_BLOCK_TAGS:
            return tag
        return "p"

    def _paragraph_source(self, paragraph) -> dict:
        """Return paragraph source metadata from explicit or extra fields."""
        source = getattr(paragraph, "source", None)
        if isinstance(source, dict):
            return source

        extras = getattr(paragraph, "model_extra", None)
        if isinstance(extras, dict):
            extra_source = extras.get("source")
            if isinstance(extra_source, dict):
                return extra_source

        return {}

    def _dir_attr(self, paragraph) -> str:
        """Return rtl direction attribute for heading-like elements."""
        if getattr(paragraph, "direction", None) != "rtl":
            return ""
        return ' dir="rtl" class="rtl"'

    def _paragraph_attrs(self, paragraph) -> str:
        """Build safe paragraph attributes from source attrs + direction metadata."""
        attrs = self._safe_source_attrs(paragraph)
        self._merge_rtl_attrs(paragraph, attrs)
        return self._attrs_to_html(attrs)

    def _safe_source_attrs(self, paragraph) -> dict[str, str | bool]:
        """Read source attrs while filtering unsafe keys/values."""
        attrs: dict[str, str | bool] = {}
        source_attrs = self._paragraph_source(paragraph).get("attrs")
        if not isinstance(source_attrs, dict):
            return attrs

        for key, value in source_attrs.items():
            key_text = str(key or "").strip()
            if not key_text or key_text.lower().startswith("on") or value is None:
                continue
            attrs[key_text] = str(value)

        return attrs

    def _merge_rtl_attrs(self, paragraph, attrs: dict[str, str | bool]) -> None:
        """Merge RTL direction/class attributes without clobbering existing class names."""
        if getattr(paragraph, "direction", None) != "rtl":
            return

        attrs["dir"] = "rtl"
        existing_class = str(attrs.get("class") or "").strip()
        if not existing_class:
            attrs["class"] = "rtl"
            return

        if "rtl" not in existing_class.split():
            attrs["class"] = f"{existing_class} rtl"

    def _is_list_paragraph(self, paragraph) -> bool:
        """Return True when paragraph should be rendered as a list item."""
        return bool(getattr(paragraph, "is_bullet", False) or getattr(paragraph, "is_numbered", False))

    def _add_list_item(
        self,
        parts: list[str],
        list_stack: list[tuple[str, int]],
        paragraph,
        text: str,
    ) -> None:
        """Append a paragraph as a nested UL/OL list item."""
        level = getattr(paragraph, "list_level", None) or 0
        desired_tag = "ul" if getattr(paragraph, "is_bullet", False) else "ol"
        start = self._list_start(paragraph)

        if not list_stack:
            self._open_list(parts, list_stack, desired_tag, level, start)
        elif list_stack[-1][1] < level:
            self._open_list(parts, list_stack, desired_tag, level, start)
        elif list_stack[-1][1] > level:
            self._close_lists_to(parts, list_stack, level)
            if not list_stack or list_stack[-1][1] != level:
                self._open_list(parts, list_stack, desired_tag, level, start)
        elif list_stack[-1][0] != desired_tag:
            self._close_lists_to(parts, list_stack, level - 1)
            self._open_list(parts, list_stack, desired_tag, level, start)

        parts.append(f"<li>{text}</li>")

    def _list_start(self, paragraph) -> int | None:
        """Extract ordered-list start value from list_info when present."""
        list_info = getattr(paragraph, "list_info", None)
        if isinstance(list_info, dict):
            return list_info.get("start")
        return None

    def _add_table(self, parts: list[str], list_stack: list[tuple[str, int]], table) -> None:
        """Append table content after closing active lists."""
        self._close_all_lists(parts, list_stack)
        parts.append(self._extracted_table_to_html(table))

    def _add_media(self, parts: list[str], list_stack: list[tuple[str, int]], media) -> None:
        """Append media image tag after closing active lists."""
        self._close_all_lists(parts, list_stack)
        src = (getattr(media, "local_url", None) or getattr(
            media, "local_file_path", None) or "").strip()
        if not src:
            return
        alt = escape((getattr(media, "alt_text", None) or "").strip())
        parts.append(
            f'<p><img src="{escape(src, quote=True)}" alt="{alt}" '
            f'style="max-width:100%;height:auto;"></p>'
        )

    def _extracted_table_to_html(self, t) -> str:
        rows_html = [self._table_row_to_html(row) for row in t.rows]
        return "<table>" + "".join(rows_html) + "</table>"

    def _table_row_to_html(self, row) -> str:
        """Render a table row to html."""
        cells_html = [self._table_cell_to_html(cell) for cell in row.cells]
        return "<tr>" + "".join(cells_html) + "</tr>"

    def _table_cell_to_html(self, cell) -> str:
        """Render a single table cell to html."""
        tag = "th" if getattr(cell, "is_header", False) else "td"
        attrs = self._table_cell_attrs(cell)
        cell_content = self._table_cell_content(cell)
        return f"<{tag}{attrs}>{cell_content}</{tag}>"

    def _table_cell_attrs(self, cell) -> str:
        """Build colspan/rowspan html attributes."""
        colspan = getattr(cell, "colspan", 1) or 1
        rowspan = getattr(cell, "rowspan", 1) or 1
        attrs = ""
        if colspan > 1:
            attrs += f' colspan="{colspan}"'
        if rowspan > 1:
            attrs += f' rowspan="{rowspan}"'
        return attrs

    def _table_cell_content(self, cell) -> str:
        """Render textual and nested-table note content for cell."""
        cell_parts = [
            self._table_paragraph_to_html(para)
            for para in (cell.paragraphs or [])
            if self._table_paragraph_to_html(para)
        ]
        nested_note = self._nested_table_note(cell)
        if nested_note:
            cell_parts.append(nested_note)
        if cell_parts:
            return " ".join(cell_parts)
        return escape(cell.text or "")

    def _table_paragraph_to_html(self, para) -> str:
        """Render paragraph inside a table cell."""
        if para.runs:
            return self._runs_to_html(para.runs)
        if para.text:
            return escape(para.text)
        return ""

    def _nested_table_note(self, cell) -> str:
        """Render nested-table marker when indices are present."""
        nested = getattr(cell, "nested_table_indices", [])
        if not nested:
            return ""
        ids = ", ".join(str(i) for i in nested)
        return f'<span class="nested-table-note">[nested table(s): {ids}]</span>'

    def _runs_to_html(self, runs: list) -> str:
        parts: list[str] = []
        for (link_url, strike), group in groupby(
            runs,
            key=lambda r: (
                getattr(r, "hyperlink_url", None),
                getattr(r, "strikethrough", None),
            ),
        ):
            group_runs = list(group)
            inner = "".join(
                self._inline_text(
                    r.text or "",
                    r.bold,
                    r.italic,
                    r.underline,
                    None,
                    getattr(r, "code", None),
                    getattr(r, "color_rgb", None),
                    None,
                    highlight_color=getattr(r, "highlight_color", None),
                    vertical_align=getattr(r, "vertical_align", None),
                    semantic_insert=getattr(r, "semantic_insert", None),
                    semantic_delete=getattr(r, "semantic_delete", None),
                )
                for r in group_runs
            )
            if strike:
                # Use <del> when the semantic_delete flag is set, else <s>.
                del_tag = "del" if any(
                    getattr(r, "semantic_delete", None) for r in group_runs
                ) else "s"
                inner = f"<{del_tag}>{inner}</{del_tag}>"
            if link_url:
                inner = f'<a href="{escape(link_url, quote=True)}">{inner}</a>'
            parts.append(inner)
        return "".join(parts).replace("\n", "<br>")

    def _inline_text(
        self,
        text: str,
        bold: bool | None,
        italic: bool | None,
        underline: bool | None,
        strikethrough: bool | None,
        code: bool | None,
        color_rgb: str | None,
        link: str | None,
        *,
        highlight_color: str | None = None,
        vertical_align: str | None = None,
        semantic_insert: bool | None = None,
        semantic_delete: bool | None = None,
    ) -> str:
        out = escape(text)
        if code:
            out = f"<code>{out}</code>"
        if strikethrough:
            del_tag = "del" if semantic_delete else "s"
            out = f"<{del_tag}>{out}</{del_tag}>"
        if underline:
            ins_tag = "ins" if semantic_insert else "u"
            out = f"<{ins_tag}>{out}</{ins_tag}>"
        if vertical_align == "sub":
            out = f"<sub>{out}</sub>"
        elif vertical_align == "sup":
            out = f"<sup>{out}</sup>"
        if highlight_color:
            out = f'<mark style="background:{escape(highlight_color)}">{out}</mark>'
        if italic:
            out = f"<em>{out}</em>"
        if bold:
            out = f"<strong>{out}</strong>"
        if color_rgb:
            out = f'<span style="color:{escape(color_rgb)}">{out}</span>'
        if link:
            out = f'<a href="{escape(link, quote=True)}">{out}</a>'
        return out

    def _heading_level(self, style: str | None) -> int | None:
        if not style:
            return None
        m = re.search(r"heading\s*([1-6])", style, re.IGNORECASE)
        if m:
            return int(m.group(1))
        # bare h1–h6 tags stored as style
        m2 = re.match(r"^h([1-6])$", style.lower())
        if m2:
            return int(m2.group(1))
        return None
