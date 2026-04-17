"""HTML generation pipeline for temp-doc service."""

import logging
import re
from html import escape
from itertools import groupby

from app.schemas.temp_doc_schema import ExtractedData


class HtmlGenerationPipeline:
    """Generate HTML from extracted data."""

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
            body_parts = self._build_body(extracted_data, title)
            body = "\n".join(body_parts)
            html = (
                "<!doctype html>\n"
                '<html lang="en">\n'
                "<head>\n"
                '  <meta charset="utf-8">\n'
                '  <meta name="viewport" content="width=device-width, initial-scale=1">\n"'
                "  <title>Generated Document</title>\n"
                f"  <style>{self._CSS}</style>\n"
                "</head>\n"
                "<body>\n"
                f"{body}\n"
                "</body>\n"
                "</html>\n"
            )
            return html.encode("utf-8")
        except Exception as e:
            self.logger.error("HTML generation failed: %s", str(e))
            raise

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
        parts.append(f"<p{self._dir_attr(paragraph)}>{text}</p>")

    def _dir_attr(self, paragraph) -> str:
        """Return rtl direction attribute for paragraph-like objects."""
        return ' dir="rtl" class="rtl"' if getattr(paragraph, "direction", None) == "rtl" else ""

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
                )
                for r in group_runs
            )
            if strike:
                inner = f"<s>{inner}</s>"
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
    ) -> str:
        out = escape(text)
        if code:
            out = f"<code>{out}</code>"
        if strikethrough:
            out = f"<s>{out}</s>"
        if underline:
            out = f"<u>{out}</u>"
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
