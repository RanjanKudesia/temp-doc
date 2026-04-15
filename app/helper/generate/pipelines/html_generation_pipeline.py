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
        "code{background:#272822;color:#f8f8f2;padding:2px 5px;border-radius:3px;font-family:monospace;}"
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
        paragraph_by_index = {p.index: p for p in data.paragraphs}
        table_by_index = {t.index: t for t in data.tables}
        media_by_index = {idx: m for idx, m in enumerate(data.media)}
        parts: list[str] = []

        if title:
            parts.append(f"<h1>{escape(title)}</h1>")

        list_stack: list[tuple[str, int]] = []

        def close_lists_to(target_level: int) -> None:
            while list_stack and list_stack[-1][1] > target_level:
                tag, _ = list_stack.pop()
                parts.append(f"</{tag}>")

        def close_all_lists() -> None:
            while list_stack:
                tag, _ = list_stack.pop()
                parts.append(f"</{tag}>")

        def open_list(tag: str, level: int, start: int | None) -> None:
            if tag == "ol" and start and start != 1:
                parts.append(f'<ol start="{start}">')
            else:
                parts.append(f"<{tag}>")
            list_stack.append((tag, level))

        def add_paragraph(p) -> None:
            text = self._runs_to_html(
                p.runs) if p.runs else escape(p.text or "")
            heading = self._heading_level(p.style)

            if getattr(p, "style", None) == "HorizontalRule":
                close_all_lists()
                parts.append('<hr class="doc-divider">')
                return

            if heading:
                close_all_lists()
                dir_attr = ' dir="rtl" class="rtl"' if getattr(
                    p, "direction", None) == "rtl" else ""
                parts.append(f"<h{heading}{dir_attr}>{text}</h{heading}>")
                return

            level = getattr(p, "list_level", None) or 0
            if getattr(p, "is_bullet", False) or getattr(p, "is_numbered", False):
                desired_tag = "ul" if getattr(p, "is_bullet", False) else "ol"
                start = None
                li = getattr(p, "list_info", None)
                if li and isinstance(li, dict):
                    start = li.get("start")

                if not list_stack:
                    open_list(desired_tag, level, start)
                elif list_stack[-1][1] < level:
                    open_list(desired_tag, level, start)
                elif list_stack[-1][1] > level:
                    close_lists_to(level)
                    if not list_stack or list_stack[-1][1] != level:
                        open_list(desired_tag, level, start)
                elif list_stack[-1][0] != desired_tag:
                    close_lists_to(level - 1)
                    open_list(desired_tag, level, start)

                parts.append(f"<li>{text}</li>")
                return

            close_all_lists()
            dir_attr = ' dir="rtl" class="rtl"' if getattr(
                p, "direction", None) == "rtl" else ""
            parts.append(f"<p{dir_attr}>{text}</p>")

        def add_table(t) -> None:
            close_all_lists()
            parts.append(self._extracted_table_to_html(t))

        def add_media(m) -> None:
            close_all_lists()
            src = (getattr(m, "local_url", None) or getattr(
                m, "local_file_path", None) or "").strip()
            if not src:
                return
            alt = escape((getattr(m, "alt_text", None) or "").strip())
            parts.append(
                f'<p><img src="{escape(src, quote=True)}" alt="{alt}" '
                f'style="max-width:100%;height:auto;"></p>'
            )

        if data.document_order:
            for item in data.document_order:
                if item.type == "paragraph":
                    p = paragraph_by_index.get(item.index)
                    if p is not None:
                        add_paragraph(p)
                elif item.type == "table":
                    t = table_by_index.get(item.index)
                    if t is not None:
                        add_table(t)
                elif item.type == "media":
                    m = media_by_index.get(item.index)
                    if m is not None:
                        add_media(m)
        else:
            for p in sorted(data.paragraphs, key=lambda x: x.index):
                add_paragraph(p)
            for t in sorted(data.tables, key=lambda x: x.index):
                add_table(t)

        close_all_lists()
        return parts

    def _extracted_table_to_html(self, t) -> str:
        rows_html: list[str] = []
        for row in t.rows:
            cells_html: list[str] = []
            for cell in row.cells:
                tag = "th" if getattr(cell, "is_header", False) else "td"
                cs = getattr(cell, "colspan", 1) or 1
                rs = getattr(cell, "rowspan", 1) or 1
                attrs = ""
                if cs > 1:
                    attrs += f' colspan="{cs}"'
                if rs > 1:
                    attrs += f' rowspan="{rs}"'
                cell_parts: list[str] = []
                for para in (cell.paragraphs or []):
                    if para.runs:
                        cell_parts.append(self._runs_to_html(para.runs))
                    elif para.text:
                        cell_parts.append(escape(para.text))
                nested = getattr(cell, "nested_table_indices", [])
                if nested:
                    ids = ", ".join(str(i) for i in nested)
                    cell_parts.append(
                        f'<span class="nested-table-note">[nested table(s): {ids}]</span>'
                    )
                cell_content = " ".join(
                    cell_parts) if cell_parts else escape(cell.text or "")
                cells_html.append(f"<{tag}{attrs}>{cell_content}</{tag}>")
            rows_html.append("<tr>" + "".join(cells_html) + "</tr>")
        return "<table>" + "".join(rows_html) + "</table>"

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
