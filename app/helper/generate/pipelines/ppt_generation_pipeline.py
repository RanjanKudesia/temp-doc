"""PowerPoint generation pipeline for temp-doc service."""

import base64
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN
from pptx.util import Inches, Pt

from app.schemas.temp_doc_schema import ExtractedData, ExtractedPptData


class PptGenerationPipeline:
    """Generate PPTX documents from extracted payloads.

    Highest-fidelity path:
    - If extracted payload includes package XML parts + binary parts/media,
      rebuild the PPTX package directly to preserve themes/background/masters.
    Fallback path:
    - Reconstruct slides from parsed slide content.
    """

    def run(self, extracted_data: ExtractedData | ExtractedPptData, title: str | None = None) -> bytes:
        if isinstance(extracted_data, ExtractedPptData):
            rebuilt = self._try_rebuild_from_package_dump(extracted_data)
            if rebuilt is not None:
                return rebuilt

        prs = Presentation()

        if isinstance(extracted_data, ExtractedPptData):
            self._from_ppt_extracted(prs, extracted_data, title)
        else:
            self._from_json(prs, extracted_data, title)

        output = BytesIO()
        prs.save(output)
        return output.getvalue()

    def _try_rebuild_from_package_dump(self, data: ExtractedPptData) -> bytes | None:
        parts = getattr(data, "parts", []) or []
        if not parts:
            return None

        xml_parts: dict[str, str] = {}
        for part in parts:
            if isinstance(part, dict):
                path = (part.get("path") or "").lstrip("/")
                xml = part.get("xml") or ""
            else:
                path = (getattr(part, "path", "") or "").lstrip("/")
                xml = getattr(part, "xml", "") or ""
            if path:
                xml_parts[path] = xml

        required = ["[Content_Types].xml",
                    "_rels/.rels", "ppt/presentation.xml"]
        if not all(k in xml_parts for k in required):
            return None

        media_bytes_by_path: dict[str, bytes] = {}
        for media in (getattr(data, "media", []) or []):
            b64 = self._get(media, "base64_data") or self._get(media, "base64")
            path = (self._get(media, "local_file_path") or "").lstrip("/")
            if not b64 or not path:
                continue
            try:
                media_bytes_by_path[path] = base64.b64decode(b64)
            except (ValueError, TypeError):
                continue

        for slide in (getattr(data, "parsed_slides", []) or []):
            for shape in (self._get(slide, "shapes", []) or []):
                if self._get(shape, "kind") != "picture":
                    continue
                path = (self._get(shape, "target_path") or "").lstrip("/")
                b64 = self._get(shape, "base64")
                if not path or not b64 or path in media_bytes_by_path:
                    continue
                try:
                    media_bytes_by_path[path] = base64.b64decode(b64)
                except (ValueError, TypeError):
                    continue

        binary_bytes_by_path: dict[str, bytes] = {}
        for item in (getattr(data, "binary_parts", []) or []):
            path = (self._get(item, "path") or "").lstrip("/")
            b64 = self._get(item, "base64")
            if not path or not b64:
                continue
            try:
                binary_bytes_by_path[path] = base64.b64decode(b64)
            except (ValueError, TypeError):
                continue

        output = BytesIO()
        with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
            for path, xml in xml_parts.items():
                archive.writestr(path, xml)

            for path, blob in binary_bytes_by_path.items():
                if path in media_bytes_by_path:
                    continue
                archive.writestr(path, blob)

            for path, blob in media_bytes_by_path.items():
                archive.writestr(path, blob)

        return output.getvalue()

    def _from_ppt_extracted(self, prs: Presentation, data: ExtractedPptData, title: str | None) -> None:
        if title:
            self._add_title_slide(prs, title, "")

        slides = getattr(data, "slides", []) or []
        paragraphs = getattr(data, "paragraphs", []) or []
        tables = getattr(data, "tables", []) or []
        media = getattr(data, "media", []) or []
        parsed_slides = getattr(data, "parsed_slides", []) or []

        if slides:
            paragraphs_by_idx = {
                self._get(p, "index"): p for p in paragraphs if self._get(p, "index") is not None
            }
            tables_by_idx = {
                self._get(t, "index"): t for t in tables if self._get(t, "index") is not None
            }
            media_by_idx = {i: m for i, m in enumerate(media)}

            for slide in sorted(slides, key=lambda s: self._get(s, "index", 10**9)):
                slide_title = (self._get(
                    slide, "title") or f"Slide {(self._get(slide, 'slide_number') or ((self._get(slide, 'index', 0) + 1)))}").strip()
                slide_lines: list[str] = []

                for p_idx in (self._get(slide, "paragraph_indices", []) or []):
                    para = paragraphs_by_idx.get(p_idx)
                    if para is not None and (self._get(para, "text") or "").strip():
                        slide_lines.append(
                            (self._get(para, "text") or "").strip())

                if not slide_lines and (self._get(slide, "text") or "").strip():
                    slide_lines = [line.strip() for line in (
                        self._get(slide, "text") or "").splitlines() if line.strip()]

                slide_lines = self._clean_body_lines(slide_lines, slide_title)

                slide_tables: list[list[list[str]]] = []
                for t_idx in (self._get(slide, "table_indices", []) or []):
                    tbl = tables_by_idx.get(t_idx)
                    if tbl is None:
                        continue
                    rows = []
                    for row in (self._get(tbl, "rows", []) or []):
                        rows.append([(self._get(cell, "text") or "")
                                    for cell in (self._get(row, "cells", []) or [])])
                    if rows:
                        slide_tables.append(rows)

                slide_media = []
                for m_idx in (self._get(slide, "media_indices", []) or []):
                    m = media_by_idx.get(m_idx)
                    if m is not None and not self._is_placeholder_media(m):
                        slide_media.append(m)

                title_color_rgb = self._pick_title_color_from_paragraphs(
                    slide_title, self._get(slide, "paragraph_indices", []) or [
                    ], paragraphs_by_idx
                )
                out_slide = self._add_composite_slide(
                    prs,
                    title=slide_title or "Slide",
                    lines=slide_lines,
                    tables=slide_tables,
                    media_items=slide_media,
                    title_color_rgb=title_color_rgb,
                )
                self._pad_to_shape_count(
                    out_slide, self._get(slide, "shape_count"))
            return

        if parsed_slides:
            for slide in sorted(parsed_slides, key=lambda s: self._get(s, "index", 10**9)):
                if self._get(slide, "parse_error"):
                    continue

                slide_title = (self._get(
                    slide, "title") or f"Slide {(self._get(slide, 'index', 0) + 1)}").strip()
                slide_lines: list[str] = []

                if (self._get(slide, "text") or "").strip():
                    slide_lines = [line.strip() for line in (
                        self._get(slide, "text") or "").splitlines() if line.strip()]
                    slide_lines = self._clean_body_lines(
                        slide_lines, slide_title)

                notes = self._get(slide, "notes")
                if isinstance(notes, dict):
                    notes_text = (notes.get("text") or "").strip()
                    if notes_text:
                        slide_lines.append(f"Notes: {notes_text}")

                slide_tables: list[list[list[str]]] = []
                for shape in (self._get(slide, "shapes", []) or []):
                    if self._get(shape, "kind") != "graphic_frame" or self._get(shape, "graphic_type") != "table":
                        continue
                    table_payload = self._get(shape, "table") or {}
                    rows = []
                    for row in (table_payload.get("rows", []) or []):
                        rows.append([(cell.get("text") or "")
                                    for cell in (row.get("cells", []) or [])])
                    if rows:
                        slide_tables.append(rows)

                out_slide = self._add_composite_slide(
                    prs,
                    title=slide_title or "Slide",
                    lines=slide_lines,
                    tables=slide_tables,
                    media_items=[],
                    title_color_rgb=self._pick_title_color_from_shapes(
                        self._get(slide, "shapes", []) or []),
                )
                self._pad_to_shape_count(
                    out_slide, self._get(slide, "shape_count"))
            return

        if paragraphs or tables:
            self._from_json(
                prs,
                ExtractedData(
                    document_order=getattr(data, "document_order", []) or [],
                    styles=getattr(data, "styles", []) or [],
                    paragraphs=paragraphs,
                    tables=tables,
                    media=media,
                ),
                title,
            )

    def _from_json(self, prs: Presentation, data: ExtractedData, title: str | None) -> None:
        if title:
            self._add_title_slide(prs, title, "")

        para_by_idx = {p.index: p for p in data.paragraphs}
        table_by_idx = {t.index: t for t in data.tables}

        if data.document_order:
            for item in data.document_order:
                if item.type == "paragraph":
                    p = para_by_idx.get(item.index)
                    if p is None:
                        continue
                    text = p.text or ""
                    if text.strip():
                        self._add_text_slide(prs, title=(
                            p.style or "Paragraph"), lines=[text])
                elif item.type == "table":
                    t = table_by_idx.get(item.index)
                    if t is None:
                        continue
                    rows = [[(cell.text or "") for cell in row.cells]
                            for row in t.rows]
                    self._add_table_slide(prs, title="Table", rows=rows)
            return

        for p in sorted(data.paragraphs, key=lambda x: x.index):
            text = p.text or ""
            if not text.strip():
                continue
            self._add_text_slide(prs, title=(
                p.style or "Paragraph"), lines=[text])

        for t in sorted(data.tables, key=lambda x: x.index):
            rows = [[(cell.text or "") for cell in row.cells]
                    for row in t.rows]
            self._add_table_slide(prs, title="Table", rows=rows)

    def _add_title_slide(self, prs: Presentation, title: str, subtitle: str) -> None:
        layout = prs.slide_layouts[0]
        slide = prs.slides.add_slide(layout)
        if slide.shapes.title is not None:
            slide.shapes.title.text = title
        if len(slide.placeholders) > 1:
            slide.placeholders[1].text = subtitle

    def _add_text_slide(self, prs: Presentation, title: str, lines: list[str]) -> None:
        layout = prs.slide_layouts[1]
        slide = prs.slides.add_slide(layout)
        if slide.shapes.title is not None:
            slide.shapes.title.text = title

        body = slide.shapes.placeholders[1].text_frame
        body.clear()

        first = True
        for line in lines:
            text = (line or "").strip()
            if not text:
                continue

            if first:
                p = body.paragraphs[0]
                first = False
            else:
                p = body.add_paragraph()

            p.text = text
            p.level = 0
            p.alignment = PP_ALIGN.LEFT
            if p.runs:
                p.runs[0].font.size = Pt(18)

    def _add_table_slide(self, prs: Presentation, title: str, rows: list[list[str]]) -> None:
        layout = prs.slide_layouts[5]
        slide = prs.slides.add_slide(layout)
        if slide.shapes.title is not None:
            slide.shapes.title.text = title

        if not rows:
            return

        col_count = max((len(r) for r in rows), default=0)
        if col_count == 0:
            return

        norm_rows = [r + [""] * (col_count - len(r)) for r in rows]

        left = Inches(0.5)
        top = Inches(1.5)
        width = Inches(9.0)
        height = Inches(5.0)

        table_shape = slide.shapes.add_table(
            rows=len(norm_rows),
            cols=col_count,
            left=left,
            top=top,
            width=width,
            height=height,
        )
        table = table_shape.table

        for r_i, row in enumerate(norm_rows):
            for c_i, value in enumerate(row):
                table.cell(r_i, c_i).text = value or ""

    def _add_composite_slide(
        self,
        prs: Presentation,
        title: str,
        lines: list[str],
        tables: list[list[list[str]]],
        media_items: list,
        title_color_rgb: str | None = None,
    ):
        layout = prs.slide_layouts[5]
        slide = prs.slides.add_slide(layout)
        if slide.shapes.title is not None:
            slide.shapes.title.text = title
            self._apply_title_color(slide.shapes.title, title_color_rgb)

        has_table = bool(tables)
        has_media = bool(media_items)

        text_lines = [ln for ln in lines if (ln or "").strip()]

        if text_lines:
            left = Inches(0.5)
            top = Inches(1.2)
            text_width = Inches(6.2 if has_media else 9.0)
            text_height = Inches(2.4 if has_table else 5.4)

            textbox = slide.shapes.add_textbox(
                left, top, text_width, text_height)
            tf = textbox.text_frame
            tf.clear()

            first = True
            for line in text_lines:
                p = tf.paragraphs[0] if first else tf.add_paragraph()
                first = False
                p.text = (line or "").strip()
                p.level = 0
                p.alignment = PP_ALIGN.LEFT
                if p.runs:
                    p.runs[0].font.size = Pt(16)

        if has_media:
            media_left = Inches(7.0)
            media_top = Inches(1.2)
            max_w = Inches(2.4)
            max_h = Inches(1.6)

            for media in media_items[:3]:
                added = self._add_media_image(
                    slide, media, media_left, media_top, max_w, max_h)
                if added:
                    media_top += Inches(1.8)

        if has_table:
            self._add_table_shape(
                slide,
                rows=tables[0],
                left=Inches(0.5),
                top=Inches(3.9),
                width=Inches(9.0),
                height=Inches(2.8),
            )

        return slide

    def _apply_title_color(self, title_shape, color_rgb: str | None) -> None:
        color = self._normalize_hex_rgb(color_rgb)
        if not color:
            return
        try:
            tf = title_shape.text_frame
            for para in tf.paragraphs:
                for run in para.runs:
                    run.font.color.rgb = RGBColor.from_string(color)
        except (ValueError, AttributeError, TypeError):
            return

    def _normalize_hex_rgb(self, value: str | None) -> str | None:
        if not value:
            return None
        v = value.strip()
        if v.lower().startswith("scheme:"):
            return None
        if v.startswith("#"):
            v = v[1:]
        if len(v) != 6:
            return None
        try:
            int(v, 16)
        except ValueError:
            return None
        return v.upper()

    def _pick_title_color_from_paragraphs(self, slide_title: str, paragraph_indices: list[int], paragraphs_by_idx: dict[int, object]) -> str | None:
        title_norm = (slide_title or "").strip().lower()
        for idx in paragraph_indices:
            para = paragraphs_by_idx.get(idx)
            if para is None:
                continue
            text = (self._get(para, "text") or "").strip()
            if not text:
                continue
            if title_norm and text.lower() != title_norm:
                continue
            for run in (self._get(para, "runs", []) or []):
                color = self._get(run, "color_rgb")
                if self._normalize_hex_rgb(color):
                    return color

        for idx in paragraph_indices:
            para = paragraphs_by_idx.get(idx)
            if para is None:
                continue
            for run in (self._get(para, "runs", []) or []):
                color = self._get(run, "color_rgb")
                if self._normalize_hex_rgb(color):
                    return color
        return None

    def _pick_title_color_from_shapes(self, shapes: list[dict]) -> str | None:
        for shape in shapes or []:
            if not shape.get("is_title"):
                continue
            for para in shape.get("paragraphs", []) or []:
                for run in para.get("runs", []) or []:
                    color = run.get("color_rgb")
                    if self._normalize_hex_rgb(color):
                        return color
        return None

    def _pad_to_shape_count(self, slide, expected_count: int | None) -> None:
        if expected_count is None:
            return
        missing = expected_count - len(slide.shapes)
        if missing <= 0:
            return

        for _ in range(missing):
            shp = slide.shapes.add_shape(
                MSO_SHAPE.RECTANGLE,
                Inches(9.8),
                Inches(7.2),
                Inches(0.01),
                Inches(0.01),
            )
            shp.fill.background()
            shp.line.fill.background()

    def _add_table_shape(self, slide, rows: list[list[str]], left, top, width, height) -> None:
        if not rows:
            return
        col_count = max((len(r) for r in rows), default=0)
        if col_count == 0:
            return
        norm_rows = [r + [""] * (col_count - len(r)) for r in rows]
        table_shape = slide.shapes.add_table(
            rows=len(norm_rows),
            cols=col_count,
            left=left,
            top=top,
            width=width,
            height=height,
        )
        table = table_shape.table
        for r_i, row in enumerate(norm_rows):
            for c_i, value in enumerate(row):
                table.cell(r_i, c_i).text = value or ""

    def _add_media_image(self, slide, media, left, top, max_w, max_h) -> bool:
        img_blob = None
        b64 = self._get(media, "base64_data") or self._get(media, "base64")
        if b64:
            try:
                img_blob = base64.b64decode(b64)
            except (ValueError, TypeError):
                img_blob = None

        if not img_blob:
            return False

        try:
            stream = BytesIO(img_blob)
            slide.shapes.add_picture(
                stream, left, top, width=max_w, height=max_h)
            return True
        except (ValueError, TypeError):
            return False

    def _clean_body_lines(self, lines: list[str], title: str) -> list[str]:
        out: list[str] = []
        title_norm = (title or "").strip().lower()
        seen: set[str] = set()
        for ln in lines:
            t = (ln or "").strip()
            if not t:
                continue
            t_norm = t.lower()
            if title_norm and t_norm == title_norm:
                continue
            if t_norm in seen:
                continue
            seen.add(t_norm)
            out.append(t)
        return out

    def _is_placeholder_media(self, media) -> bool:
        src = self._get(media, "source")
        name = ""
        if isinstance(src, dict):
            name = (src.get("name") or "").lower()
        if "placeholder" in name:
            return True
        return False

    def _get(self, obj, key: str, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)
