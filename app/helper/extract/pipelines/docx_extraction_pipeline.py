"""DOCX extraction pipeline for temp-doc service - simplified, no storage."""

import base64
import logging
import mimetypes
from io import BytesIO
from typing import Any
from zipfile import is_zipfile

from docx import Document
from docx.document import Document as DocumentObject
from docx.opc.constants import RELATIONSHIP_TYPE as RT
from docx.oxml.ns import qn
from docx.table import _Cell, Table
from docx.text.paragraph import Paragraph
from docx.text.run import Run
from lxml import etree

XML_NS = {
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
}


class DocxExtractionPipeline:
    """Extract paragraphs, tables, and styles from DOCX files."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, file_bytes: bytes) -> dict[str, Any]:
        """Parse a DOCX byte stream and return extracted JSON payload."""
        if not is_zipfile(BytesIO(file_bytes)):
            raise ValueError(
                "Invalid DOCX file: not a valid ZIP archive. File may be corrupted.")

        try:
            document = Document(BytesIO(file_bytes))
        except (ValueError, TypeError, OSError, KeyError) as e:
            raise ValueError(
                f"Failed to parse DOCX document: {str(e)}") from e

        extracted = self._extract_document(document)
        return extracted

    def _extract_document(self, document: DocumentObject) -> dict[str, Any]:
        """Extract all document content."""
        media_index = self._extract_and_save_media(document, "temp-doc")

        paragraphs = [
            self._extract_paragraph(paragraph, index, document, media_index)
            for index, paragraph in enumerate(document.paragraphs)
        ]
        tables = [
            self._extract_table(table, index, document, media_index)
            for index, table in enumerate(document.tables)
        ]

        body_order: list[dict[str, Any]] = []
        paragraph_index = 0
        table_index = 0
        for child in document.element.body.iterchildren():
            tag = child.tag.rsplit("}", 1)[-1]
            if tag == "p":
                body_order.append(
                    {"type": "paragraph", "index": paragraph_index})
                paragraph_index += 1
            elif tag == "tbl":
                body_order.append({"type": "table", "index": table_index})
                table_index += 1

        styles = self._extract_styles(document)
        document_defaults = self._extract_document_defaults(document)

        return {
            "document_order": body_order,
            "document_defaults": document_defaults,
            "styles": styles,
            "paragraphs": paragraphs,
            "tables": tables,
            "media": list(media_index.values()),
        }

    def _extract_and_save_media(self, document: DocumentObject, output_basename: str) -> dict[str, dict[str, Any]]:
        """Extract embedded images and keep them in JSON as base64 payload."""
        media_index: dict[str, dict[str, Any]] = {}

        for rel_id, rel in document.part.rels.items():
            if rel.reltype != RT.IMAGE:
                continue

            try:
                image_part = rel.target_part
                if image_part is None or not hasattr(image_part, "blob"):
                    continue

                blob = image_part.blob
                if not blob:
                    continue

                content_type = image_part.content_type or "application/octet-stream"
                extension = self._content_type_to_extension(content_type)
                file_name = f"{output_basename}_{rel_id}.{extension}"
                media_index[rel_id] = {
                    "relationship_id": rel_id,
                    "content_type": content_type,
                    "file_name": file_name,
                    "local_file_path": None,
                    "local_url": None,
                    "base64": base64.b64encode(blob).decode("ascii"),
                    "size_bytes": len(blob),
                    "source_partname": str(image_part.partname),
                    "alt_text": None,
                }
            except (AttributeError, KeyError, ValueError, TypeError, OSError):
                continue

        return media_index

    def _content_type_to_extension(self, content_type: str) -> str:
        normalized = content_type.lower().strip()
        if not normalized:
            return "bin"

        ext = mimetypes.guess_extension(normalized, strict=False)
        if ext:
            return ext.lstrip(".")

        fallback = {
            "image/jpeg": "jpg",
            "image/jpg": "jpg",
            "image/png": "png",
            "image/gif": "gif",
            "image/bmp": "bmp",
            "image/tiff": "tiff",
            "image/webp": "webp",
            "image/svg+xml": "svg",
            "image/x-wmf": "wmf",
            "image/x-emf": "emf",
        }
        return fallback.get(normalized, "bin")

    def _extract_document_defaults(self, document: DocumentObject) -> dict[str, Any]:
        """Extract DOCX-level default run properties."""
        defaults = {
            "font_name": None,
            "font_size_pt": None,
            "color_rgb": None,
        }

        try:
            styles_root = document.styles.element
            run_defaults = styles_root.find(
                "w:docDefaults/w:rPrDefault/w:rPr", XML_NS)
            if run_defaults is None:
                return defaults

            r_fonts = run_defaults.find("w:rFonts", XML_NS)
            if r_fonts is not None:
                font_name = (
                    r_fonts.get(qn("w:ascii"))
                    or r_fonts.get(qn("w:hAnsi"))
                    or r_fonts.get(qn("w:cs"))
                )
                if font_name:
                    defaults["font_name"] = font_name

            sz_elem = run_defaults.find("w:sz", XML_NS)
            if sz_elem is not None:
                sz_val = sz_elem.get(qn("w:val"))
                if sz_val is not None:
                    defaults["font_size_pt"] = int(sz_val) / 2.0

            color_elem = run_defaults.find("w:color", XML_NS)
            if color_elem is not None:
                color_val = color_elem.get(qn("w:val"))
                if color_val and color_val.lower() != "auto":
                    defaults["color_rgb"] = color_val.upper()

        except (AttributeError, KeyError, ValueError, TypeError, etree.XMLSyntaxError) as e:
            self.logger.debug(
                f"Failed to extract document defaults: {str(e)}")

        return defaults

    def _extract_styles(self, document: DocumentObject) -> list[dict[str, Any]]:
        """Extract paragraph and character styles."""
        styles = []
        try:
            for style in document.styles:
                style_dict = {
                    "style_id": style.style_id,
                    "name": style.name,
                    "type": str(style.type) if style.type else None,
                    "font": None,
                }

                # Try to extract font info if it's a paragraph or character style
                try:
                    font = style.font
                    if font:
                        style_dict["font"] = {
                            "name": font.name,
                            "size_pt": font.size.pt if font.size else None,
                            "bold": font.bold,
                            "italic": font.italic,
                            "underline": font.underline,
                            "color_rgb": str(
                                font.color.rgb) if font.color and font.color.rgb else None,
                        }
                except Exception:
                    pass

                styles.append(style_dict)
        except Exception as e:
            self.logger.debug(f"Failed to extract styles: {str(e)}")

        return styles

    def _extract_paragraph(
        self,
        paragraph: Paragraph,
        index: int,
        document: DocumentObject,
        media_index: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Extract paragraph with formatting."""
        runs = [self._extract_run(run, i, media_index)
                for i, run in enumerate(paragraph.runs)]

        alignment = None
        if paragraph.alignment is not None:
            alignment = paragraph.alignment.name

        paragraph_element = paragraph._element
        p_pr = paragraph_element.pPr
        num_pr = p_pr.numPr if p_pr is not None else None

        list_info = None
        if num_pr is not None:
            num_id = num_pr.numId.val if num_pr.numId is not None else None
            ilvl = num_pr.ilvl.val if num_pr.ilvl is not None else None
            list_info = {
                "num_id": int(num_id) if num_id is not None else None,
                "level": int(ilvl) if ilvl is not None else None,
            }

        style_name = paragraph.style.name if paragraph.style else None
        numbering_format = self._resolve_list_formatting(list_info, document)

        is_bullet = bool(style_name and "bullet" in style_name.lower())
        is_numbered = bool(style_name and "number" in style_name.lower())
        if numbering_format:
            fmt = numbering_format.split(":", 1)[0].lower()
            if fmt in {"bullet"}:
                is_bullet = True
            else:
                is_numbered = True

        return {
            "index": index,
            "text": paragraph.text,
            "style": style_name,
            "alignment": alignment,
            "is_bullet": is_bullet or list_info is not None,
            "is_numbered": is_numbered,
            "list_info": list_info,
            "numbering_format": numbering_format,
            "list_level": list_info.get("level") if list_info else None,
            "runs": runs,
        }

    def _resolve_list_formatting(self, list_info: dict[str, Any] | None, document: DocumentObject) -> str | None:
        """Resolve abstract numbering format to human-readable list format."""
        if not list_info:
            return None

        num_id = list_info.get("num_id")
        ilvl = list_info.get("level") or 0
        if num_id is None:
            return None

        try:
            numbering_part = document.part.numbering_part
            if numbering_part is None:
                return None

            root = numbering_part.element
            for num in root.findall("w:num", XML_NS):
                if int(num.get(qn("w:numId")) or -1) == num_id:
                    abstract_elem = num.find("w:abstractNumId", XML_NS)
                    if abstract_elem is None:
                        return None
                    abstract_id = int(abstract_elem.get(qn("w:val")) or -1)
                    if abstract_id < 0:
                        return None

                    for abs_num in root.findall("w:abstractNum", XML_NS):
                        if int(abs_num.get(qn("w:abstractNumId")) or -1) != abstract_id:
                            continue
                        for lvl in abs_num.findall("w:lvl", XML_NS):
                            if int(lvl.get(qn("w:ilvl")) or -1) != ilvl:
                                continue
                            num_fmt = lvl.find("w:numFmt", XML_NS)
                            lvl_text = lvl.find("w:lvlText", XML_NS)
                            if num_fmt is not None and lvl_text is not None:
                                fmt = num_fmt.get(qn("w:val"))
                                text = lvl_text.get(qn("w:val"))
                                if fmt and text:
                                    return f"{fmt}:{text}"
                            return None
                    return None
        except (AttributeError, ValueError, TypeError, KeyError):
            return None

        return None

    def _extract_run(self, run: Run, index: int, media_index: dict[str, dict[str, Any]]) -> dict[str, Any]:
        """Extract text run with formatting."""
        text_color = None
        if run.font.color and run.font.color.rgb:
            text_color = str(run.font.color.rgb)

        embedded_media = []
        drawing_blips = run._element.xpath(".//*[local-name()='blip']")
        for blip in drawing_blips:
            rel_id = blip.get(qn("r:embed"))
            if rel_id and rel_id in media_index:
                embedded_media.append(dict(media_index[rel_id]))

        return {
            "index": index,
            "text": run.text,
            "bold": run.font.bold,
            "italic": run.font.italic,
            "underline": run.font.underline,
            "font_name": run.font.name,
            "font_size_pt": run.font.size.pt if run.font.size else None,
            "color_rgb": text_color,
            "embedded_media": embedded_media,
        }

    def _extract_table(
        self,
        table: Table,
        index: int,
        document: DocumentObject,
        media_index: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Extract table with cells and content."""
        rows = []
        for row_idx, row in enumerate(table.rows):
            cells = []
            for cell in row.cells:
                cell_text = "\n".join(
                    [p.text for p in cell.paragraphs if p.text])

                cells.append({
                    "text": cell_text,
                    "paragraphs": [
                        self._extract_paragraph(p, i, document, media_index)
                        for i, p in enumerate(cell.paragraphs)
                    ],
                })

            rows.append({"cells": cells, "row_index": row_idx})

        return {
            "index": index,
            "row_count": len(table.rows),
            "column_count": len(table.columns),
            "style": table.style.name if table.style else None,
            "rows": rows,
        }
