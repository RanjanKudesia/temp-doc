"""Markdown extraction pipeline for temp-doc service."""

import logging
import re
from typing import Any


class MarkdownExtractionPipeline:
    """Extract Markdown content to JSON format."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, file_bytes: bytes) -> dict[str, Any]:
        """Extract Markdown and return JSON data."""
        try:
            text = file_bytes.decode("utf-8-sig", errors="replace")
        except Exception as e:
            raise ValueError(f"Failed to decode Markdown: {str(e)}") from e

        lines = text.splitlines()
        paragraphs: list[dict[str, Any]] = []
        tables: list[dict[str, Any]] = []
        media: list[dict[str, Any]] = []
        document_order: list[dict[str, Any]] = []

        paragraph_index = 0
        table_index = 0
        line_index = 0

        while line_index < len(lines):
            line = lines[line_index]
            stripped = line.strip()

            if not stripped:
                line_index += 1
                continue

            # Markdown table
            if self._is_table_start(lines, line_index):
                table_lines: list[str] = []
                while line_index < len(lines) and self._looks_like_table_row(lines[line_index]):
                    table_lines.append(lines[line_index])
                    line_index += 1

                table_rows = self._parse_table_lines(table_lines)
                if table_rows:
                    rows_payload: list[dict[str, Any]] = []
                    max_cols = max((len(row) for row in table_rows), default=0)
                    for row_idx, row in enumerate(table_rows):
                        cells = []
                        for col_idx, cell_text in enumerate(row):
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
                                    "runs": [{
                                        "index": 0,
                                        "text": cell_text,
                                        "bold": None,
                                        "italic": None,
                                        "underline": None,
                                        "font_name": None,
                                        "font_size_pt": None,
                                        "color_rgb": None,
                                        "highlight_color": None,
                                        "hyperlink_url": None,
                                        "embedded_media": [],
                                    }],
                                }],
                                "tables": [],
                                "cell_index": col_idx,
                            })
                        rows_payload.append(
                            {"row_index": row_idx, "cells": cells})

                    tables.append({
                        "index": table_index,
                        "row_count": len(rows_payload),
                        "column_count": max_cols,
                        "style": None,
                        "rows": rows_payload,
                        "source": {"format": "markdown"},
                    })
                    document_order.append(
                        {"type": "table", "index": table_index})
                    table_index += 1
                continue

            # Aggregate block lines (heading/bullet/number are single-line; plain text may span multiple)
            block_lines = [line]
            line_index += 1
            while line_index < len(lines):
                next_line = lines[line_index]
                next_stripped = next_line.strip()
                if not next_stripped:
                    break
                if self._is_structural_line(next_line) or self._is_table_start(lines, line_index):
                    break
                block_lines.append(next_line)
                line_index += 1

            paragraph = self._build_paragraph(block_lines, paragraph_index)
            media.extend(self._extract_inline_media(
                block_lines, paragraph_index))
            paragraphs.append(paragraph)
            document_order.append(
                {"type": "paragraph", "index": paragraph_index})
            paragraph_index += 1

        return {
            "metadata": {
                "source_type": "markdown",
                "extraction_mode": "markdown",
            },
            "document_order": document_order,
            "document_defaults": None,
            "styles": [],
            "paragraphs": paragraphs,
            "tables": tables,
            "media": media,
        }

    # ── paragraph builder ────────────────────────────────────────────────────

    def _build_paragraph(self, block_lines: list[str], paragraph_index: int) -> dict[str, Any]:
        raw = "\n".join(block_lines).strip()
        heading_level = None
        style = None
        is_bullet = False
        is_numbered = False
        numbering_format = None

        heading_match = re.match(r"^\s{0,3}(#{1,6})\s+(.*)$", raw)
        if heading_match:
            heading_level = len(heading_match.group(1))
            raw = heading_match.group(2).strip()
            style = f"Heading {heading_level}"
        else:
            bullet_match = re.match(r"^\s*[-*+]\s+(.*)$", raw)
            number_match = re.match(r"^\s*(\d+[.)])\s+(.*)$", raw)
            if bullet_match:
                is_bullet = True
                numbering_format = "bullet"
                raw = bullet_match.group(1).strip()
            elif number_match:
                is_numbered = True
                numbering_format = number_match.group(1)
                raw = number_match.group(2).strip()

        # Build inline-formatted runs from the text
        runs = self._parse_inline_runs(raw)

        return {
            "index": paragraph_index,
            "text": raw,
            "style": style,
            "is_bullet": is_bullet,
            "is_numbered": is_numbered,
            "list_info": {
                "kind": "bullet" if is_bullet else ("numbered" if is_numbered else None),
                "numbering_format": numbering_format,
            } if (is_bullet or is_numbered) else None,
            "numbering_format": numbering_format,
            "alignment": None,
            "runs": runs,
            "source": {"format": "markdown"},
        }

    def _parse_inline_runs(self, text: str) -> list[dict[str, Any]]:
        """Parse inline Markdown into styled runs (bold/italic/code/links)."""
        runs: list[dict[str, Any]] = []
        # Pattern priority: bold+italic > bold > italic > code > link > plain
        pattern = re.compile(
            r"(\*\*\*(?P<bi>[^*]+?)\*\*\*"
            r"|\*\*(?P<b>[^*]+?)\*\*"
            r"|__(?P<b2>[^_]+?)__"
            r"|(\*|_)(?P<i>[^*_]+?)(\*|_)"
            r"|`(?P<code>[^`]+?)`"
            r"|\[(?P<link_text>[^\]]+)\]\((?P<link_url>[^)]+)\)"
            r"|(?P<plain>[^*_`\[]+)"
            r")"
        )
        for m in pattern.finditer(text):
            bold = italic = False
            code = None
            url = None
            run_text = None

            if m.group("bi"):
                run_text = m.group("bi")
                bold = italic = True
            elif m.group("b"):
                run_text = m.group("b")
                bold = True
            elif m.group("b2"):
                run_text = m.group("b2")
                bold = True
            elif m.group("i"):
                run_text = m.group("i")
                italic = True
            elif m.group("code"):
                run_text = m.group("code")
                code = True
            elif m.group("link_text"):
                run_text = m.group("link_text")
                url = m.group("link_url")
            elif m.group("plain"):
                run_text = m.group("plain")

            if not run_text:
                continue

            runs.append({
                "index": len(runs),
                "text": run_text,
                "bold": bold or None,
                "italic": italic or None,
                "underline": None,
                "font_name": None,
                "font_size_pt": None,
                "color_rgb": None,
                "highlight_color": None,
                "hyperlink_url": url,
                "embedded_media": [],
                **({"code": True} if code else {}),
            })

        if not runs:
            runs.append({
                "index": 0,
                "text": text,
                "bold": None,
                "italic": None,
                "underline": None,
                "font_name": None,
                "font_size_pt": None,
                "color_rgb": None,
                "highlight_color": None,
                "hyperlink_url": None,
                "embedded_media": [],
            })

        return runs

    def _extract_inline_media(self, block_lines: list[str], paragraph_index: int) -> list[dict[str, Any]]:
        """Extract ![alt](src) image references from block lines."""
        media: list[dict[str, Any]] = []
        pattern = re.compile(r"!\[(?P<alt>[^\]]*)\]\((?P<target>[^)\s]+)\)")
        block_text = "\n".join(block_lines)
        for image_index, match in enumerate(pattern.finditer(block_text)):
            target = match.group("target")
            media.append({
                "relationship_id": f"md_p_{paragraph_index}_img_{image_index}",
                "content_type": None,
                "file_name": target.rsplit("/", 1)[-1],
                "local_file_path": target,
                "local_url": target,
                "width_emu": None,
                "height_emu": None,
                "alt_text": match.group("alt") or None,
            })
        return media

    # ── table helpers ────────────────────────────────────────────────────────

    def _is_table_start(self, lines: list[str], index: int) -> bool:
        if index + 1 >= len(lines):
            return False
        return (
            self._looks_like_table_row(lines[index])
            and self._looks_like_table_separator(lines[index + 1])
        )

    def _looks_like_table_row(self, line: str) -> bool:
        stripped = line.strip()
        return "|" in stripped and len([p for p in stripped.split("|") if p.strip()]) >= 2

    def _looks_like_table_separator(self, line: str) -> bool:
        stripped = line.strip().strip("|")
        if not stripped:
            return False
        parts = [p.strip() for p in stripped.split("|")]
        return all(bool(re.fullmatch(r":?-{3,}:?", p or "")) for p in parts if p)

    def _is_structural_line(self, line: str) -> bool:
        stripped = line.strip()
        if not stripped:
            return False
        return bool(
            re.match(r"^\s{0,3}(#{1,6})\s+", line)
            or re.match(r"^\s*[-*+]\s+", line)
            or re.match(r"^\s*\d+[.)]\s+", line)
        )

    def _parse_table_lines(self, lines: list[str]) -> list[list[str]]:
        if len(lines) < 2:
            return []
        rows: list[list[str]] = []
        for index, line in enumerate(lines):
            if index == 1 and self._looks_like_table_separator(line):
                continue
            stripped = line.strip().strip("|")
            rows.append([cell.strip() for cell in stripped.split("|")])
        return rows
