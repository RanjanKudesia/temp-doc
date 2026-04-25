#!/Users/MAC/Desktop/ai-testing/document-playground/myvenv/bin/python3
"""Single-file Markdown extract -> edit -> generate API runner.

Targets complex_sample.md by default and exercises EVERY available edit op:

  Generic JSON-pointer ops (3 ops):
    add     — append a new paragraph at the end of the paragraphs array
    replace — overwrite a field value by pointer (title + table cells)
    remove  — delete a field/element by pointer (runs[0] from a paragraph)

  Advanced paragraph ops (6 ops):
    replace_text          — find-and-replace inside a string field (× 8 variants)
    insert_paragraph_after — insert new paragraph at multiple anchor positions
    remove_paragraph      — delete paragraphs by logical index (× 3 calls)
    remove_empty_paragraphs — prune blank paragraphs from entire document

  Advanced table ops (6 ops):
    replace           — overwrite table cell text (tables 0, 1, 2)
    insert_table_after — insert a brand-new table after an existing table
    remove_table      — remove a table by logical index
    insert_table_row  — insert row into a table (tables 0, 1)
    remove_table_row  — delete row from a table (table 0)
    insert_table_column — add column to a table (table 0)
    remove_table_column — delete column from a table (table 0)

Run:
    cd temp-doc/test
    python3 md_test.py
    python3 md_test.py --output-format markdown
    python3 md_test.py --file complex_sample.md --output-format markdown
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import mimetypes
import re
import time
from pathlib import Path
from typing import Any

import httpx


TEST_FILES_DIR = Path("../app/helper/test-files")
DEFAULT_FILE = "complex_sample.md"

PARAGRAPH_0_TEXT_PATH = "/paragraphs/0/text"
TABLE_0_PATH = "/tables/0"
TABLE_0_CELL_0_TEXT_PATH = "/tables/0/rows/0/cells/0/text"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _infer_extension(file_name: str) -> str:
    return Path(file_name).suffix.lower().lstrip(".")


def _content_type_for_file(file_name: str) -> str:
    guessed, _ = mimetypes.guess_type(file_name)
    return guessed or "application/octet-stream"


def _extract_filename_from_disposition(content_disposition: str | None) -> str | None:
    if not content_disposition:
        return None
    match = re.search(r'filename="?([^";]+)"?', content_disposition)
    if not match:
        return None
    return match.group(1).strip()


def _extension_for_output_format(output_format: str) -> str:
    return {
        "docx": "docx",
        "pdf": "pdf",
        "pptx": "pptx",
        "html": "html",
        "markdown": "md",
        "text": "txt",
    }.get(output_format.lower(), "md")


def _text_path(idx: int) -> str:
    return f"/paragraphs/{idx}/text"


def _find_para_by_contains(
    paragraphs: list[dict[str, Any]], needle: str
) -> int | None:
    needle_norm = needle.strip().lower()
    if not needle_norm:
        return None
    for idx, paragraph in enumerate(paragraphs):
        text = str((paragraph or {}).get("text") or "").strip().lower()
        if needle_norm in text:
            return idx
    return None


def _append_replace_text_if_found(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
    needle: str,
    old_value: str,
    new_value: str,
    count: int | None = None,
    *,
    label: str = "",
) -> bool:
    idx = _find_para_by_contains(paragraphs, needle)
    tag = label or needle
    if idx is None:
        print(
            f"  [WARN] replace_text skipped: paragraph containing {tag!r} not found")
        return False
    instr: dict[str, Any] = {
        "op": "replace_text",
        "path": _text_path(idx),
        "old_value": old_value,
        "new_value": new_value,
    }
    if count is not None:
        instr["count"] = count
    instructions.append(instr)
    return True


# ── Instruction builders ───────────────────────────────────────────────────────

def _build_generic_ops(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
) -> None:
    """
    Generic JSON-pointer ops: add, replace (paragraph title), remove (a run).

    These are placed first so indices are stable before any inserts/removes.
    """
    # ── (A) add ──────────────────────────────────────────────────────────────
    # Append a brand-new paragraph at the very end of the paragraphs array
    # using the JSON-pointer '-' sentinel (means "append to array").
    # NOTE: _add_value does a raw parent.append(value) with no payload builder,
    # so we must supply a complete ExtractedParagraph-compatible dict — a plain
    # string would survive in the list but fail ExtractedData.model_validate().
    _add_text = (
        "Auto-appended via generic JSON-pointer add op — "
        "inserted at the end of the paragraphs collection."
    )
    instructions.append(
        {
            "op": "add",
            "path": "/paragraphs/-",
            "value": {
                "index": 9999,
                "style": "Normal",
                "text": _add_text,
                "is_bullet": False,
                "is_numbered": False,
                "list_info": None,
                "numbering_format": None,
                "list_level": None,
                "alignment": None,
                "direction": None,
                "runs": [
                    {
                        "index": 0,
                        "text": _add_text,
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
                ],
            },
        }
    )

    # ── (B) replace ───────────────────────────────────────────────────────────
    # Overwrite the document title via generic replace pointer.
    instructions.append(
        {
            "op": "replace",
            "path": PARAGRAPH_0_TEXT_PATH,
            "value": "# Complex Markdown Test Document — Edited by MD Pipeline Runner",
        }
    )

    # ── (C) remove ────────────────────────────────────────────────────────────
    # Remove the first run from paragraph 2 (metadata line "**Owner:** ...").
    # Guard: only emit if para 2 has at least one run so the pointer is valid.
    if len(paragraphs) > 2:
        runs = (paragraphs[2] or {}).get("runs") or []
        if runs:
            instructions.append(
                {"op": "remove", "path": "/paragraphs/2/runs/0"})
        else:
            print("  [WARN] remove /paragraphs/2/runs/0 skipped: runs array empty")
    else:
        print("  [WARN] remove /paragraphs/2/runs/0 skipped: fewer than 3 paragraphs")


def _build_replace_text_ops(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
) -> None:
    """
    replace_text — 8 distinct find-and-replace operations across the document,
    targeting different content areas: blockquotes, code blocks, nested lists,
    header metadata, headings, and inline text.
    """
    # 1. Blockquote body
    _append_replace_text_if_found(
        instructions, paragraphs,
        "reliability and clarity",
        "not side effects", "not accidents",
        label="blockquote body",
    )

    # 2. JSON code block — flag name
    _append_replace_text_if_found(
        instructions, paragraphs,
        "strict_validation",
        "strict_validation", "strict_mode",
        label="JSON code block flag",
    )

    # 3. Nested list item — jitter setting
    _append_replace_text_if_found(
        instructions, paragraphs,
        "jitter enabled",
        "Jitter enabled", "Jitter enabled (default)",
        label="nested list jitter",
    )

    # 4. Header metadata — owner/team name (first occurrence only)
    _append_replace_text_if_found(
        instructions, paragraphs,
        "platform team",
        "Platform Team", "Platform Engineering Team",
        count=1,
        label="header team name",
    )

    # 5. Header metadata — date (first occurrence only)
    _append_replace_text_if_found(
        instructions, paragraphs,
        "date:",
        "2026-04-21", "2026-04-25",
        count=1,
        label="header date",
    )

    # 6. Section heading — TypeScript snippet heading
    _append_replace_text_if_found(
        instructions, paragraphs,
        "typescript",
        "TypeScript", "TypeScript/ES2024",
        count=1,
        label="TypeScript heading",
    )

    # 7. Deep nested list — circuit breaker threshold line
    _append_replace_text_if_found(
        instructions, paragraphs,
        "circuit breaker threshold",
        "Circuit breaker threshold: 5 failures / 30 s",
        "Circuit breaker threshold: 3 failures / 60 s",
        label="circuit breaker list item",
    )

    # 8. Service description paragraph — replace a proper noun
    _append_replace_text_if_found(
        instructions, paragraphs,
        "insight-service",
        "insight-service", "insight-svc",
        count=1,
        label="service name in architecture section",
    )


def _build_insert_paragraph_ops(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
) -> None:
    """
    insert_paragraph_after — 4 calls:
      * Rich multi-run paragraph after index 0 (after title)
      * Plain string after index 2
      * Plain string after index 5
      * Plain string after index 20 (mid-document anchor)
    """
    # After title (index 0) — multi-run rich paragraph
    instructions.append(
        {
            "op": "insert_paragraph_after",
            "index": 0,
            "value": {
                "text": (
                    "Inserted summary paragraph: this run validates "
                    "MD extract → edit → generate fidelity."
                ),
                "style": "Normal",
                "runs": [
                    {
                        "index": 0,
                        "text": "Inserted summary paragraph: this run validates ",
                        "bold": False,
                        "italic": False,
                        "underline": False,
                        "strikethrough": False,
                        "code": False,
                        "color_rgb": None,
                        "highlight_color": None,
                        "hyperlink_url": None,
                        "embedded_media": [],
                    },
                    {
                        "index": 1,
                        "text": "MD extract → edit → generate",
                        "bold": True,
                        "italic": False,
                        "underline": False,
                        "strikethrough": False,
                        "code": False,
                        "color_rgb": None,
                        "highlight_color": None,
                        "hyperlink_url": None,
                        "embedded_media": [],
                    },
                    {
                        "index": 2,
                        "text": " fidelity.",
                        "bold": False,
                        "italic": False,
                        "underline": False,
                        "strikethrough": False,
                        "code": False,
                        "color_rgb": None,
                        "highlight_color": None,
                        "hyperlink_url": None,
                        "embedded_media": [],
                    },
                ],
            },
        }
    )

    # After index 2 — plain string
    instructions.append(
        {
            "op": "insert_paragraph_after",
            "index": 2,
            "value": (
                "Second inserted paragraph: stress-testing anchor remapping "
                "after first insert."
            ),
        }
    )

    # After index 5 — plain string
    instructions.append(
        {
            "op": "insert_paragraph_after",
            "index": 5,
            "value": "Third inserted paragraph: validates paragraph index remapping chain.",
        }
    )

    # After index 20 — plain string (mid-document anchor)
    if len(paragraphs) > 20:
        instructions.append(
            {
                "op": "insert_paragraph_after",
                "index": 20,
                "value": (
                    "Fourth inserted paragraph: mid-document anchor insert — "
                    "exercises remapping across a larger offset."
                ),
            }
        )
    else:
        print(
            f"  [WARN] insert_paragraph_after(20) skipped: "
            f"only {len(paragraphs)} paragraphs extracted"
        )


def _build_remove_paragraph_ops(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
) -> None:
    """
    remove_paragraph — 3 calls at progressively deeper indices.
    Skipped with a warning if the document is too small.
    """
    for target_idx in (15, 25, 35):
        if len(paragraphs) > target_idx:
            instructions.append(
                {"op": "remove_paragraph", "index": target_idx})
        else:
            print(
                f"  [WARN] remove_paragraph({target_idx}) skipped: "
                f"only {len(paragraphs)} paragraphs extracted"
            )


def _build_table_cell_replace_ops(
    instructions: list[dict[str, Any]],
    tables: list[dict[str, Any]],
) -> None:
    """
    replace — overwrite top-left cell of tables 0, 1, and 2 (if present).
    This exercises generic replace via JSON pointer on nested table paths.
    """
    labels = [
        "KPI (updated by MD pipeline runner)",
        "Region (updated by MD pipeline runner)",
        "Decision (updated by MD pipeline runner)",
    ]
    for t_idx, label in enumerate(labels):
        if len(tables) > t_idx:
            instructions.append(
                {
                    "op": "replace",
                    "path": f"/tables/{t_idx}/rows/0/cells/0/text",
                    "value": label,
                }
            )
        else:
            print(
                f"  [WARN] replace table[{t_idx}] cell skipped: "
                f"only {len(tables)} tables extracted"
            )


def _build_table_row_col_ops(
    instructions: list[dict[str, Any]],
    tables: list[dict[str, Any]],
) -> None:
    """
    insert_table_row, remove_table_row, insert_table_column, remove_table_column
    exercised on table 0 (KPI) and insert_table_row on table 1 (Region Breakdown).
    """
    if not tables:
        print("  [WARN] No tables found — row/column ops skipped")
        return

    # Table 0 — all four row/col operations
    instructions.append(
        {
            "op": "insert_table_row",
            "path": TABLE_0_PATH,
            "index": 0,
            "value": "Inserted row — MD runner",
        }
    )
    instructions.append(
        {
            "op": "insert_table_column",
            "path": TABLE_0_PATH,
            "index": 0,
            "value": "Trend",
        }
    )
    instructions.append(
        {
            "op": "remove_table_row",
            "path": TABLE_0_PATH,
            "index": 2,
        }
    )
    instructions.append(
        {
            "op": "remove_table_column",
            "path": TABLE_0_PATH,
            "index": 2,
        }
    )

    # Table 1 — insert_table_row (region breakdown table)
    if len(tables) > 1:
        instructions.append(
            {
                "op": "insert_table_row",
                "path": "/tables/1",
                "index": 0,
                "value": "Inserted row — Region table",
            }
        )
    else:
        print("  [WARN] insert_table_row table[1] skipped: only 1 table found")


def _build_insert_remove_table_ops(
    instructions: list[dict[str, Any]],
    tables: list[dict[str, Any]],
) -> None:
    """
    insert_table_after — insert a 2-row × 3-column summary table after table 3
      (the HTTP status-codes table).
    remove_table — remove the Changelog table (originally index 5; after the
      insert it becomes index 6).

    Both ops are skipped with a warning if the table count is too low.
    """
    # insert_table_after index 3 (HTTP status codes table)
    INSERT_AFTER_IDX = 3
    if len(tables) > INSERT_AFTER_IDX:
        instructions.append(
            {
                "op": "insert_table_after",
                "index": INSERT_AFTER_IDX,
                "value": [
                    # Row 0 — header row
                    ["Stage", "Op", "Status"],
                    # Row 1 — data row
                    ["extract → edit → generate",
                        "MD round-trip", "Inserted by MD runner"],
                ],
            }
        )
    else:
        print(
            f"  [WARN] insert_table_after({INSERT_AFTER_IDX}) skipped: "
            f"only {len(tables)} tables extracted"
        )

    # remove_table — Changelog table (originally index 5, now index 6 after insert)
    # Original table count: 7 (indices 0-6). After insert_table_after(3),
    # Changelog was index 5 → becomes index 6.
    REMOVE_IDX = 6 if len(tables) > INSERT_AFTER_IDX else 5
    REQUIRED = REMOVE_IDX + 1
    if len(tables) >= REQUIRED:
        instructions.append({"op": "remove_table", "index": REMOVE_IDX})
    else:
        print(
            f"  [WARN] remove_table({REMOVE_IDX}) skipped: "
            f"only {len(tables)} tables extracted (need {REQUIRED})"
        )


def _build_md_instructions(extracted: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Build the full ordered instruction list for the markdown pipeline run.

    Instruction groups (in application order):
      1. Generic JSON-pointer ops: add, replace, remove
      2. replace_text × 8
      3. insert_paragraph_after × 4
      4. remove_paragraph × 3
      5. remove_empty_paragraphs
      6. Table cell replace (tables 0, 1, 2)
      7. Table row/col ops (tables 0, 1)
      8. insert_table_after + remove_table
    """
    instructions: list[dict[str, Any]] = []
    paragraphs: list[dict[str, Any]] = extracted.get("paragraphs") or []
    tables: list[dict[str, Any]] = extracted.get("tables") or []

    _build_generic_ops(instructions, paragraphs)
    _build_replace_text_ops(instructions, paragraphs)
    _build_insert_paragraph_ops(instructions, paragraphs)
    _build_remove_paragraph_ops(instructions, paragraphs)
    instructions.append({"op": "remove_empty_paragraphs"})
    _build_table_cell_replace_ops(instructions, tables)
    _build_table_row_col_ops(instructions, tables)
    _build_insert_remove_table_ops(instructions, tables)

    return instructions


# ── Pipeline runner ────────────────────────────────────────────────────────────

def run_md_pipeline(
    file_name: str,
    output_format: str,
    include_media: bool,
    api_base_url: str,
    write_debug_json: bool,
) -> Path:
    input_path = TEST_FILES_DIR / file_name
    if not input_path.exists() or not input_path.is_file():
        raise FileNotFoundError(f"Test input file not found: {input_path}")

    extension = _infer_extension(file_name)
    file_bytes = input_path.read_bytes()
    file_content_type = _content_type_for_file(file_name)

    extract_url = f"{api_base_url.rstrip('/')}/extract"
    edit_url = f"{api_base_url.rstrip('/')}/edit"
    generate_url = f"{api_base_url.rstrip('/')}/generate"

    t_total = time.perf_counter()

    with httpx.Client(timeout=120) as client:
        # ── Stage 1: Extract ──────────────────────────────────────────────────
        print(
            f"[1/3] Extracting {file_name} ({len(file_bytes)/1024:.1f} KB) ...")
        t0 = time.perf_counter()
        extract_response = client.post(
            extract_url,
            params={"include_media": str(include_media).lower()},
            files={"file": (file_name, file_bytes, file_content_type)},
        )
        extract_response.raise_for_status()
        extract_payload = extract_response.json()
        extract_ms = round((time.perf_counter() - t0) * 1000)
        extracted_data = extract_payload["extracted_data"]
        print(
            f"     done in {extract_ms} ms  "
            f"| response {len(extract_response.content)/1024:.1f} KB  "
            f"| paragraphs={len(extracted_data.get('paragraphs') or [])}  "
            f"| tables={len(extracted_data.get('tables') or [])}"
        )

        # ── Stage 2: Edit ─────────────────────────────────────────────────────
        print("[2/3] Building and applying edit instructions ...")
        instructions = _build_md_instructions(extracted_data)
        if not instructions:
            raise ValueError(
                "No edit instructions could be generated for input payload")

        # Summarise op-type coverage for the run log
        op_counts: dict[str, int] = {}
        for instr in instructions:
            op = str(instr.get("op") or "unknown")
            op_counts[op] = op_counts.get(op, 0) + 1
        op_summary = "  ".join(
            f"{op}×{n}" for op, n in sorted(op_counts.items()))
        print(f"     instructions count: {len(instructions)}  ({op_summary})")

        t0 = time.perf_counter()
        edit_request = {
            "extension": extension,
            "extracted_data": extracted_data,
            "instructions": instructions,
        }
        edit_response = client.post(edit_url, json=edit_request)
        if edit_response.is_error:
            print(
                f"  [ERROR] Edit {edit_response.status_code}: {edit_response.text[:2000]}")
        edit_response.raise_for_status()
        edited_payload = edit_response.json()
        edit_ms = round((time.perf_counter() - t0) * 1000)
        edited_data = edited_payload["extracted_data"]
        print(
            f"     done in {edit_ms} ms  "
            f"| response {len(edit_response.content)/1024:.1f} KB  "
            f"| paragraphs={len(edited_data.get('paragraphs') or [])}  "
            f"| tables={len(edited_data.get('tables') or [])}"
        )

        # ── Stage 3: Generate ─────────────────────────────────────────────────
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_base = f"{input_path.stem}_edited_{timestamp}"
        generated_title = (
            None
            if output_format.lower() in {"markdown", "md"}
            else f"Edited output for {file_name}"
        )

        print(f"[3/3] Generating {output_format.upper()} output ...")
        t0 = time.perf_counter()
        generate_request = {
            "output_format": output_format,
            "extracted_data": edited_data,
            "file_name": output_base,
            "title": generated_title,
        }
        generate_response = client.post(generate_url, json=generate_request)
        generate_response.raise_for_status()
        generate_ms = round((time.perf_counter() - t0) * 1000)

        generated_bytes = generate_response.content
        generated_name = _extract_filename_from_disposition(
            generate_response.headers.get("content-disposition")
        ) or f"{output_base}.{_extension_for_output_format(output_format)}"

        if "." not in generated_name:
            generated_name = f"{generated_name}.{_extension_for_output_format(output_format)}"

        print(
            f"     done in {generate_ms} ms  "
            f"| output {len(generated_bytes)/1024:.1f} KB"
        )

    output_path = TEST_FILES_DIR / generated_name
    output_path.write_bytes(generated_bytes)

    total_ms = round((time.perf_counter() - t_total) * 1000)
    print(
        f"\n--- Pipeline complete in {total_ms} ms "
        f"(extract={extract_ms}ms  edit={edit_ms}ms  generate={generate_ms}ms) ---"
    )

    if write_debug_json:
        debug_payload_path = TEST_FILES_DIR / f"{output_base}.json"
        debug_payload_path.write_text(
            json.dumps(
                {
                    "source_file": file_name,
                    "extension": extension,
                    "output_format": output_format,
                    "instructions": instructions,
                    "instructions_count": len(instructions),
                    "extract_response": extract_payload,
                    "edit_response": edited_payload,
                    "generated_file": generated_name,
                    "api_base_url": api_base_url,
                    "api_endpoints": {
                        "extract": extract_url,
                        "edit": edit_url,
                        "generate": generate_url,
                    },
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"DEBUG JSON saved at {debug_payload_path}")

    return output_path


# ── CLI ────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run single-file markdown extract->edit->generate pipeline "
            "from app/helper/test-files"
        ),
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_FILE,
        help=f"File name inside app/helper/test-files (default: {DEFAULT_FILE})",
    )
    parser.add_argument(
        "--output-format",
        default="markdown",
        choices=["docx", "pdf", "pptx", "html", "markdown", "text"],
        help="Format for generated output (default: markdown)",
    )
    parser.add_argument(
        "--include-media",
        action="store_true",
        default=False,
        help="Include media payload during extraction",
    )
    parser.add_argument(
        "--api-base-url",
        default="http://localhost:8000",
        help="Base URL for temp-doc API (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--write-debug-json",
        action="store_true",
        default=True,
        help="Write debug JSON payload next to generated file (default: on)",
    )
    parser.add_argument(
        "--no-debug-json",
        dest="write_debug_json",
        action="store_false",
        help="Suppress writing the debug JSON payload",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    output_path = run_md_pipeline(
        file_name=args.file,
        output_format=args.output_format,
        include_media=args.include_media,
        api_base_url=args.api_base_url,
        write_debug_json=args.write_debug_json,
    )
    print(f"SUCCESS: generated file saved at {output_path}")


if __name__ == "__main__":
    main()
