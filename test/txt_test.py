#!/Users/MAC/Desktop/ai-testing/document-playground/myvenv/bin/python3
"""Single-file TXT extract -> edit -> generate API runner.

Targets complex_sample.txt by default.

TXT format characteristics (shapes which ops are meaningful):
  - No tables — everything is paragraphs (including pseudo-table rows)
  - No inline formatting — runs have bold/italic=None, text is raw
  - Bullet lines (- / * / +) become is_bullet=True paragraphs
  - Numbered lines (1. / 1) ) become is_numbered=True paragraphs
  - Blank lines split blocks; contiguous non-blank non-list lines merge
  - No code_fence_language, no hyperlinks, no media

Ops exercised (all 10 edit ops):

  Generic JSON-pointer ops:
    add     — append a new paragraph at end via /paragraphs/-
    replace — overwrite paragraph text by pointer (title + 2 others)
    remove  — delete a specific run from a paragraph

  Advanced paragraph ops:
    replace_text          — find-and-replace within a paragraph (×6)
    insert_paragraph_after — insert paragraphs at multiple anchors (×4)
    remove_paragraph      — delete paragraphs by logical index (×3)
    remove_empty_paragraphs — prune blank paragraphs

  Table ops — skipped with warnings (TXT produces no tables).
    insert_table_after, remove_table, insert_table_row, remove_table_row,
    insert_table_column, remove_table_column — all emit [SKIP] messages
    explaining why, so the runner still documents coverage intent.

Run:
    cd temp-doc/test
    python3 txt_test.py
    python3 txt_test.py --output-format text
    python3 txt_test.py --file complex_sample.txt --output-format text
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
DEFAULT_FILE = "complex_sample.txt"

PARAGRAPH_0_TEXT_PATH = "/paragraphs/0/text"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _infer_extension(file_name: str) -> str:
    return Path(file_name).suffix.lower().lstrip(".")


def _content_type_for_file(file_name: str) -> str:
    guessed, _ = mimetypes.guess_type(file_name)
    return guessed or "text/plain"


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
    }.get(output_format.lower(), "txt")


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
    Generic JSON-pointer ops: add, replace, remove.

    (A) add — append a complete paragraph dict at the end of /paragraphs
    (B) replace — overwrite the document title (para 0) via pointer
    (C) remove — delete runs[0] from para 2 (first body paragraph)
    """
    # ── (A) add ───────────────────────────────────────────────────────────────
    # Supply a full ExtractedParagraph-compatible dict; a plain string would
    # pass _add_value but fail ExtractedData.model_validate() with 422.
    _add_text = (
        "Auto-appended via generic JSON-pointer add op — "
        "appended at the end of the paragraphs collection."
    )
    instructions.append(
        {
            "op": "add",
            "path": "/paragraphs/-",
            "value": {
                "index": 9999,
                "style": None,
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
    instructions.append(
        {
            "op": "replace",
            "path": PARAGRAPH_0_TEXT_PATH,
            "value": "COMPLEX TEXT TEST DOCUMENT — Edited by TXT Pipeline Runner",
        }
    )

    # ── (C) remove ────────────────────────────────────────────────────────────
    # Remove runs[0] from para 2 (the "======" separator line, if present).
    # Guard: only emit if para 2 exists and has at least one run.
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
    replace_text — 6 operations targeting different paragraph types:
      key-value line, JSON-like block, numbered list item,
      checklist item, section heading, free-text body.
    """
    # 1. Key-value block — flag value
    _append_replace_text_if_found(
        instructions, paragraphs,
        "strict_validation = true",
        "strict_validation = true", "strict_validation = false",
        label="key-value strict_validation",
    )

    # 2. JSON-like block — flag inside multi-line paragraph
    _append_replace_text_if_found(
        instructions, paragraphs,
        "async_pipeline",
        "async_pipeline\": false", "async_pipeline\": true",
        label="JSON async_pipeline flag",
    )

    # 3. Numbered list — retry count
    _append_replace_text_if_found(
        instructions, paragraphs,
        "max retry attempts",
        "Max retry attempts = 4", "Max retry attempts = 6",
        label="numbered list retry count",
    )

    # 4. Checklist — first pending item
    _append_replace_text_if_found(
        instructions, paragraphs,
        "add malformed payload",
        "Add malformed payload sample", "Add malformed payload sample [UPDATED]",
        label="checklist pending item",
    )

    # 5. Section heading — date in header block (count=1, first occurrence)
    _append_replace_text_if_found(
        instructions, paragraphs,
        "date: 2026-04-21",
        "2026-04-21", "2026-04-25",
        count=1,
        label="header date",
    )

    # 6. Free-text body — quote paragraph
    _append_replace_text_if_found(
        instructions, paragraphs,
        "reliability is not accidental",
        "Reliability is not accidental. It is engineered, measured, and improved.",
        "Reliability is not accidental. It is engineered, measured, improved, and automated.",
        label="quote paragraph",
    )


def _build_insert_paragraph_ops(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
) -> None:
    """
    insert_paragraph_after — 4 anchors:
      index 0 (after title)
      index 2 (after separator / metadata block)
      index 5 (mid-document)
      index 20 (deep anchor)
    """
    # After title
    instructions.append(
        {
            "op": "insert_paragraph_after",
            "index": 0,
            "value": (
                "Inserted summary paragraph: this run validates "
                "TXT extract -> edit -> generate fidelity."
            ),
        }
    )

    # After index 2
    instructions.append(
        {
            "op": "insert_paragraph_after",
            "index": 2,
            "value": "Second inserted paragraph: anchor remapping after first insert.",
        }
    )

    # After index 5
    instructions.append(
        {
            "op": "insert_paragraph_after",
            "index": 5,
            "value": "Third inserted paragraph: validates paragraph index remapping chain.",
        }
    )

    # After index 20 — only if document has enough paragraphs
    if len(paragraphs) > 20:
        instructions.append(
            {
                "op": "insert_paragraph_after",
                "index": 20,
                "value": (
                    "Fourth inserted paragraph: mid-document anchor — "
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
    """remove_paragraph at 3 progressively deeper indices."""
    for target_idx in (10, 20, 30):
        if len(paragraphs) > target_idx:
            instructions.append(
                {"op": "remove_paragraph", "index": target_idx})
        else:
            print(
                f"  [WARN] remove_paragraph({target_idx}) skipped: "
                f"only {len(paragraphs)} paragraphs extracted"
            )


def _build_table_skip_notes() -> None:
    """
    TXT extraction produces no tables — document this explicitly so the
    test log shows intentional coverage decisions, not silent omissions.
    """
    skipped = [
        "insert_table_after",
        "remove_table",
        "insert_table_row",
        "remove_table_row",
        "insert_table_column",
        "remove_table_column",
    ]
    for op in skipped:
        print(
            f"  [SKIP] {op} — TXT format produces no tables; op not applicable")


def _build_txt_instructions(extracted: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Build the full ordered instruction list for the TXT pipeline run.

    Groups (in application order):
      1. Generic JSON-pointer ops: add, replace, remove
      2. replace_text ×6
      3. insert_paragraph_after ×4
      4. remove_paragraph ×3
      5. remove_empty_paragraphs
      6. Table ops → all skipped with [SKIP] log messages
    """
    instructions: list[dict[str, Any]] = []
    paragraphs: list[dict[str, Any]] = extracted.get("paragraphs") or []
    tables: list[dict[str, Any]] = extracted.get("tables") or []

    _build_generic_ops(instructions, paragraphs)
    _build_replace_text_ops(instructions, paragraphs)
    _build_insert_paragraph_ops(instructions, paragraphs)
    _build_remove_paragraph_ops(instructions, paragraphs)
    instructions.append({"op": "remove_empty_paragraphs"})

    if tables:
        print(
            f"  [INFO] {len(tables)} table(s) found — "
            "running table ops (unexpected for TXT input)"
        )
    else:
        _build_table_skip_notes()

    return instructions


# ── Pipeline runner ────────────────────────────────────────────────────────────

def run_txt_pipeline(
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
            f"[1/3] Extracting {file_name} ({len(file_bytes)/1024:.1f} KB) ..."
        )
        t0 = time.perf_counter()
        extract_response = client.post(
            extract_url,
            params={"include_media": str(include_media).lower()},
            files={"file": (file_name, file_bytes, file_content_type)},
        )
        if extract_response.is_error:
            print(
                f"  [ERROR] Extract {extract_response.status_code}: "
                f"{extract_response.text[:2000]}"
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
        instructions = _build_txt_instructions(extracted_data)
        if not instructions:
            raise ValueError(
                "No edit instructions could be generated for input payload"
            )

        op_counts: dict[str, int] = {}
        for instr in instructions:
            op = str(instr.get("op") or "unknown")
            op_counts[op] = op_counts.get(op, 0) + 1
        op_summary = "  ".join(
            f"{op}×{n}" for op, n in sorted(op_counts.items())
        )
        print(f"     instructions count: {len(instructions)}  ({op_summary})")

        t0 = time.perf_counter()
        edit_request = {
            "extracted_data": extracted_data,
            "instructions": instructions,
        }
        edit_response = client.post(edit_url, json=edit_request)
        if edit_response.is_error:
            print(
                f"  [ERROR] Edit {edit_response.status_code}: "
                f"{edit_response.text[:2000]}"
            )
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
            if output_format.lower() in {"text", "txt"}
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
        if generate_response.is_error:
            print(
                f"  [ERROR] Generate {generate_response.status_code}: "
                f"{generate_response.text[:2000]}"
            )
        generate_response.raise_for_status()
        generate_ms = round((time.perf_counter() - t0) * 1000)

        generated_bytes = generate_response.content
        generated_name = _extract_filename_from_disposition(
            generate_response.headers.get("content-disposition")
        ) or f"{output_base}.{_extension_for_output_format(output_format)}"

        if "." not in generated_name:
            generated_name = (
                f"{generated_name}.{_extension_for_output_format(output_format)}"
            )

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
            "Run single-file TXT extract->edit->generate pipeline "
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
        default="text",
        choices=["docx", "pdf", "pptx", "html", "markdown", "text"],
        help="Format for generated output (default: text)",
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

    output_path = run_txt_pipeline(
        file_name=args.file,
        output_format=args.output_format,
        include_media=args.include_media,
        api_base_url=args.api_base_url,
        write_debug_json=args.write_debug_json,
    )
    print(f"SUCCESS: generated file saved at {output_path}")


if __name__ == "__main__":
    main()
