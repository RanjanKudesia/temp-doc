#!/Users/MAC/Desktop/ai-testing/document-playground/myvenv/bin/python3
"""Single-file PDF extract -> edit -> generate API runner.

Default input file:
    app/helper/test-files/machine-learning-roadmap-v2.pdf

Run:
    cd temp-doc/test
    ./pdf_test.py
    python3 pdf_test.py --output-format pdf
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
DEFAULT_FILE = "machine-learning-roadmap-v2.pdf"


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
    }.get(output_format.lower(), "pdf")


def _text_path(idx: int) -> str:
    return f"/paragraphs/{idx}/text"


def _first_token_for_replace(text: str) -> str | None:
    for token in re.findall(r"[A-Za-z][A-Za-z0-9_-]{3,}", text):
        if token.lower() not in {"this", "that", "with", "from", "your", "have", "will"}:
            return token
    return None


def _build_generic_ops(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
) -> None:
    add_text = (
        "Auto-appended via generic JSON-pointer add op - "
        "inserted at the end of the paragraphs collection."
    )
    instructions.append(
        {
            "op": "add",
            "path": "/paragraphs/-",
            "value": {
                "index": 9999,
                "style": "Normal",
                "text": add_text,
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
                        "text": add_text,
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

    if paragraphs:
        instructions.append(
            {
                "op": "replace",
                "path": "/paragraphs/0/text",
                "value": "PDF Pipeline Test Document - Edited by pdf_test.py",
            }
        )
    else:
        print("  [WARN] replace /paragraphs/0/text skipped: no paragraphs extracted")

    if len(paragraphs) > 2 and ((paragraphs[2] or {}).get("runs") or []):
        instructions.append({"op": "remove", "path": "/paragraphs/2/runs/0"})
    else:
        print("  [WARN] remove /paragraphs/2/runs/0 skipped: paragraph/runs missing")


def _build_replace_text_ops(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
    max_ops: int = 6,
) -> None:
    added = 0
    seen_tokens: set[str] = set()
    for idx, paragraph in enumerate(paragraphs):
        if added >= max_ops:
            break
        text = str((paragraph or {}).get("text") or "").strip()
        if len(text) < 8:
            continue
        old_value = _first_token_for_replace(text)
        if not old_value:
            continue
        key = old_value.lower()
        if key in seen_tokens:
            continue
        seen_tokens.add(key)
        instructions.append(
            {
                "op": "replace_text",
                "path": _text_path(idx),
                "old_value": old_value,
                "new_value": f"{old_value}_PDF",
                "count": 1,
            }
        )
        added += 1

    if added == 0:
        print("  [WARN] replace_text skipped: no suitable paragraph text found")


def _build_insert_paragraph_ops(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
) -> None:
    candidates = [
        (0, "Inserted paragraph A - after index 0."),
        (2, "Inserted paragraph B - after index 2."),
        (5, "Inserted paragraph C - after index 5."),
        (20, "Inserted paragraph D - after index 20."),
    ]
    for idx, value in candidates:
        if len(paragraphs) > idx:
            instructions.append(
                {
                    "op": "insert_paragraph_after",
                    "index": idx,
                    "value": value,
                }
            )
        else:
            print(
                f"  [WARN] insert_paragraph_after({idx}) skipped: "
                f"only {len(paragraphs)} paragraphs extracted"
            )


def _build_remove_paragraph_ops(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
) -> None:
    for target_idx in (10, 20, 30):
        if len(paragraphs) > target_idx:
            instructions.append(
                {"op": "remove_paragraph", "index": target_idx})
        else:
            print(
                f"  [WARN] remove_paragraph({target_idx}) skipped: "
                f"only {len(paragraphs)} paragraphs extracted"
            )


def _table_has_cell_text(table: dict[str, Any]) -> bool:
    rows = table.get("rows") or []
    if not rows:
        return False
    cells = (rows[0] or {}).get("cells") or []
    return bool(cells)


def _build_table_ops(
    instructions: list[dict[str, Any]],
    tables: list[dict[str, Any]],
) -> None:
    if not tables:
        for op in (
            "insert_table_after",
            "remove_table",
            "insert_table_row",
            "remove_table_row",
            "insert_table_column",
            "remove_table_column",
        ):
            print(f"  [SKIP] {op} - no tables extracted")
        return

    for idx, table in enumerate(tables[:3]):
        if _table_has_cell_text(table):
            instructions.append(
                {
                    "op": "replace",
                    "path": f"/tables/{idx}/rows/0/cells/0/text",
                    "value": f"PDF_Table{idx}_Header_Updated",
                }
            )

    instructions.append(
        {
            "op": "insert_table_row",
            "path": "/tables/0",
            "index": 0,
            "value": "Inserted row by pdf_test.py",
        }
    )
    instructions.append(
        {
            "op": "insert_table_column",
            "path": "/tables/0",
            "index": 0,
            "value": "Inserted col",
        }
    )
    instructions.append(
        {"op": "remove_table_row", "path": "/tables/0", "index": 1})
    instructions.append({"op": "remove_table_column",
                        "path": "/tables/0", "index": 1})

    insert_after_index = min(1, len(tables) - 1)
    instructions.append(
        {
            "op": "insert_table_after",
            "index": insert_after_index,
            "value": [
                ["Stage", "Operation", "Status"],
                ["extract-edit-generate", "pdf round-trip", "inserted"],
            ],
        }
    )
    instructions.append({"op": "remove_table", "index": 0})


def _build_pdf_instructions(extracted: dict[str, Any]) -> list[dict[str, Any]]:
    instructions: list[dict[str, Any]] = []
    paragraphs: list[dict[str, Any]] = extracted.get("paragraphs") or []
    tables: list[dict[str, Any]] = extracted.get("tables") or []

    _build_generic_ops(instructions, paragraphs)
    _build_replace_text_ops(instructions, paragraphs, max_ops=6)
    _build_insert_paragraph_ops(instructions, paragraphs)
    _build_remove_paragraph_ops(instructions, paragraphs)
    instructions.append({"op": "remove_empty_paragraphs"})
    _build_table_ops(instructions, tables)

    return instructions


def run_pdf_pipeline(
    file_name: str,
    output_format: str,
    include_media: bool,
    include_title: bool,
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

    with httpx.Client(timeout=180) as client:
        print(
            f"[1/3] Extracting {file_name} ({len(file_bytes)/1024:.1f} KB) ...")
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
            f"| tables={len(extracted_data.get('tables') or [])}  "
            f"| media={len(extracted_data.get('media') or [])}"
        )

        print("[2/3] Building and applying edit instructions ...")
        instructions = _build_pdf_instructions(extracted_data)
        if not instructions:
            raise ValueError(
                "No edit instructions could be generated for input payload")

        op_counts: dict[str, int] = {}
        for instruction in instructions:
            op = str(instruction.get("op") or "unknown")
            op_counts[op] = op_counts.get(op, 0) + 1
        op_summary = "  ".join(
            f"{op}x{n}" for op, n in sorted(op_counts.items()))
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
        applied_instructions = int(
            edited_payload.get("applied_instructions", 0))
        instruction_confidence = (
            (applied_instructions / len(instructions)) *
            100.0 if instructions else 100.0
        )
        print(
            f"     done in {edit_ms} ms  "
            f"| response {len(edit_response.content)/1024:.1f} KB  "
            f"| applied={applied_instructions}/{len(instructions)}  "
            f"| confidence={instruction_confidence:.2f}%  "
            f"| paragraphs={len(edited_data.get('paragraphs') or [])}  "
            f"| tables={len(edited_data.get('tables') or [])}"
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_base = f"{input_path.stem}_edited_{timestamp}"

        print(f"[3/3] Generating {output_format.upper()} output ...")
        t0 = time.perf_counter()
        generate_request = {
            "output_format": output_format,
            "extracted_data": edited_data,
            "file_name": output_base,
        }
        if include_title:
            generate_request["title"] = f"Generated output for {file_name}"
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run single-file PDF extract->edit->generate pipeline "
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
        default="pdf",
        choices=["docx", "pdf", "pptx", "html", "markdown", "text"],
        help="Format for generated output (default: pdf)",
    )
    parser.add_argument(
        "--include-media",
        action="store_true",
        default=True,
        help="Include media payload during extraction (default: on)",
    )
    parser.add_argument(
        "--no-include-media",
        dest="include_media",
        action="store_false",
        help="Disable media payload during extraction",
    )
    parser.add_argument(
        "--api-base-url",
        default="http://localhost:8000",
        help="Base URL for temp-doc API (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--include-title",
        action="store_true",
        default=False,
        help="Include auto-generated title in /generate request (default: off)",
    )
    parser.add_argument(
        "--no-title",
        dest="include_title",
        action="store_false",
        help="Disable auto-generated title for higher source fidelity",
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

    output_path = run_pdf_pipeline(
        file_name=args.file,
        output_format=args.output_format,
        include_media=args.include_media,
        include_title=args.include_title,
        api_base_url=args.api_base_url,
        write_debug_json=args.write_debug_json,
    )
    print(f"SUCCESS: generated file saved at {output_path}")


if __name__ == "__main__":
    main()
