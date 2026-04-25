"""Single-file extract -> edit -> generate API runner.

Default flow starts with HTML and applies complex edit instructions.
Calls real HTTP APIs (/extract, /edit, /generate) and writes output
back to app/helper/test-files/.
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


TEST_FILES_DIR = Path("app/helper/test-files")
PARAGRAPH_0_TEXT_PATH = "/paragraphs/0/text"
TABLE_0_PATH = "/tables/0"
TABLE_0_CELL_0_TEXT_PATH = "/tables/0/rows/0/cells/0/text"


def _instruction_index(instruction: dict[str, Any]) -> int:
    """Return parsed instruction index, or -1 when missing/invalid."""
    raw_index = instruction.get("index")
    if raw_index is None:
        return -1
    try:
        return int(raw_index)
    except (TypeError, ValueError):
        return -1


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
    mapping = {
        "docx": "docx",
        "pdf": "pdf",
        "pptx": "pptx",
        "html": "html",
        "markdown": "md",
        "text": "txt",
    }
    return mapping.get(output_format.lower(), "html")


def _text_path(idx: int) -> str:
    return f"/paragraphs/{idx}/text"


def _find_para_by_contains(paragraphs: list[dict[str, Any]], needle: str) -> int | None:
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
    count: int = 1,
) -> None:
    idx = _find_para_by_contains(paragraphs, needle)
    if idx is None:
        print(
            f"  [WARN] replace_text skipped: paragraph containing {needle!r} not found")
        return
    instructions.append(
        {
            "op": "replace_text",
            "path": _text_path(idx),
            "old_value": old_value,
            "new_value": new_value,
            "count": count,
        }
    )


def _rich_insert_paragraph_value() -> dict[str, Any]:
    return {
        "text": "Inserted paragraph with bold + italic + hyperlink + code style flags in runs.",
        "style": "Normal",
        "runs": [
            {
                "index": 0,
                "text": "Inserted paragraph with ",
                "bold": False,
                "italic": False,
                "underline": False,
                "strikethrough": False,
                "code": False,
                "font_name": "Calibri",
                "font_size_pt": 11,
                "color_rgb": "#222222",
                "highlight_color": None,
                "hyperlink_url": None,
                "embedded_media": [],
            },
            {
                "index": 1,
                "text": "bold",
                "bold": True,
                "italic": False,
                "underline": False,
                "strikethrough": False,
                "code": False,
                "font_name": "Calibri",
                "font_size_pt": 11,
                "color_rgb": "#222222",
                "highlight_color": None,
                "hyperlink_url": None,
                "embedded_media": [],
            },
            {
                "index": 2,
                "text": " + ",
                "bold": False,
                "italic": False,
                "underline": False,
                "strikethrough": False,
                "code": False,
                "font_name": "Calibri",
                "font_size_pt": 11,
                "color_rgb": "#222222",
                "highlight_color": None,
                "hyperlink_url": None,
                "embedded_media": [],
            },
            {
                "index": 3,
                "text": "italic",
                "bold": False,
                "italic": True,
                "underline": False,
                "strikethrough": False,
                "code": False,
                "font_name": "Calibri",
                "font_size_pt": 11,
                "color_rgb": "#222222",
                "highlight_color": None,
                "hyperlink_url": None,
                "embedded_media": [],
            },
            {
                "index": 4,
                "text": " + ",
                "bold": False,
                "italic": False,
                "underline": False,
                "strikethrough": False,
                "code": False,
                "font_name": "Calibri",
                "font_size_pt": 11,
                "color_rgb": "#222222",
                "highlight_color": None,
                "hyperlink_url": None,
                "embedded_media": [],
            },
            {
                "index": 5,
                "text": "hyperlink",
                "bold": False,
                "italic": False,
                "underline": True,
                "strikethrough": False,
                "code": False,
                "font_name": "Calibri",
                "font_size_pt": 11,
                "color_rgb": "#1155cc",
                "highlight_color": None,
                "hyperlink_url": "https://example.com/pipeline",
                "embedded_media": [],
            },
            {
                "index": 6,
                "text": " + ",
                "bold": False,
                "italic": False,
                "underline": False,
                "strikethrough": False,
                "code": False,
                "font_name": "Calibri",
                "font_size_pt": 11,
                "color_rgb": "#222222",
                "highlight_color": None,
                "hyperlink_url": None,
                "embedded_media": [],
            },
            {
                "index": 7,
                "text": "code",
                "bold": False,
                "italic": False,
                "underline": False,
                "strikethrough": False,
                "code": True,
                "font_name": "Consolas",
                "font_size_pt": 11,
                "color_rgb": "#222222",
                "highlight_color": "#fff176",
                "hyperlink_url": None,
                "embedded_media": [],
            },
            {
                "index": 8,
                "text": " style flags in runs.",
                "bold": False,
                "italic": False,
                "underline": False,
                "strikethrough": False,
                "code": False,
                "font_name": "Calibri",
                "font_size_pt": 11,
                "color_rgb": "#222222",
                "highlight_color": None,
                "hyperlink_url": None,
                "embedded_media": [],
            },
        ],
    }


def _append_html_paragraph_instructions(
    instructions: list[dict[str, Any]],
    paragraphs: list[dict[str, Any]],
) -> None:
    if not paragraphs:
        return

    # Replace entire paragraph 0 text directly (no preceding replace_text needed)
    instructions.append(
        {
            "op": "replace",
            "path": PARAGRAPH_0_TEXT_PATH,
            "value": (
                "Intro paragraph rewritten by advanced single-file HTML edit test: "
                "semantic tags, media, JS hooks, and table mutations are intentionally exercised."
            ),
        }
    )

    _append_replace_text_if_found(
        instructions, paragraphs, "Reliability is not an accident", "deliberate", "intentional"
    )
    _append_replace_text_if_found(
        instructions, paragraphs, "Health check endpoint", "GET /health", "GET /healthz"
    )
    _append_replace_text_if_found(
        instructions, paragraphs, "Additional entities and symbols", "parser", "parser/editor/generator"
    )
    _append_replace_text_if_found(
        instructions, paragraphs, "JavaScript not yet executed", "not yet", "already"
    )
    _append_replace_text_if_found(
        instructions, paragraphs, "Pipeline progress", "progress", "completion progress"
    )

    instructions.append(
        {
            "op": "insert_paragraph_after",
            "index": 0,
            "value": _rich_insert_paragraph_value(),
        }
    )
    instructions.append(
        {
            "op": "insert_paragraph_after",
            "index": 2,
            "value": "Second inserted paragraph for stress-testing multiple insert anchors.",
        }
    )
    instructions.append(
        {
            "op": "insert_paragraph_after",
            "index": 5,
            "value": "Third inserted paragraph: validates index remapping after prior insertions.",
        }
    )

    if len(paragraphs) > 12:
        instructions.append({"op": "remove_paragraph", "index": 12})
    else:
        print(
            f"  [WARN] remove_paragraph(12) skipped: only {len(paragraphs)} paragraphs extracted")
    if len(paragraphs) > 20:
        instructions.append({"op": "remove_paragraph", "index": 20})
    else:
        print(
            f"  [WARN] remove_paragraph(20) skipped: only {len(paragraphs)} paragraphs extracted")


def _append_html_table_instructions(
    instructions: list[dict[str, Any]],
    tables: list[dict[str, Any]],
) -> None:
    if not tables:
        return

    instructions.append(
        {
            "op": "replace",
            "path": TABLE_0_CELL_0_TEXT_PATH,
            "value": "Top-left cell updated by HTML single-file pipeline.",
        }
    )
    instructions.append(
        {"op": "insert_table_row", "path": TABLE_0_PATH,
            "index": 0, "value": "Inserted row value"}
    )
    instructions.append(
        {"op": "insert_table_column", "path": TABLE_0_PATH,
            "index": 0, "value": "Inserted col"}
    )
    instructions.append(
        {
            "op": "insert_table_row",
            "path": TABLE_0_PATH,
            "index": 1,
            "value": "Inserted secondary row value",
        }
    )
    instructions.append(
        {
            "op": "insert_table_column",
            "path": TABLE_0_PATH,
            "index": 1,
            "value": "Inserted second col",
        }
    )
    instructions.append(
        {"op": "remove_table_row", "path": TABLE_0_PATH, "index": 2})
    instructions.append({"op": "remove_table_column",
                        "path": TABLE_0_PATH, "index": 2})

    if len(tables) > 1:
        instructions.append(
            {
                "op": "replace",
                "path": "/tables/1/rows/0/cells/0/text",
                "value": "Second-table top-left replaced",
            }
        )
        instructions.append(
            {
                "op": "insert_table_row",
                "path": "/tables/1",
                "index": 0,
                "value": "Second-table inserted row",
            }
        )


def _build_html_complex_instructions(extracted: dict[str, Any]) -> list[dict[str, Any]]:
    """Build complex edit instructions based on the extracted payload shape."""
    instructions: list[dict[str, Any]] = []
    paragraphs = extracted.get("paragraphs") or []
    tables = extracted.get("tables") or []

    _append_html_paragraph_instructions(instructions, paragraphs)
    instructions.append({"op": "remove_empty_paragraphs"})
    _append_html_table_instructions(instructions, tables)

    return instructions


def _build_generic_complex_instructions(extracted: dict[str, Any]) -> list[dict[str, Any]]:
    """Fallback complex instructions for non-HTML extracted payloads."""
    instructions: list[dict[str, Any]] = []
    paragraphs = extracted.get("paragraphs") or []

    if paragraphs:
        first_text = str((paragraphs[0] or {}).get("text") or "")
        first_word = first_text.split(" ")[0] if first_text.strip() else ""
        if first_word:
            instructions.append(
                {
                    "op": "replace_text",
                    "path": PARAGRAPH_0_TEXT_PATH,
                    "old_value": first_word,
                    "new_value": f"[UPDATED:{first_word}]",
                    "count": 1,
                }
            )

        instructions.append(
            {
                "op": "insert_paragraph_after",
                "index": 0,
                "value": "Inserted paragraph from single-file test runner.",
            }
        )

    instructions.append({"op": "remove_empty_paragraphs"})
    return instructions


def _instructions_for_extension(
    extension: str,
    extracted_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    if extension in {"html", "htm"}:
        return _build_html_complex_instructions(extracted_payload)
    return _build_generic_complex_instructions(extracted_payload)


def run_single_file_pipeline(
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

        print("[2/3] Building and applying edit instructions ...")
        instructions = _instructions_for_extension(extension, extracted_data)
        if not instructions:
            raise ValueError(
                "No edit instructions could be generated for input payload")
        print(f"     instructions count: {len(instructions)}")

        t0 = time.perf_counter()
        edit_request = {
            "extension": extension,
            "extracted_data": extracted_data,
            "instructions": instructions,
        }
        edit_response = client.post(edit_url, json=edit_request)
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

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_base = f"{input_path.stem}_edited_{timestamp}"
        generated_title = None if output_format.lower(
        ) == "html" else f"Edited output for {file_name}"

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

    return output_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run single-file extract->edit->generate pipeline from app/helper/test-files",
    )
    parser.add_argument(
        "--file",
        default="complex_sample.html",
        help="File name inside app/helper/test-files (default: complex_sample.html)",
    )
    parser.add_argument(
        "--output-format",
        default="html",
        choices=["docx", "pdf", "pptx", "html", "markdown", "text"],
        help="Format for generated output (default: html)",
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
        help="Also write debug JSON payload next to generated file (default: on)",
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

    output_path = run_single_file_pipeline(
        file_name=args.file,
        output_format=args.output_format,
        include_media=args.include_media,
        api_base_url=args.api_base_url,
        write_debug_json=args.write_debug_json,
    )
    print(f"SUCCESS: generated file saved at {output_path}")


if __name__ == "__main__":
    main()
