#!/Users/MAC/Desktop/ai-testing/document-playground/myvenv/bin/python3
"""Chunking endpoint test.

Sends every test file from app/helper/test-files/ to the /chunking endpoint,
prints a summary, and saves each chunk response as JSON under
test/chunking_results/<filename>_chunks.json

Run from temp-doc/test/:
    python3 chunking_test.py
    python3 chunking_test.py --base-url http://127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import sys
import time
from pathlib import Path

import httpx

# ── Config ────────────────────────────────────────────────────────────────────

TEST_FILES_DIR = Path(__file__).parent.parent / "app" / "helper" / "test-files"
RESULTS_DIR = Path(__file__).parent / "chunking_results"
DEFAULT_BASE_URL = "http://127.0.0.1:8000"

SUPPORTED_EXTENSIONS = {
    ".docx", ".pdf", ".pptx", ".html", ".htm", ".md", ".txt"
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _content_type_for(path: Path) -> str:
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"


def _divider(char: str = "─", width: int = 70) -> str:
    return char * width


def _print_header(text: str) -> None:
    print(f"\n{_divider('═')}")
    print(f"  {text}")
    print(_divider("═"))


def _print_result(
    filename: str,
    status_code: int,
    elapsed: float,
    chunk_count: int | None,
    error: str | None,
) -> None:
    status_icon = "✓" if status_code == 200 else "✗"
    chunks_info = f"{chunk_count} chunks" if chunk_count is not None else "N/A"
    err_info = f"  ERROR: {error}" if error else ""
    print(
        f"  {status_icon} [{status_code}] {filename:<40} "
        f"{elapsed:>6.2f}s  {chunks_info}{err_info}"
    )


# ── Core test ─────────────────────────────────────────────────────────────────

def run_chunking_test(base_url: str) -> dict:
    """Run /chunking against every supported test file. Returns summary dict."""
    url = f"{base_url.rstrip('/')}/chunking"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    test_files = sorted(
        f for f in TEST_FILES_DIR.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
    )

    if not test_files:
        print(f"No supported test files found in {TEST_FILES_DIR}")
        return {}

    _print_header(f"Chunking endpoint tests  →  {url}")
    print(f"  Test files directory : {TEST_FILES_DIR}")
    print(f"  Results directory    : {RESULTS_DIR}")
    print(f"  Files found          : {len(test_files)}")
    print(_divider())

    summary: dict = {
        "passed": 0,
        "failed": 0,
        "results": [],
    }

    with httpx.Client(timeout=120.0) as client:
        for path in test_files:
            file_data = path.read_bytes()
            content_type = _content_type_for(path)

            t0 = time.perf_counter()
            try:
                response = client.post(
                    url,
                    files={"file": (path.name, file_data, content_type)},
                    params={"include_media": "false"},
                )
                elapsed = time.perf_counter() - t0
            except httpx.ConnectError:
                print(
                    f"\n  [ERROR] Cannot connect to {base_url}.\n"
                    "  Start the server with:  uvicorn app.main:app --reload\n"
                )
                sys.exit(1)
            except Exception as exc:
                elapsed = time.perf_counter() - t0
                _print_result(path.name, 0, elapsed, None, str(exc))
                summary["failed"] += 1
                summary["results"].append({
                    "file": path.name,
                    "status_code": 0,
                    "elapsed_s": round(elapsed, 3),
                    "chunk_count": None,
                    "error": str(exc),
                })
                continue

            # Parse response
            chunk_count: int | None = None
            error: str | None = None

            if response.status_code == 200:
                try:
                    body = response.json()
                    chunk_count = body.get(
                        "chunk_count", len(body.get("chunks", [])))

                    # Save chunks to file — include extension so same-stem files don't overwrite
                    out_path = RESULTS_DIR / \
                        f"{path.stem}_{path.suffix.lstrip('.')}_chunks.json"
                    out_path.write_text(
                        json.dumps(body, indent=2, ensure_ascii=False),
                        encoding="utf-8",
                    )

                    summary["passed"] += 1
                except Exception as exc:
                    error = f"JSON parse error: {exc}"
                    summary["failed"] += 1
            else:
                try:
                    detail = response.json().get("detail", response.text[:200])
                except Exception:
                    detail = response.text[:200]
                error = str(detail)
                summary["failed"] += 1

            _print_result(
                path.name, response.status_code, elapsed, chunk_count, error
            )

            summary["results"].append({
                "file": path.name,
                "status_code": response.status_code,
                "elapsed_s": round(elapsed, 3),
                "chunk_count": chunk_count,
                "error": error,
            })

    # ── Summary ───────────────────────────────────────────────────────────────
    total = summary["passed"] + summary["failed"]
    print(_divider())
    print(f"\n  Total  : {total}")
    print(f"  Passed : {summary['passed']}")
    print(f"  Failed : {summary['failed']}")

    if summary["passed"]:
        passing = [r for r in summary["results"] if r["status_code"] == 200]
        avg_time = sum(r["elapsed_s"] for r in passing) / len(passing)
        total_chunks = sum(r["chunk_count"] or 0 for r in passing)
        print(f"  Avg time (passed) : {avg_time:.2f}s")
        print(f"  Total chunks      : {total_chunks}")
        print(f"\n  Chunk JSON files saved to: {RESULTS_DIR}")

    print()
    return summary


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Test the /chunking endpoint")
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Server base URL (default: {DEFAULT_BASE_URL})",
    )
    args = parser.parse_args()

    summary = run_chunking_test(args.base_url)
    if summary.get("failed", 0) > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
