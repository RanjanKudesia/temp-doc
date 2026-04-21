"""PPT normalization helpers shared across patch modules."""

from __future__ import annotations

from typing import Any

from ..shared.json_patch_service import (
    _normalize_paragraph_collection,
    _normalize_table_collection,
)


def normalize_ppt_document(document: dict[str, Any]) -> None:
    """Normalise flat paragraph/table/document_order collections."""
    paragraphs = document.get("paragraphs")
    if isinstance(paragraphs, list):
        _normalize_paragraph_collection(paragraphs)

    tables = document.get("tables")
    if isinstance(tables, list):
        _normalize_table_collection(tables)

    document_order = document.get("document_order")
    if isinstance(document_order, list):
        _normalize_ppt_document_order(document)


def _normalize_ppt_document_order(document: dict[str, Any]) -> None:
    """Drop out-of-range order items and append any missing ones."""
    doc_order = document.get("document_order") or []
    limits = {
        "paragraph": len(document.get("paragraphs") or []),
        "table": len(document.get("tables") or []),
        "media": len(document.get("media") or []),
    }

    normalized, seen = _collect_valid_order_items(doc_order, limits)
    _append_missing_order_items(normalized, seen, limits)

    document["document_order"] = normalized


def _collect_valid_order_items(
    doc_order: list[Any],
    limits: dict[str, int],
) -> tuple[list[dict[str, Any]], set[tuple[str, int]]]:
    """Collect valid order items that are in-range and not duplicate."""
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()

    for item in doc_order:
        if not isinstance(item, dict):
            continue

        item_type = item.get("type")
        item_index = item.get("index")

        if item_type not in limits or not isinstance(item_index, int):
            continue
        if item_index < 0 or item_index >= limits[item_type]:
            continue

        key = (item_type, item_index)
        if key in seen:
            continue

        normalized.append({"type": item_type, "index": item_index})
        seen.add(key)

    return normalized, seen


def _append_missing_order_items(
    normalized: list[dict[str, Any]],
    seen: set[tuple[str, int]],
    limits: dict[str, int],
) -> None:
    """Append any items missing from the order list."""
    for item_type in ("paragraph", "table", "media"):
        for item_index in range(limits[item_type]):
            if (item_type, item_index) not in seen:
                normalized.append({"type": item_type, "index": item_index})
