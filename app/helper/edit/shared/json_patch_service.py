"""Apply patch instructions to extracted JSON payloads."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException, status

from .json_patch_utils import (
    add_value as _add_value,
    build_default_runs as _build_default_runs,
    build_paragraph_payload as _build_paragraph_payload,
    build_table_cell_payload as _build_table_cell_payload,
    build_table_payload as _build_table_payload,
    build_table_row_payload as _build_table_row_payload,
    collection_path as _collection_path,
    column_cell_payloads as _column_cell_payloads,
    decode_pointer_tokens as _decode_pointer_tokens,
    document_order as _document_order,
    find_indexed_item_position as _find_indexed_item_position,
    parse_list_index as _parse_list_index,
    read_container_value as _read_container_value,
    remove_value as _remove_value,
    replace_value as _replace_value,
    require_int as _require_int,
    required_path as _required_path,
    resolve_dict_at_path as _resolve_dict_at_path,
    resolve_list_at_path as _resolve_list_at_path,
    resolve_parent as _resolve_parent,
    resolve_value as _resolve_value,
    row_cells as _row_cells,
    table_rows as _table_rows,
    write_container_value as _write_container_value,
)

from ...schemas.temp_doc_schema import (
    EditRequest,
    EditResponse,
    ExtractResponse,
    ExtractedData,
)

logger = logging.getLogger(__name__)

TOP_LEVEL_PARAGRAPHS_PATH = "/paragraphs"
TOP_LEVEL_TABLES_PATH = "/tables"
PARAGRAPHS_COLLECTION_INVALID = "Document paragraphs collection is invalid"
TABLES_COLLECTION_INVALID = "Document tables collection is invalid"
DOCUMENT_ORDER_COLLECTION_INVALID = "Document order collection is invalid"
TABLE_TARGET_INVALID = "Table target is invalid"
NON_CONTAINER_VALUE_ERROR = "Cannot navigate into non-container value"


ADVANCED_OPS = {
    "replace_text",
    "insert_paragraph_after",
    "remove_paragraph",
    "remove_empty_paragraphs",
    "insert_table_after",
    "remove_table",
    "insert_table_row",
    "remove_table_row",
    "insert_table_column",
    "remove_table_column",
}


TOP_LEVEL_COLLECTION_TYPES = {
    "paragraphs": "paragraph",
    "tables": "table",
    "media": "media",
}


def edit_extracted_json(
    request_data: EditRequest | dict[str, Any],
    extension_hint: str | None = None,
) -> EditResponse:
    """Apply instructions to DOCX/HTML extracted_data and return updated payload."""
    if isinstance(request_data, dict):
        try:
            request_data = EditRequest.model_validate(request_data)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid edit payload: {str(exc)}",
            ) from exc

    normalized_extension = _normalize_edit_extension(
        extension_hint or _infer_edit_extension(request_data)
    )
    allowed_aliases = _allowed_extension_aliases(normalized_extension)
    source_data = _coerce_request_extracted_data(
        request_data.extracted_data,
        allowed_extensions=allowed_aliases,
    )
    document = source_data.model_dump()
    _merge_model_extra(document, source_data)

    try:
        for index, instruction in enumerate(request_data.instructions):
            _apply_instruction(document, instruction.model_dump(), index)

        validated = ExtractedData.model_validate(document)
        return EditResponse(
            extension=normalized_extension,
            output_format="json",
            extracted_data=validated,
            applied_instructions=len(request_data.instructions),
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Edit operation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Failed to apply edit instructions: {str(exc)}",
        ) from exc


def _coerce_request_extracted_data(
    payload: ExtractedData | ExtractResponse,
    allowed_extensions: set[str],
) -> ExtractedData:
    """Accept either raw extracted_data or full /extract response payload."""
    if isinstance(payload, ExtractedData):
        return payload

    normalized_extension = str(payload.extension or "").strip().lower()
    if normalized_extension not in allowed_extensions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Unsupported extract payload extension for /edit: "
                f"{payload.extension}. Supported in this request: "
                f"{', '.join(sorted(allowed_extensions))}"
            ),
        )

    if payload.output_format != "json":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="/edit supports JSON extract payloads only.",
        )

    if not isinstance(payload.extracted_data, ExtractedData):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="/edit currently supports structured JSON extracted_data payloads only.",
        )

    return payload.extracted_data


def _merge_model_extra(document: dict[str, Any], model: ExtractedData) -> None:
    """Carry extra ExtractedData fields (for example HTML metadata) into edit payload."""
    extras = getattr(model, "model_extra", None)
    if not isinstance(extras, dict):
        return

    for key, value in extras.items():
        if key in document:
            continue
        document[key] = value


def _normalize_edit_extension(extension_hint: str | None) -> str:
    """Normalize extension used in EditResponse for structured document edits."""
    normalized = str(extension_hint or "docx").strip().lower()
    if normalized in {"html", "htm"}:
        return "html"
    if normalized in {"md", "markdown"}:
        return "md"
    if normalized in {"txt", "text"}:
        return "txt"
    return "docx"


def _infer_edit_extension(request_data: EditRequest) -> str | None:
    """Infer edit extension from request payload when callers omit a hint."""
    extracted_payload = request_data.extracted_data

    if isinstance(extracted_payload, ExtractResponse):
        return extracted_payload.extension

    metadata = getattr(extracted_payload, "model_extra", None)
    if isinstance(metadata, dict):
        source_type = metadata.get("source_type")
        if isinstance(source_type, str) and source_type.strip():
            return source_type

        nested_metadata = metadata.get("metadata")
        if isinstance(nested_metadata, dict):
            nested_source_type = nested_metadata.get("source_type")
            if isinstance(nested_source_type, str) and nested_source_type.strip():
                return nested_source_type

    return None


def _allowed_extension_aliases(normalized_extension: str) -> set[str]:
    """Return accepted input extension aliases for one normalized output extension."""
    if normalized_extension == "html":
        return {"html", "htm"}
    if normalized_extension == "md":
        return {"md", "markdown"}
    if normalized_extension == "txt":
        return {"txt", "text"}
    return {"doc", "docx"}


def _apply_instruction(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Apply one instruction to document in-place."""
    op = str(instruction.get("op") or "").strip().lower()

    if op in ADVANCED_OPS:
        _apply_advanced_instruction(document, instruction, idx)
    else:
        _apply_generic_instruction(document, instruction, idx)

    _normalize_document(document)


def _apply_generic_instruction(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Apply generic JSON-pointer instructions."""
    op = str(instruction.get("op") or "").strip().lower()
    path = _required_path(instruction.get("path"), idx, op)
    value = instruction.get("value")

    if op not in {"add", "replace", "remove"}:
        raise ValueError(f"Instruction[{idx}] has unsupported op: {op}")

    parent, key = _resolve_parent(document, path)
    tokens = _decode_pointer_tokens(path)

    if op == "add":
        _add_value(parent, key, value, idx)
        _sync_top_level_order_for_generic_add(document, tokens, parent, key)
        return

    if op == "replace":
        _replace_value(parent, key, value, idx)
        _sync_text_containers_after_direct_replace(
            document=document,
            path=path,
            updated_value=value,
        )
        return

    _sync_top_level_order_for_generic_remove(document, tokens)
    _remove_value(parent, key, idx)


def _apply_advanced_instruction(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Apply higher-level edit operations that maintain document structure."""
    op = str(instruction.get("op") or "").strip().lower()

    if op == "replace_text":
        _replace_text(document, instruction, idx)
        return
    if op == "insert_paragraph_after":
        _insert_paragraph_after(document, instruction, idx)
        return
    if op == "remove_paragraph":
        _remove_paragraph(document, instruction, idx)
        return
    if op == "remove_empty_paragraphs":
        _remove_empty_paragraphs(document, instruction)
        return
    if op == "insert_table_after":
        _insert_table_after(document, instruction, idx)
        return
    if op == "remove_table":
        _remove_table(document, instruction, idx)
        return
    if op == "insert_table_row":
        _insert_table_row(document, instruction, idx)
        return
    if op == "remove_table_row":
        _remove_table_row(document, instruction, idx)
        return
    if op == "insert_table_column":
        _insert_table_column(document, instruction, idx)
        return
    if op == "remove_table_column":
        _remove_table_column(document, instruction, idx)
        return

    raise ValueError(f"Instruction[{idx}] has unsupported advanced op: {op}")


def _replace_text(document: dict[str, Any], instruction: dict[str, Any], idx: int) -> None:
    """Replace text within a string field addressed by JSON pointer."""
    path = _required_path(instruction.get("path"), idx, "replace_text")
    old_value = instruction.get("old_value")
    new_value = instruction.get("new_value")
    count_raw = instruction.get("count")
    count: int | None = None

    if old_value is None or new_value is None:
        raise ValueError(
            f"Instruction[{idx}] old_value and new_value are required for replace_text"
        )

    if count_raw is not None:
        try:
            count = int(count_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Instruction[{idx}] count must be an integer"
            ) from exc
        if count < 0:
            raise ValueError(
                f"Instruction[{idx}] count must be >= 0"
            )

    parent, key = _resolve_parent(document, path)
    target = _read_container_value(parent, key, idx)
    if not isinstance(target, str):
        raise ValueError(
            f"Instruction[{idx}] replace_text target must be a string")

    updated = target.replace(old_value, new_value) if count is None else target.replace(
        old_value,
        new_value,
        count,
    )
    _write_container_value(parent, key, updated, idx)
    _sync_text_containers_after_replace(
        document=document,
        path=path,
        old_value=old_value,
        new_value=new_value,
        count=count,
        updated_value=updated,
    )


def _insert_paragraph_after(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Insert a paragraph into a paragraph collection after a logical index."""
    anchor_index = _require_int(instruction.get("index"), idx, "index")
    collection_path = _collection_path(
        instruction.get("path"), TOP_LEVEL_PARAGRAPHS_PATH)
    paragraphs = _resolve_list_at_path(
        document,
        collection_path,
        PARAGRAPHS_COLLECTION_INVALID,
    )

    insert_at = _find_indexed_item_position(
        paragraphs, anchor_index, "index", "Paragraph")
    paragraphs.insert(
        insert_at + 1,
        _build_paragraph_payload(instruction.get("value"), anchor_index + 1),
    )

    if collection_path == TOP_LEVEL_PARAGRAPHS_PATH:
        _insert_document_order_item_after(document, "paragraph", anchor_index)


def _remove_paragraph(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Remove a paragraph from a paragraph collection."""
    paragraph_index = _require_int(instruction.get("index"), idx, "index")
    collection_path = _collection_path(
        instruction.get("path"), TOP_LEVEL_PARAGRAPHS_PATH)
    paragraphs = _resolve_list_at_path(
        document,
        collection_path,
        PARAGRAPHS_COLLECTION_INVALID,
    )

    remove_at = _find_indexed_item_position(
        paragraphs, paragraph_index, "index", "Paragraph")
    del paragraphs[remove_at]

    if collection_path == TOP_LEVEL_PARAGRAPHS_PATH:
        _remove_document_order_item(document, "paragraph", paragraph_index)


def _remove_empty_paragraphs(
    document: dict[str, Any],
    instruction: dict[str, Any],
) -> None:
    """Remove blank paragraphs from a paragraph collection."""
    collection_path = _collection_path(
        instruction.get("path"), TOP_LEVEL_PARAGRAPHS_PATH)
    paragraphs = _resolve_list_at_path(
        document,
        collection_path,
        PARAGRAPHS_COLLECTION_INVALID,
    )

    removed_indices = {
        int(paragraph.get("index", -1))
        for paragraph in paragraphs
        if isinstance(paragraph, dict) and _paragraph_is_empty(paragraph)
    }
    paragraphs[:] = [
        paragraph
        for paragraph in paragraphs
        if not isinstance(paragraph, dict) or not _paragraph_is_empty(paragraph)
    ]

    if collection_path == TOP_LEVEL_PARAGRAPHS_PATH and removed_indices:
        _sync_document_order_after_removed_items(
            document, "paragraph", removed_indices)


def _insert_table_after(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Insert a table into a table collection after a logical index."""
    anchor_index = _require_int(instruction.get("index"), idx, "index")
    collection_path = _collection_path(
        instruction.get("path"), TOP_LEVEL_TABLES_PATH)
    tables = _resolve_list_at_path(
        document, collection_path, TABLES_COLLECTION_INVALID)

    insert_at = _find_indexed_item_position(
        tables, anchor_index, "index", "Table")
    tables.insert(
        insert_at + 1,
        _build_table_payload(instruction.get("value"), anchor_index + 1),
    )

    if collection_path == TOP_LEVEL_TABLES_PATH:
        _insert_document_order_item_after(document, "table", anchor_index)


def _remove_table(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Remove a table from a table collection."""
    table_index = _require_int(instruction.get("index"), idx, "index")
    collection_path = _collection_path(
        instruction.get("path"), TOP_LEVEL_TABLES_PATH)
    tables = _resolve_list_at_path(
        document, collection_path, TABLES_COLLECTION_INVALID)

    remove_at = _find_indexed_item_position(
        tables, table_index, "index", "Table")
    del tables[remove_at]

    if collection_path == TOP_LEVEL_TABLES_PATH:
        _remove_document_order_item(document, "table", table_index)


def _insert_table_row(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Insert a row into a table after an existing row index."""
    table = _resolve_dict_at_path(
        document,
        _required_path(instruction.get("path"), idx, "insert_table_row"),
        TABLE_TARGET_INVALID,
    )
    row_index = _require_int(instruction.get("index"), idx, "index")
    rows = _table_rows(table)

    insert_at = _find_indexed_item_position(
        rows, row_index, "row_index", "Row")
    column_count = max((len(row.get("cells", []))
                       for row in rows if isinstance(row, dict)), default=1)
    rows.insert(
        insert_at + 1,
        _build_table_row_payload(instruction.get(
            "value"), row_index + 1, column_count),
    )


def _remove_table_row(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Remove a row from a table."""
    table = _resolve_dict_at_path(
        document,
        _required_path(instruction.get("path"), idx, "remove_table_row"),
        TABLE_TARGET_INVALID,
    )
    row_index = _require_int(instruction.get("index"), idx, "index")
    rows = _table_rows(table)

    remove_at = _find_indexed_item_position(
        rows, row_index, "row_index", "Row")
    del rows[remove_at]


def _insert_table_column(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Insert a column into a table after an existing column index."""
    table = _resolve_dict_at_path(
        document,
        _required_path(instruction.get("path"), idx, "insert_table_column"),
        TABLE_TARGET_INVALID,
    )
    column_index = _require_int(instruction.get("index"), idx, "index")
    rows = _table_rows(table)
    if not rows:
        raise ValueError(
            f"Instruction[{idx}] table must contain rows before inserting a column")

    column_cells = _column_cell_payloads(instruction.get("value"), len(rows))
    for row, cell_value in zip(rows, column_cells):
        cells = _row_cells(row)
        if column_index >= len(cells):
            raise ValueError(
                f"Instruction[{idx}] column index out of range: {column_index}")
        cells.insert(column_index + 1, _build_table_cell_payload(cell_value))


def _remove_table_column(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Remove a column from a table."""
    table = _resolve_dict_at_path(
        document,
        _required_path(instruction.get("path"), idx, "remove_table_column"),
        TABLE_TARGET_INVALID,
    )
    column_index = _require_int(instruction.get("index"), idx, "index")
    rows = _table_rows(table)

    for row in rows:
        cells = _row_cells(row)
        if column_index >= len(cells):
            raise ValueError(
                f"Instruction[{idx}] column index out of range: {column_index}")
        del cells[column_index]


def _normalize_document(document: dict[str, Any]) -> None:
    """Normalize derived DOCX structures after each edit."""
    paragraphs = document.get("paragraphs")
    if isinstance(paragraphs, list):
        _normalize_paragraph_collection(paragraphs)

    tables = document.get("tables")
    if isinstance(tables, list):
        _normalize_table_collection(tables)

    document_order = document.get("document_order")
    if isinstance(document_order, list):
        _normalize_document_order(document)


def _normalize_paragraph_collection(paragraphs: list[Any]) -> None:
    """Normalize a paragraph collection and its indices."""
    for new_index, paragraph in enumerate(paragraphs):
        if not isinstance(paragraph, dict):
            continue
        paragraph["index"] = new_index
        _normalize_paragraph(paragraph)


def _normalize_paragraph(paragraph: dict[str, Any]) -> None:
    """Normalize a paragraph's runs and text."""
    runs = paragraph.get("runs")
    if isinstance(runs, list):
        for run_index, run in enumerate(runs):
            if isinstance(run, dict):
                run["index"] = run_index
        paragraph["text"] = "".join(
            str(run.get("text") or "") for run in runs if isinstance(run, dict)
        )
        return

    paragraph["text"] = str(paragraph.get("text") or "")


def _normalize_table_collection(tables: list[Any]) -> None:
    """Normalize a table collection and its indices."""
    for new_index, table in enumerate(tables):
        if not isinstance(table, dict):
            continue
        table["index"] = new_index
        _normalize_table(table)


def _normalize_table(table: dict[str, Any]) -> None:
    """Normalize rows, cells, counts, and nested table metadata."""
    rows = table.get("rows")
    if not isinstance(rows, list):
        table["rows"] = []
        table["row_count"] = 0
        table["column_count"] = 0
        return

    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        row["row_index"] = row_index
        _normalize_table_row(row)

    table["row_count"] = len(rows)
    table["column_count"] = max(
        (len(row.get("cells", [])) for row in rows if isinstance(row, dict)),
        default=0,
    )


def _normalize_table_row(row: dict[str, Any]) -> None:
    """Normalize cells inside a table row."""
    cells = row.get("cells")
    if not isinstance(cells, list):
        row["cells"] = []
        return

    for position, cell in enumerate(cells):
        if not isinstance(cell, dict):
            cells[position] = _build_table_cell_payload(cell)
            cell = cells[position]
        _normalize_table_cell(cell)


def _normalize_table_cell(cell: dict[str, Any]) -> None:
    """Normalize cell paragraphs, nested tables, and composed text."""
    paragraphs = cell.get("paragraphs")
    if isinstance(paragraphs, list):
        _normalize_paragraph_collection(paragraphs)
    else:
        cell["paragraphs"] = []

    tables = cell.get("tables")
    if isinstance(tables, list):
        _normalize_table_collection(tables)
        cell["nested_table_indices"] = [
            table.get("index", 0) for table in tables if isinstance(table, dict)
        ]
    else:
        cell["tables"] = []
        cell["nested_table_indices"] = []

    cell["text"] = _compose_cell_text(cell)


def _compose_cell_text(cell: dict[str, Any]) -> str:
    """Compose table cell text from its local paragraphs when available."""
    paragraphs = cell.get("paragraphs")
    if isinstance(paragraphs, list) and paragraphs:
        return "\n".join(
            str(paragraph.get("text") or "")
            for paragraph in paragraphs
            if isinstance(paragraph, dict)
        )
    return str(cell.get("text") or "")


def _normalize_document_order(document: dict[str, Any]) -> None:
    """Drop invalid order items and append any missing top-level items."""
    document_order = _document_order(document)
    limits = {
        "paragraph": len(document.get("paragraphs") or []),
        "table": len(document.get("tables") or []),
        "media": len(document.get("media") or []),
    }

    normalized, seen = _collect_valid_document_order_items(
        document_order, limits)
    _append_missing_document_order_items(normalized, seen, limits)

    document["document_order"] = normalized


def _collect_valid_document_order_items(
    document_order: list[dict[str, Any]],
    limits: dict[str, int],
) -> tuple[list[dict[str, Any]], set[tuple[str, int]]]:
    """Collect valid unique document order items."""
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()

    for item in document_order:
        validated = _validated_document_order_item(item, limits, seen)
        if validated is None:
            continue
        normalized.append(validated)
        seen.add((validated["type"], validated["index"]))

    return normalized, seen


def _validated_document_order_item(
    item: Any,
    limits: dict[str, int],
    seen: set[tuple[str, int]],
) -> dict[str, Any] | None:
    """Return a normalized order item when valid and unique."""
    if not isinstance(item, dict):
        return None

    item_type = item.get("type")
    item_index = item.get("index")
    if item_type not in limits or not isinstance(item_index, int):
        return None
    if item_index < 0 or item_index >= limits[item_type]:
        return None
    if (item_type, item_index) in seen:
        return None

    return {"type": item_type, "index": item_index}


def _append_missing_document_order_items(
    normalized: list[dict[str, Any]],
    seen: set[tuple[str, int]],
    limits: dict[str, int],
) -> None:
    """Append missing top-level items, interleaving by logical index."""
    order_types = ("paragraph", "table", "media")
    max_len = max((limits.get(item_type, 0)
                  for item_type in order_types), default=0)

    for item_index in range(max_len):
        for item_type in order_types:
            if item_index >= limits.get(item_type, 0):
                continue
            if (item_type, item_index) in seen:
                continue
            normalized.append({"type": item_type, "index": item_index})


def _sync_top_level_order_for_generic_add(
    document: dict[str, Any],
    tokens: list[str],
    parent: Any,
    key: str,
) -> None:
    """Keep document_order aligned when top-level lists are edited generically."""
    item_type = _top_level_item_type(tokens)
    if item_type is None or not isinstance(parent, list):
        return

    item_index = len(
        parent) - 1 if key == "-" else _parse_list_index(key, allow_append=False)
    _insert_document_order_item_at(document, item_type, item_index)


def _sync_top_level_order_for_generic_remove(
    document: dict[str, Any],
    tokens: list[str],
) -> None:
    """Keep document_order aligned when a top-level item is removed generically."""
    item_type = _top_level_item_type(tokens)
    if item_type is None:
        return

    item_index = _parse_list_index(tokens[1], allow_append=False)
    _remove_document_order_item(document, item_type, item_index)


def _top_level_item_type(tokens: list[str]) -> str | None:
    """Map top-level collection names to document_order item types."""
    if len(tokens) != 2:
        return None
    return TOP_LEVEL_COLLECTION_TYPES.get(tokens[0])


def _insert_document_order_item_at(
    document: dict[str, Any],
    item_type: str,
    item_index: int,
) -> None:
    """Insert a top-level item at a logical index."""
    document_order = _document_order(document)
    insert_position = len(document_order)

    for order_position, item in enumerate(document_order):
        if item.get("type") != item_type:
            continue
        current_index = item.get("index")
        if not isinstance(current_index, int):
            continue
        if current_index >= item_index and insert_position == len(document_order):
            insert_position = order_position
        if current_index >= item_index:
            item["index"] = current_index + 1

    document_order.insert(
        insert_position, {"type": item_type, "index": item_index})


def _insert_document_order_item_after(
    document: dict[str, Any],
    item_type: str,
    anchor_index: int,
) -> None:
    """Insert a top-level item after an existing logical index."""
    document_order = _document_order(document)
    insert_position = None

    for order_position, item in enumerate(document_order):
        if item.get("type") != item_type:
            continue
        current_index = item.get("index")
        if not isinstance(current_index, int):
            continue
        if current_index > anchor_index:
            item["index"] = current_index + 1
        if current_index == anchor_index:
            insert_position = order_position + 1

    if insert_position is None:
        raise ValueError(
            f"Document order {item_type} index not found: {anchor_index}")

    document_order.insert(
        insert_position, {"type": item_type, "index": anchor_index + 1})


def _remove_document_order_item(
    document: dict[str, Any],
    item_type: str,
    item_index: int,
) -> None:
    """Remove a top-level item and shift later indices."""
    document_order = _document_order(document)
    updated_order: list[dict[str, Any]] = []
    removed = False

    for item in document_order:
        if item.get("type") != item_type:
            updated_order.append(item)
            continue

        current_index = item.get("index")
        if current_index == item_index:
            removed = True
            continue
        if isinstance(current_index, int) and current_index > item_index:
            updated_order.append({**item, "index": current_index - 1})
        else:
            updated_order.append(item)

    if not removed:
        raise ValueError(
            f"Document order {item_type} index not found: {item_index}")

    document["document_order"] = updated_order


def _sync_document_order_after_removed_items(
    document: dict[str, Any],
    item_type: str,
    removed_indices: set[int],
) -> None:
    """Rebuild document_order indices after multiple top-level removals."""
    document_order = _document_order(document)
    if not removed_indices:
        return

    updated_order: list[dict[str, Any]] = []
    for item in document_order:
        if item.get("type") != item_type:
            updated_order.append(item)
            continue

        current_index = item.get("index")
        if not isinstance(current_index, int):
            updated_order.append(item)
            continue
        if current_index in removed_indices:
            continue

        shift = sum(
            1 for removed_index in removed_indices if removed_index < current_index)
        updated_order.append({**item, "index": current_index - shift})

    document["document_order"] = updated_order


def _sync_text_containers_after_replace(
    document: dict[str, Any],
    path: str,
    old_value: str,
    new_value: str,
    count: int | None,
    updated_value: str,
) -> None:
    """Keep paragraph text and run text aligned after replace_text."""
    tokens = _decode_pointer_tokens(path)
    if not tokens or tokens[-1] != "text":
        return

    paragraph_token_position = _last_token_position(tokens, "paragraphs")
    if paragraph_token_position == -1 or paragraph_token_position + 1 >= len(tokens):
        return

    paragraph = _resolve_value_from_tokens(
        document, tokens[: paragraph_token_position + 2])
    if not isinstance(paragraph, dict):
        return

    remaining_tokens = tokens[paragraph_token_position + 2:]
    if remaining_tokens == ["text"]:
        _sync_runs_from_paragraph_text(
            paragraph=paragraph,
            old_value=old_value,
            new_value=new_value,
            count=count,
            updated_value=updated_value,
        )
        return

    if len(remaining_tokens) == 3 and remaining_tokens[0] == "runs":
        paragraph["text"] = "".join(
            str(run.get("text") or "")
            for run in paragraph.get("runs", [])
            if isinstance(run, dict)
        )


def _sync_text_containers_after_direct_replace(
    document: dict[str, Any],
    path: str,
    updated_value: Any,
) -> None:
    """Keep dependent text containers aligned after generic replace on `.../text`."""
    if not isinstance(updated_value, str):
        return

    tokens = _decode_pointer_tokens(path)
    if not tokens or tokens[-1] != "text":
        return

    if _sync_direct_replace_for_paragraph(document, tokens, updated_value):
        return

    _sync_direct_replace_for_cell(document, tokens, updated_value)


def _sync_direct_replace_for_paragraph(
    document: dict[str, Any],
    tokens: list[str],
    updated_value: str,
) -> bool:
    """Sync paragraph text/runs for generic replace operations."""
    paragraph_token_position = _last_token_position(tokens, "paragraphs")
    if paragraph_token_position == -1 or paragraph_token_position + 1 >= len(tokens):
        return False

    paragraph = _resolve_value_from_tokens(
        document,
        tokens[: paragraph_token_position + 2],
    )
    if not isinstance(paragraph, dict):
        return False

    remaining_tokens = tokens[paragraph_token_position + 2:]
    if remaining_tokens == ["text"]:
        _sync_runs_for_replaced_paragraph_text(paragraph, updated_value)
        return True

    if len(remaining_tokens) == 3 and remaining_tokens[0] == "runs":
        paragraph["text"] = "".join(
            str(run.get("text") or "")
            for run in paragraph.get("runs", [])
            if isinstance(run, dict)
        )
        return True

    return False


def _sync_direct_replace_for_cell(
    document: dict[str, Any],
    tokens: list[str],
    updated_value: str,
) -> None:
    """Sync table cell text and paragraphs for generic replace operations."""
    cell_token_position = _last_token_position(tokens, "cells")
    if cell_token_position == -1 or cell_token_position + 1 >= len(tokens):
        return

    cell = _resolve_value_from_tokens(
        document, tokens[: cell_token_position + 2])
    if not isinstance(cell, dict):
        return

    if tokens[cell_token_position + 2:] != ["text"]:
        return

    # Cell text is derived from paragraph content during normalization.
    cell["paragraphs"] = [_build_paragraph_payload(updated_value, 0)]


def _sync_runs_for_replaced_paragraph_text(
    paragraph: dict[str, Any],
    updated_value: str,
) -> None:
    """Ensure paragraph runs remain compatible with directly replaced paragraph text."""
    runs = paragraph.get("runs")
    if not isinstance(runs, list) or not runs:
        paragraph["runs"] = _build_default_runs(updated_value)
        return

    first_run = runs[0] if isinstance(runs[0], dict) else {}
    paragraph["runs"] = [{**first_run, "index": 0, "text": updated_value}]


def _last_token_position(tokens: list[str], token: str) -> int:
    """Return the last position of a token in a pointer token list."""
    for position in range(len(tokens) - 1, -1, -1):
        if tokens[position] == token:
            return position
    return -1


def _resolve_value_from_tokens(document: dict[str, Any], tokens: list[str]) -> Any:
    """Resolve a value directly from decoded JSON pointer tokens."""
    current: Any = document
    for token in tokens:
        if isinstance(current, dict):
            current = current[token]
            continue
        if isinstance(current, list):
            current = current[_parse_list_index(token, allow_append=False)]
            continue
        raise ValueError(NON_CONTAINER_VALUE_ERROR)
    return current


def _sync_runs_from_paragraph_text(
    paragraph: dict[str, Any],
    old_value: str,
    new_value: str,
    count: int | None,
    updated_value: str,
) -> None:
    """Apply replacement across paragraph runs when paragraph text changes."""
    runs = paragraph.get("runs")
    if not isinstance(runs, list) or not runs:
        paragraph["runs"] = _build_default_runs(updated_value)
        return

    remaining = count
    replacements_applied = 0
    for run in runs:
        replace_count = _get_run_replace_count(run, old_value, remaining)
        if replace_count <= 0:
            continue
        _replace_run_text(run, old_value, new_value, replace_count)
        replacements_applied += replace_count
        remaining = _decrement_remaining_replacements(remaining, replace_count)
        if remaining == 0:
            break

    # If no per-run replacement happened but paragraph text changed, the match likely
    # spanned run boundaries. Collapse to one run so text stays consistent.
    if replacements_applied == 0:
        composed = "".join(
            str(run.get("text") or "") for run in runs if isinstance(run, dict)
        )
        if composed != updated_value:
            _sync_runs_for_replaced_paragraph_text(paragraph, updated_value)


def _get_run_replace_count(run: Any, old_value: str, remaining: int | None) -> int:
    """Return the number of replacements that should be applied to a run."""
    if not isinstance(run, dict):
        return 0

    occurrences = str(run.get("text") or "").count(old_value)
    if occurrences == 0:
        return 0
    if remaining is None:
        return occurrences
    return min(remaining, occurrences)


def _replace_run_text(
    run: dict[str, Any],
    old_value: str,
    new_value: str,
    replace_count: int,
) -> None:
    """Apply replacement to one run."""
    run_text = str(run.get("text") or "")
    run["text"] = run_text.replace(old_value, new_value, replace_count)


def _decrement_remaining_replacements(
    remaining: int | None,
    replace_count: int,
) -> int | None:
    """Track remaining replacement budget for multi-run replacements."""
    if remaining is None:
        return None
    return remaining - replace_count


def _paragraph_is_empty(paragraph: dict[str, Any]) -> bool:
    """Return True when paragraph text and all runs are blank."""
    if str(paragraph.get("text") or "").strip():
        return False

    runs = paragraph.get("runs") or []
    if not isinstance(runs, list):
        return True
    return not any(str(run.get("text") or "").strip() for run in runs if isinstance(run, dict))
