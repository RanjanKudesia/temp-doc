"""Utility helpers for JSON pointer and payload shaping operations."""

from __future__ import annotations

from typing import Any

NON_CONTAINER_VALUE_ERROR = "Cannot navigate into non-container value"
DOCUMENT_ORDER_COLLECTION_INVALID = "Document order collection is invalid"


def collection_path(path: Any, default_path: str) -> str:
    """Return a normalized collection path."""
    normalized = str(path or "").strip()
    return normalized or default_path


def required_path(path: Any, idx: int, op_name: str) -> str:
    """Return a required instruction path."""
    normalized = str(path or "").strip()
    if not normalized:
        raise ValueError(f"Instruction[{idx}] path is required for {op_name}")
    return normalized


def require_int(value: Any, idx: int, field_name: str) -> int:
    """Validate integer instruction fields."""
    if not isinstance(value, int):
        raise ValueError(f"Instruction[{idx}] {field_name} is required")
    return value


def resolve_list_at_path(
    document: dict[str, Any],
    path: str,
    error_message: str,
) -> list[Any]:
    """Resolve a JSON pointer path to a list."""
    value = resolve_value(document, path)
    if not isinstance(value, list):
        raise ValueError(error_message)
    return value


def resolve_dict_at_path(
    document: dict[str, Any],
    path: str,
    error_message: str,
) -> dict[str, Any]:
    """Resolve a JSON pointer path to a dict."""
    value = resolve_value(document, path)
    if not isinstance(value, dict):
        raise ValueError(error_message)
    return value


def resolve_value(document: dict[str, Any], path: str) -> Any:
    """Resolve a JSON pointer path to a value."""
    tokens = decode_pointer_tokens(path)
    if not tokens:
        return document

    current: Any = document
    for token in tokens:
        if isinstance(current, dict):
            if token not in current:
                raise ValueError(f"Path segment not found: {token}")
            current = current[token]
            continue
        if isinstance(current, list):
            list_index = parse_list_index(token, allow_append=False)
            if list_index >= len(current):
                raise ValueError(f"List index out of range: {token}")
            current = current[list_index]
            continue
        raise ValueError(NON_CONTAINER_VALUE_ERROR)
    return current


def find_indexed_item_position(
    items: list[Any],
    item_index: int,
    field_name: str,
    item_label: str,
) -> int:
    """Find list position for an item identified by an index field."""
    for position, item in enumerate(items):
        if isinstance(item, dict) and item.get(field_name) == item_index:
            return position
    raise ValueError(f"{item_label} index not found: {item_index}")


def build_paragraph_payload(value: Any, default_index: int) -> dict[str, Any]:
    """Build a paragraph payload from either a string or a paragraph object."""
    paragraph = dict(value) if isinstance(
        value, dict) else {"text": str(value or "")}
    text = str(paragraph.get("text") or "")

    paragraph["index"] = default_index
    paragraph.setdefault("style", "Normal")
    paragraph.setdefault("is_bullet", False)
    paragraph.setdefault("is_numbered", False)
    paragraph.setdefault("list_info", None)
    paragraph.setdefault("numbering_format", None)
    paragraph.setdefault("list_level", None)
    paragraph.setdefault("alignment", None)
    paragraph.setdefault("direction", None)
    paragraph.setdefault("runs", build_default_runs(text))
    return paragraph


def build_default_runs(text: str) -> list[dict[str, Any]]:
    """Build a single default run for paragraph text."""
    return [
        {
            "index": 0,
            "text": text,
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
    ]


def build_table_payload(value: Any, default_index: int) -> dict[str, Any]:
    """Build a table payload from a dict, 2D array, or scalar."""
    if isinstance(value, dict):
        table = dict(value)
    elif isinstance(value, list):
        table = {
            "rows": [
                build_table_row_payload(
                    row_value,
                    row_index,
                    len(row_value) if isinstance(row_value, list) else 1,
                )
                for row_index, row_value in enumerate(value)
            ]
        }
    else:
        table = {"rows": [build_table_row_payload([value], 0, 1)]}

    table["index"] = default_index
    table.setdefault("style", None)
    table.setdefault("rows", [])
    table.setdefault("row_count", len(table["rows"]))
    table.setdefault(
        "column_count",
        max(
            (
                len(row.get("cells", []))
                for row in table["rows"]
                if isinstance(row, dict)
            ),
            default=0,
        ),
    )
    return table


def build_table_row_payload(
    value: Any,
    default_row_index: int,
    column_count: int,
) -> dict[str, Any]:
    """Build a table row payload from a dict or list of cells."""
    if isinstance(value, dict):
        row = dict(value)
        row.setdefault("cells", [])
    elif isinstance(value, list):
        row = {"cells": [build_table_cell_payload(cell) for cell in value]}
    else:
        seed_cells = [value] + [""] * max(column_count - 1, 0)
        row = {"cells": [build_table_cell_payload(
            cell) for cell in seed_cells]}

    row["row_index"] = default_row_index
    row["cells"] = [build_table_cell_payload(
        cell) for cell in row.get("cells", [])]
    return row


def build_table_cell_payload(value: Any) -> dict[str, Any]:
    """Build a table cell payload from a dict or scalar value."""
    if isinstance(value, dict):
        cell = dict(value)
    else:
        text = str(value or "")
        cell = {
            "text": text,
            "paragraphs": [build_paragraph_payload(text, 0)] if text else [],
        }

    cell.setdefault("text", "")
    cell.setdefault("paragraphs", [])
    cell.setdefault("tables", [])
    cell.setdefault("is_header", False)
    cell.setdefault("colspan", None)
    cell.setdefault("rowspan", None)
    cell.setdefault("nested_table_indices", [])
    return cell


def column_cell_payloads(value: Any, row_count: int) -> list[Any]:
    """Build per-row cell payloads for column insertion."""
    if isinstance(value, list):
        if len(value) != row_count:
            raise ValueError("Column value list must match table row count")
        return value
    return [value for _ in range(row_count)]


def table_rows(table: dict[str, Any]) -> list[dict[str, Any]]:
    """Return table rows, ensuring the collection exists."""
    rows = table.setdefault("rows", [])
    if not isinstance(rows, list):
        raise ValueError("Table rows collection is invalid")
    return rows


def row_cells(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Return row cells, ensuring the collection exists."""
    cells = row.setdefault("cells", [])
    if not isinstance(cells, list):
        raise ValueError("Table cells collection is invalid")
    return cells


def document_order(document: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the top-level document order collection."""
    order = document.get("document_order")
    if not isinstance(order, list):
        raise ValueError(DOCUMENT_ORDER_COLLECTION_INVALID)
    return order


def read_container_value(parent: Any, key: str, idx: int) -> Any:
    """Read a value from a dict or list container."""
    if isinstance(parent, dict):
        if key not in parent:
            raise ValueError(f"Instruction[{idx}] key not found: {key}")
        return parent[key]

    if isinstance(parent, list):
        list_index = parse_list_index(key, allow_append=False)
        if list_index >= len(parent):
            raise ValueError(f"Instruction[{idx}] index out of range: {key}")
        return parent[list_index]

    raise ValueError(f"Instruction[{idx}] target is not a container")


def write_container_value(parent: Any, key: str, value: Any, idx: int) -> None:
    """Write a value into a dict or list container."""
    if isinstance(parent, dict):
        if key not in parent:
            raise ValueError(f"Instruction[{idx}] key not found: {key}")
        parent[key] = value
        return

    if isinstance(parent, list):
        list_index = parse_list_index(key, allow_append=False)
        if list_index >= len(parent):
            raise ValueError(f"Instruction[{idx}] index out of range: {key}")
        parent[list_index] = value
        return

    raise ValueError(f"Instruction[{idx}] target is not a container")


def decode_pointer_tokens(path: str) -> list[str]:
    """Decode JSON pointer path into tokens."""
    if path == "":
        return []
    if not path.startswith("/"):
        raise ValueError("Path must start with '/'")

    tokens = path.split("/")[1:]
    return [token.replace("~1", "/").replace("~0", "~") for token in tokens]


def resolve_parent(document: dict[str, Any], path: str) -> tuple[Any, str]:
    """Resolve parent node and final key/index token for a path."""
    tokens = decode_pointer_tokens(path)
    if not tokens:
        raise ValueError("Root path edits are not supported")

    current: Any = document
    for token in tokens[:-1]:
        if isinstance(current, dict):
            if token not in current:
                raise ValueError(f"Path segment not found: {token}")
            current = current[token]
            continue
        if isinstance(current, list):
            list_index = parse_list_index(token, allow_append=False)
            if list_index >= len(current):
                raise ValueError(f"List index out of range: {token}")
            current = current[list_index]
            continue
        raise ValueError(NON_CONTAINER_VALUE_ERROR)

    return current, tokens[-1]


def parse_list_index(token: str, allow_append: bool) -> int:
    """Parse list index token, supporting '-' for append when allowed."""
    if allow_append and token == "-":
        return -1
    if not token.isdigit():
        raise ValueError(f"Invalid list index: {token}")
    return int(token)


def add_value(parent: Any, key: str, value: Any, idx: int) -> None:
    """Add value to dictionary key or list index."""
    if isinstance(parent, dict):
        parent[key] = value
        return

    if isinstance(parent, list):
        list_index = parse_list_index(key, allow_append=True)
        if list_index == -1:
            parent.append(value)
            return
        if list_index > len(parent):
            raise ValueError(
                f"Instruction[{idx}] add index out of range: {key}")
        parent.insert(list_index, value)
        return

    raise ValueError(f"Instruction[{idx}] add target is not a container")


def replace_value(parent: Any, key: str, value: Any, idx: int) -> None:
    """Replace existing value at dictionary key or list index."""
    if isinstance(parent, dict):
        if key not in parent:
            raise ValueError(
                f"Instruction[{idx}] replace key not found: {key}")
        parent[key] = value
        return

    if isinstance(parent, list):
        list_index = parse_list_index(key, allow_append=False)
        if list_index >= len(parent):
            raise ValueError(
                f"Instruction[{idx}] replace index out of range: {key}")
        parent[list_index] = value
        return

    raise ValueError(f"Instruction[{idx}] replace target is not a container")


def remove_value(parent: Any, key: str, idx: int) -> None:
    """Remove value from dictionary key or list index."""
    if isinstance(parent, dict):
        if key not in parent:
            raise ValueError(f"Instruction[{idx}] remove key not found: {key}")
        del parent[key]
        return

    if isinstance(parent, list):
        list_index = parse_list_index(key, allow_append=False)
        if list_index >= len(parent):
            raise ValueError(
                f"Instruction[{idx}] remove index out of range: {key}")
        del parent[list_index]
        return

    raise ValueError(f"Instruction[{idx}] remove target is not a container")
