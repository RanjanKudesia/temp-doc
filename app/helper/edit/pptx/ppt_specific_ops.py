"""PPT-specific patch operations (slides, text, formatting)."""

import copy
from typing import Any

from ..shared.json_patch_service import (
    _apply_advanced_instruction,
    _apply_generic_instruction,
    _build_default_runs,
    _build_paragraph_payload,
    _decode_pointer_tokens,
    _find_indexed_item_position,
    _parse_list_index,
    _require_int,
    _required_path,
    _resolve_dict_at_path,
    _row_cells,
    _table_rows,
    TABLE_TARGET_INVALID,
    ADVANCED_OPS,
)

from .ppt_context import _PptContext
from .ppt_normalization import normalize_ppt_document

PPT_SPECIFIC_OPS = {
    "add_slide", "remove_slide", "replace_slide_title", "replace_slide_notes",
    "move_slide", "duplicate_slide", "swap_slides", "replace_text_in_slide",
    "set_paragraph_formatting", "set_run_formatting", "set_table_cell_text",
    "bulk_replace_text",
}

_PARA_LEVEL_FORMAT_FIELDS: frozenset[str] = frozenset({
    "style", "alignment", "direction", "is_bullet",
    "is_numbered", "list_level", "list_info", "numbering_format",
})
_RUN_LEVEL_FORMAT_FIELDS: frozenset[str] = frozenset({
    "bold", "italic", "underline", "strikethrough", "code",
    "font_name", "font_size_pt", "color_rgb", "highlight_color", "hyperlink_url",
})

SLIDES_INVALID = "Document slides collection is invalid"
SLIDE_NOT_FOUND = "Slide index not found"


# ---------------------------------------------------------------------------
# Instruction dispatcher
# ---------------------------------------------------------------------------

def apply_ppt_instruction(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
    ctx: _PptContext,
) -> None:
    """Dispatch one instruction and normalise the document afterwards."""
    op = str(instruction.get("op") or "").strip().lower()

    paras_before = {id(p) for p in (document.get("paragraphs") or [])
                    if isinstance(p, dict)}
    tables_before = {id(t) for t in (document.get("tables") or [])
                     if isinstance(t, dict)}

    if op in PPT_SPECIFIC_OPS:
        _apply_ppt_specific(document, instruction, idx, ctx)
    elif op in ADVANCED_OPS:
        anchor_slide = _anchor_slide_for_op(document, instruction, op, ctx)
        _apply_advanced_instruction(document, instruction, idx)
        ctx.assign_new_paragraphs(document, paras_before, anchor_slide)
        ctx.assign_new_tables(document, tables_before, anchor_slide)
    else:
        anchor_slide = _anchor_slide_for_generic(document, instruction, ctx)
        _apply_generic_instruction(document, instruction, idx)
        ctx.assign_new_paragraphs(document, paras_before, anchor_slide)
        ctx.assign_new_tables(document, tables_before, anchor_slide)

    normalize_ppt_document(document)
    ctx.rebuild_slide_indices(document)


# ---------------------------------------------------------------------------
# Slide anchor helpers
# ---------------------------------------------------------------------------

def _anchor_slide_for_op(
    document: dict[str, Any],
    instruction: dict[str, Any],
    op: str,
    ctx: _PptContext,
) -> int | None:
    """Return the slide_index that should own newly created items for this op."""
    if op in ("insert_paragraph_after", "remove_paragraph", "remove_empty_paragraphs"):
        anchor = instruction.get("index")
        if isinstance(anchor, int):
            return ctx.slide_for_para_index(document, anchor)
    if op in ("insert_table_after", "remove_table"):
        anchor = instruction.get("index")
        if isinstance(anchor, int):
            return ctx.slide_for_table_index(document, anchor)
    return None


def _anchor_slide_for_generic(
    document: dict[str, Any],
    instruction: dict[str, Any],
    ctx: _PptContext,
) -> int | None:
    """Return a default slide for items inserted via generic add op."""
    op = str(instruction.get("op") or "").strip().lower()
    if op != "add":
        return None

    path = str(instruction.get("path") or "")
    if not path.startswith("/"):
        return None

    tokens = _decode_pointer_tokens(path)
    if not tokens:
        return None

    return _resolve_anchor_slide(document, tokens, ctx)


def _resolve_anchor_slide(
    document: dict[str, Any],
    tokens: list[str],
    ctx: _PptContext,
) -> int | None:
    """Resolve anchor slide based on path tokens."""
    slides = document.get("slides") or []
    first_slide_index = next(
        (s.get("index") for s in slides if isinstance(s, dict)
         and isinstance(s.get("index"), int)),
        None,
    )

    if tokens[0] == "paragraphs":
        return _anchor_for_paragraph_path(document, tokens, ctx, first_slide_index)

    if tokens[0] == "tables":
        return first_slide_index

    return None


def _anchor_for_paragraph_path(
    document: dict[str, Any],
    tokens: list[str],
    ctx: _PptContext,
    default_slide: int | None,
) -> int | None:
    """Resolve anchor slide for paragraph path tokens."""
    if len(tokens) == 2:
        try:
            para_idx = _parse_list_index(tokens[1], allow_append=False)
            paras = document.get("paragraphs") or []
            if para_idx < len(paras):
                return ctx.para_id_to_slide.get(id(paras[para_idx]))
        except (ValueError, TypeError):
            pass
    return default_slide


# ---------------------------------------------------------------------------
# PPT-specific operation dispatcher
# ---------------------------------------------------------------------------

def _apply_ppt_specific(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
    ctx: _PptContext,
) -> None:
    """Dispatch to specific PPT operation handler."""
    op = str(instruction.get("op") or "").strip().lower()

    # Simple slide operations
    simple_ops = {
        "add_slide": _add_slide,
        "remove_slide": _remove_slide,
        "replace_slide_title": _replace_slide_title,
        "replace_slide_notes": _replace_slide_notes,
        "move_slide": _move_slide,
        "duplicate_slide": _duplicate_slide,
        "swap_slides": _swap_slides,
    }

    # Complex text/format operations
    complex_ops = {
        "replace_text_in_slide": _replace_text_in_slide,
        "set_paragraph_formatting": _set_paragraph_formatting,
        "set_run_formatting": _set_run_formatting,
        "set_table_cell_text": _set_table_cell_text,
        "bulk_replace_text": _bulk_replace_text,
    }

    if op in simple_ops:
        if op in ("remove_slide", "duplicate_slide"):
            simple_ops[op](document, instruction, idx, ctx)
        else:
            simple_ops[op](document, instruction, idx)
        return

    if op in complex_ops:
        complex_ops[op](document, instruction, idx)
        return

    raise ValueError(f"Instruction[{idx}] has unsupported PPT op: {op}")


# ---------------------------------------------------------------------------
# Simple slide operations
# ---------------------------------------------------------------------------

def _add_slide(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Add a new empty slide."""
    slides = _resolve_slides(document)
    anchor_index: int | None = instruction.get("index")
    value = instruction.get("value") or {}

    title = value.get("title") if isinstance(value, dict) else None
    notes_text = value.get("notes_text") if isinstance(value, dict) else None

    new_slide_index = _next_slide_index(slides)
    new_slide = _create_empty_slide(new_slide_index, title, notes_text)

    if anchor_index is None or not slides:
        slides.append(new_slide)
    else:
        insert_at = _find_slide_position(slides, anchor_index, idx)
        slides.insert(insert_at + 1, new_slide)


def _create_empty_slide(
    slide_index: int, title: str | None, notes_text: str | None
) -> dict[str, Any]:
    """Create empty slide dict with given metadata."""
    return {
        "index": slide_index,
        "slide_number": slide_index + 1,
        "slide_id": None,
        "path": None,
        "title": title,
        "text": title or "",
        "notes_text": notes_text,
        "paragraph_indices": [],
        "table_indices": [],
        "media_indices": [],
        "shape_count": 0,
        "image_count": 0,
        "table_count": 0,
    }


def _next_slide_index(slides: list[Any]) -> int:
    """Get next available slide index."""
    if not slides:
        return 0
    return max(
        (s.get("index", 0) for s in slides if isinstance(s, dict)),
        default=0
    ) + 1


def _remove_slide(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
    ctx: _PptContext,
) -> None:
    """Remove a slide and its owned content."""
    slides = _resolve_slides(document)
    slide_index = _require_int(instruction.get("index"), idx, "index")
    slide_pos = _find_slide_position(slides, slide_index, idx)
    slide = slides[slide_pos]

    para_indices = set(slide.get("paragraph_indices") or [])
    table_indices = set(slide.get("table_indices") or [])
    media_indices = set(slide.get("media_indices") or [])

    _remove_items_from_collections(
        document, para_indices, table_indices, media_indices)
    _remove_items_from_context(
        ctx, document, para_indices, table_indices, media_indices)
    _remove_items_from_order(document, para_indices,
                             table_indices, media_indices)

    del slides[slide_pos]


def _remove_items_from_collections(
    document: dict[str, Any],
    para_indices: set[int],
    table_indices: set[int],
    media_indices: set[int],
) -> None:
    """Remove items from flat collections."""
    paragraphs = document.get("paragraphs") or []
    tables = document.get("tables") or []
    media = document.get("media") or []

    document["paragraphs"] = [
        p for p in paragraphs
        if not (isinstance(p, dict) and p.get("index") in para_indices)
    ]
    document["tables"] = [
        t for t in tables
        if not (isinstance(t, dict) and t.get("index") in table_indices)
    ]
    document["media"] = [
        m for m in media
        if not (isinstance(m, dict) and m.get("index") in media_indices)
    ]


def _remove_items_from_context(
    ctx: _PptContext,
    document: dict[str, Any],
    para_indices: set[int],
    table_indices: set[int],
    media_indices: set[int],
) -> None:
    """Remove items from context maps."""
    for p in (document.get("paragraphs") or []):
        if isinstance(p, dict) and p.get("index") in para_indices:
            ctx.para_id_to_slide.pop(id(p), None)
    for t in (document.get("tables") or []):
        if isinstance(t, dict) and t.get("index") in table_indices:
            ctx.table_id_to_slide.pop(id(t), None)
    for m in (document.get("media") or []):
        if isinstance(m, dict) and m.get("index") in media_indices:
            ctx.media_id_to_slide.pop(id(m), None)


def _remove_items_from_order(
    document: dict[str, Any],
    para_indices: set[int],
    table_indices: set[int],
    media_indices: set[int],
) -> None:
    """Remove items from document_order."""
    doc_order = document.get("document_order") or []
    document["document_order"] = [
        o for o in doc_order
        if not (
            isinstance(o, dict)
            and (
                (o.get("type") == "paragraph" and o.get("index") in para_indices)
                or (o.get("type") == "table" and o.get("index") in table_indices)
                or (o.get("type") == "media" and o.get("index") in media_indices)
            )
        )
    ]


def _replace_slide_title(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Replace slide title."""
    slides = _resolve_slides(document)
    slide_index = _require_int(instruction.get("index"), idx, "index")
    new_value = instruction.get("new_value")
    if new_value is None:
        raise ValueError(
            f"Instruction[{idx}] new_value required for replace_slide_title")
    slide_pos = _find_slide_position(slides, slide_index, idx)
    slides[slide_pos]["title"] = str(new_value)


def _replace_slide_notes(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Replace slide notes."""
    slides = _resolve_slides(document)
    slide_index = _require_int(instruction.get("index"), idx, "index")
    new_value = instruction.get("new_value")
    if new_value is None:
        raise ValueError(
            f"Instruction[{idx}] new_value required for replace_slide_notes")
    slide_pos = _find_slide_position(slides, slide_index, idx)
    slides[slide_pos]["notes_text"] = str(new_value)


def _move_slide(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Move slide to new position."""
    slides = _resolve_slides(document)
    slide_index_a = _require_int(instruction.get("index"), idx, "index")
    target_index = _require_int(instruction.get(
        "target_index"), idx, "target_index")

    from_pos = _find_slide_position(slides, slide_index_a, idx)
    slide = slides.pop(from_pos)

    if target_index == -1:
        slides.insert(0, slide)
        return

    to_pos = len(slides)
    for pos, s in enumerate(slides):
        if isinstance(s, dict) and s.get("index") == target_index:
            to_pos = pos + 1
            break

    slides.insert(to_pos, slide)


# ---------------------------------------------------------------------------
# Complex slide operations
# ---------------------------------------------------------------------------

def _duplicate_slide(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
    ctx: _PptContext,
) -> None:
    """Deep-copy a slide and its owned content."""
    slides = _resolve_slides(document)
    slide_index = _require_int(instruction.get("index"), idx, "index")
    target_index = instruction.get("target_index")
    overrides = instruction.get("value") or {}

    slide_pos = _find_slide_position(slides, slide_index, idx)
    original_slide = slides[slide_pos]

    new_slide_index = _next_slide_index(slides)
    new_slide = copy.deepcopy(original_slide)
    new_slide["index"] = new_slide_index
    new_slide["slide_number"] = new_slide_index + 1
    new_slide["slide_id"] = None
    new_slide["path"] = None

    if isinstance(overrides, dict):
        if "title" in overrides:
            new_slide["title"] = overrides["title"]
        if "notes_text" in overrides:
            new_slide["notes_text"] = overrides["notes_text"]

    _duplicate_slide_content(document, original_slide,
                             new_slide, new_slide_index, ctx)

    if target_index is None:
        slides.append(new_slide)
    else:
        insert_pos = _find_slide_position(slides, target_index, idx)
        slides.insert(insert_pos + 1, new_slide)


def _duplicate_slide_content(
    document: dict[str, Any],
    original_slide: dict[str, Any],
    new_slide: dict[str, Any],
    new_slide_index: int,
    ctx: _PptContext,
) -> None:
    """Duplicate paragraphs, tables, and media for duplicated slide."""
    owned_paras = set(original_slide.get("paragraph_indices") or [])
    owned_tables = set(original_slide.get("table_indices") or [])
    owned_media = set(original_slide.get("media_indices") or [])

    paragraphs: list[dict[str, Any]] = document.get("paragraphs") or []
    tables: list[dict[str, Any]] = document.get("tables") or []
    media_list: list[dict[str, Any]] = document.get("media") or []

    new_paras = _duplicate_items(paragraphs, owned_paras, len(
        paragraphs), ctx, new_slide_index, "para")
    new_tables = _duplicate_items(tables, owned_tables, len(
        tables), ctx, new_slide_index, "table")
    new_media = _duplicate_items(media_list, owned_media, len(
        media_list), ctx, new_slide_index, "media")

    document["paragraphs"] = paragraphs + new_paras
    document["tables"] = tables + new_tables
    document["media"] = media_list + new_media

    new_slide["paragraph_indices"] = [p["index"] for p in new_paras]
    new_slide["table_indices"] = [t["index"] for t in new_tables]
    new_slide["media_indices"] = [m["index"] for m in new_media]
    new_slide["shape_count"] = len(
        new_paras) + len(new_tables) + len(new_media)
    new_slide["table_count"] = len(new_tables)
    new_slide["image_count"] = len(new_media)


def _duplicate_items(
    items: list[dict[str, Any]],
    owned_indices: set[int],
    start_index: int,
    ctx: _PptContext,
    slide_index: int,
    item_type: str,
) -> list[dict[str, Any]]:
    """Duplicate specific items and track them in context."""
    new_items = []
    for item in items:
        if isinstance(item, dict) and item.get("index") in owned_indices:
            new_item = copy.deepcopy(item)
            new_item["index"] = start_index + len(new_items)
            new_items.append(new_item)

            if item_type == "para":
                ctx.para_id_to_slide[id(new_item)] = slide_index
            elif item_type == "table":
                ctx.table_id_to_slide[id(new_item)] = slide_index
            elif item_type == "media":
                ctx.media_id_to_slide[id(new_item)] = slide_index

    return new_items


def _swap_slides(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Swap two slides."""
    slides = _resolve_slides(document)
    slide_index_a = _require_int(instruction.get("index"), idx, "index")
    slide_index_b = _require_int(instruction.get(
        "target_index"), idx, "target_index")

    pos_a = _find_slide_position(slides, slide_index_a, idx)
    pos_b = _find_slide_position(slides, slide_index_b, idx)
    slides[pos_a], slides[pos_b] = slides[pos_b], slides[pos_a]


# ---------------------------------------------------------------------------
# Complex text/format operations
# ---------------------------------------------------------------------------

def _replace_text_in_slide(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Replace text in paragraphs belonging to a slide."""
    slide_index = _require_int(instruction.get("index"), idx, "index")
    old_value = instruction.get("old_value")
    new_value = instruction.get("new_value")
    count = instruction.get("count")

    if old_value is None or new_value is None:
        raise ValueError(
            f"Instruction[{idx}] old_value and new_value required")

    slides = _resolve_slides(document)
    slide_pos = _find_slide_position(slides, slide_index, idx)
    para_indices = set(slides[slide_pos].get("paragraph_indices") or [])

    for p in (document.get("paragraphs") or []):
        if not isinstance(p, dict) or p.get("index") not in para_indices:
            continue
        _replace_in_paragraph(p, old_value, new_value, count)


def _replace_in_paragraph(
    para: dict[str, Any],
    old_value: str,
    new_value: str,
    count: int | None,
) -> None:
    """Apply find/replace to paragraph text and runs."""
    text = str(para.get("text") or "")
    if old_value in text:
        para["text"] = (
            text.replace(old_value, new_value)
            if count is None
            else text.replace(old_value, new_value, int(count))
        )

    for run in (para.get("runs") or []):
        if not isinstance(run, dict):
            continue
        rt = str(run.get("text") or "")
        if old_value in rt:
            run["text"] = (
                rt.replace(old_value, new_value)
                if count is None
                else rt.replace(old_value, new_value, int(count))
            )


def _set_paragraph_formatting(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Apply formatting to paragraph and its runs."""
    para_index = _require_int(instruction.get("index"), idx, "index")
    value = instruction.get("value")
    if not isinstance(value, dict):
        raise ValueError(f"Instruction[{idx}] value must be formatting dict")

    paragraphs = document.get("paragraphs") or []
    pos = _find_indexed_item_position(
        paragraphs, para_index, "index", "Paragraph")
    para = paragraphs[pos]

    _apply_paragraph_fields(para, value)
    _apply_run_fields_to_para(para, value)


def _apply_paragraph_fields(para: dict[str, Any], value: dict[str, Any]) -> None:
    """Apply paragraph-level formatting fields."""
    for field in _PARA_LEVEL_FORMAT_FIELDS:
        if field in value:
            para[field] = value[field]


def _apply_run_fields_to_para(para: dict[str, Any], value: dict[str, Any]) -> None:
    """Apply run-level formatting fields to all runs in paragraph."""
    for run in (para.get("runs") or []):
        if isinstance(run, dict):
            for field in _RUN_LEVEL_FORMAT_FIELDS:
                if field in value:
                    run[field] = value[field]


def _set_run_formatting(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Apply formatting to runs in a paragraph."""
    para_index = _require_int(instruction.get("index"), idx, "index")
    run_index = instruction.get("target_index")
    value = instruction.get("value")
    if not isinstance(value, dict):
        raise ValueError(f"Instruction[{idx}] value must be formatting dict")

    paragraphs = document.get("paragraphs") or []
    pos = _find_indexed_item_position(
        paragraphs, para_index, "index", "Paragraph")
    runs = paragraphs[pos].get("runs") or []

    if run_index is None:
        target_runs = [r for r in runs if isinstance(r, dict)]
    else:
        target_runs = [r for r in runs if isinstance(
            r, dict) and r.get("index") == run_index]
        if not target_runs:
            raise ValueError(
                f"Instruction[{idx}] Run index not found: {run_index}")

    for run in target_runs:
        for field in _RUN_LEVEL_FORMAT_FIELDS:
            if field in value:
                run[field] = value[field]


def _set_table_cell_text(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Set text of specific table cell."""
    path = _required_path(instruction.get("path"), idx, "set_table_cell_text")
    row_index = _require_int(instruction.get("row_index"), idx, "row_index")
    column_index = _require_int(instruction.get(
        "column_index"), idx, "column_index")
    new_text = str(instruction.get("value") or "")

    table = _resolve_dict_at_path(document, path, TABLE_TARGET_INVALID)
    rows = _table_rows(table)

    row_pos = _find_indexed_item_position(rows, row_index, "row_index", "Row")
    cells = _row_cells(rows[row_pos])

    if column_index >= len(cells):
        raise ValueError(f"Instruction[{idx}] column_index out of range")

    cell = cells[column_index]
    cell["text"] = new_text

    cell_paras = cell.get("paragraphs") or []
    if cell_paras and isinstance(cell_paras[0], dict):
        cell_paras[0]["text"] = new_text
        runs = cell_paras[0].get("runs") or []
        if runs and isinstance(runs[0], dict):
            runs[0]["text"] = new_text
        else:
            cell_paras[0]["runs"] = _build_default_runs(new_text)
    elif new_text:
        cell["paragraphs"] = [_build_paragraph_payload(new_text, 0)]


def _bulk_replace_text(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> None:
    """Apply multiple find/replace pairs to paragraphs."""
    replacements = instruction.get("value")
    if not isinstance(replacements, list):
        raise ValueError(
            f"Instruction[{idx}] value must be list of replacements")

    para_indices = _get_bulk_replace_scope(document, instruction, idx)
    paragraphs: list[dict[str, Any]] = document.get("paragraphs") or []

    for p in paragraphs:
        if not isinstance(p, dict):
            continue
        if para_indices is not None and p.get("index") not in para_indices:
            continue

        for replacement in replacements:
            if not isinstance(replacement, dict):
                continue
            _apply_replacement_to_para(p, replacement)


def _get_bulk_replace_scope(
    document: dict[str, Any],
    instruction: dict[str, Any],
    idx: int,
) -> set[int] | None:
    """Get the paragraph indices scope for bulk replace (or None for all)."""
    slide_filter = instruction.get("index")
    if slide_filter is None:
        return None

    slides = _resolve_slides(document)
    slide_pos = _find_slide_position(slides, slide_filter, idx)
    return set(slides[slide_pos].get("paragraph_indices") or [])


def _apply_replacement_to_para(
    para: dict[str, Any],
    replacement: dict[str, Any],
) -> None:
    """Apply a single find/replace to a paragraph and its runs."""
    old = str(replacement.get("old_value") or "")
    new = str(replacement.get("new_value") or "")
    rep_count = replacement.get("count")
    if not old:
        return

    _replace_in_paragraph(para, old, new, rep_count)


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _resolve_slides(document: dict[str, Any]) -> list[dict[str, Any]]:
    """Return slides list, raising on invalid."""
    slides = document.get("slides")
    if not isinstance(slides, list):
        raise ValueError(SLIDES_INVALID)
    return slides


def _find_slide_position(
    slides: list[Any], slide_index: int, instruction_idx: int
) -> int:
    """Find position of slide with given index."""
    for pos, slide in enumerate(slides):
        if isinstance(slide, dict) and slide.get("index") == slide_index:
            return pos
    raise ValueError(
        f"Instruction[{instruction_idx}] {SLIDE_NOT_FOUND}: {slide_index}")
