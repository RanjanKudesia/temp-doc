# Edit Instructions Allowed

This file documents the current behavior of the unified edit entry.

- API endpoint: `/edit`
- Deprecated endpoint: `/edit/ppt` (removed)
- Common entry point in code: `app/helper/edit/__init__.py` -> `edit_document(request_body)`
- Extension-specific services:
  - `docx_edit_service.py`
  - `html_edit_service.py`
  - `markdown_edit_service.py`
  - `text_edit_service.py`
  - `ppt_edit_service.py`

## Request Shape

Use this structure for all supported extensions.

```json
{
  "extracted_data": {
    "extension": "docx",
    "output_format": "json",
    "extracted_data": {}
  },
  "instructions": [
    {
      "op": "replace_text",
      "path": "/paragraphs/0/text",
      "old_value": "Old",
      "new_value": "New"
    }
  ]
}
```

`extracted_data` supports both:
- raw extracted payload (`ExtractedData` or `ExtractedPptData`), and
- wrapped `/extract` response payload (`{extension, output_format, extracted_data}`)

`/edit` also accepts full `/edit` response payloads for chaining (add/update `instructions` and submit again).

## Supported Extensions

`/edit` supports:
- `docx` (`doc`, `docx`)
- `html` (`html`, `htm`)
- `md` (`md`, `markdown`)
- `txt` (`txt`, `text`)
- `pptx` (`ppt`, `pptx`)

## Common Operations (All Structured Extensions)

These are supported for `docx`, `html`, `md`, `txt`, and also for `pptx`.

### Generic JSON-pointer operations

- `add`
- `replace`
- `remove`

Required fields:
- `op`
- `path`
- `value` (required for `add` and `replace`)

Notes:
- `path` must be JSON-pointer style, example: `/paragraphs/2/text`
- For list append with `add`, use `/-`

### Text operation

- `replace_text`

Required fields:
- `op`
- `path`
- `old_value`
- `new_value`

Optional fields:
- `count`

Behavior:
- Replaces text at a string target
- Keeps paragraph `text` and run `text` aligned when targeting paragraph/run paths

### Paragraph operations

- `insert_paragraph_after`
- `remove_paragraph`
- `remove_empty_paragraphs`

Required fields:
- `insert_paragraph_after`: `op`, `index`
- `remove_paragraph`: `op`, `index`
- `remove_empty_paragraphs`: `op`

Optional fields:
- `path`
- `value` (for `insert_paragraph_after`)

### Table operations

- `insert_table_after`
- `remove_table`
- `insert_table_row`
- `remove_table_row`
- `insert_table_column`
- `remove_table_column`

Required fields:
- `insert_table_after`: `op`, `index`
- `remove_table`: `op`, `index`
- `insert_table_row`: `op`, `path`, `index`
- `remove_table_row`: `op`, `path`, `index`
- `insert_table_column`: `op`, `path`, `index`
- `remove_table_column`: `op`, `path`, `index`

Optional fields:
- `value` (for insert operations)

## PPTX-specific Operations

In addition to all common operations above, `pptx` supports these operations.

### Slide operations

- `add_slide`
- `remove_slide`
- `replace_slide_title`
- `replace_slide_notes`
- `move_slide`
- `duplicate_slide`
- `swap_slides`

### Slide/content scoped operations

- `replace_text_in_slide`
- `set_paragraph_formatting`
- `set_run_formatting`
- `set_table_cell_text`
- `bulk_replace_text`

### Field requirements for new PPTX operations

- `duplicate_slide`
  - required: `op`, `index`
  - optional: `target_index`, `value` (`title`, `notes_text`)

- `swap_slides`
  - required: `op`, `index`, `target_index`

- `replace_text_in_slide`
  - required: `op`, `index`, `old_value`, `new_value`
  - optional: `count`

- `set_paragraph_formatting`
  - required: `op`, `index`, `value` (formatting dict)

- `set_run_formatting`
  - required: `op`, `index`, `value` (formatting dict)
  - optional: `target_index` (run index; omit to apply to all runs)

- `set_table_cell_text`
  - required: `op`, `path`, `row_index`, `column_index`, `value`

- `bulk_replace_text`
  - required: `op`, `value` (list of replacement objects)
  - optional: `index` (slide scope)

## Allowed `op` Values by Extension

### docx/html/md/txt

- `add`
- `replace`
- `remove`
- `replace_text`
- `insert_paragraph_after`
- `remove_paragraph`
- `remove_empty_paragraphs`
- `insert_table_after`
- `remove_table`
- `insert_table_row`
- `remove_table_row`
- `insert_table_column`
- `remove_table_column`

### pptx

All operations above, plus:
- `add_slide`
- `remove_slide`
- `replace_slide_title`
- `replace_slide_notes`
- `move_slide`
- `duplicate_slide`
- `swap_slides`
- `replace_text_in_slide`
- `set_paragraph_formatting`
- `set_run_formatting`
- `set_table_cell_text`
- `bulk_replace_text`

## Chaining Compatibility

`/edit` response can be reused directly:

1. `/edit` -> `/edit`
   - Add a new `instructions` array and call `/edit` again
2. `/edit` -> `/generate`
   - Send the edit response payload to `/generate`

The service detects extension from top-level `extension` and/or payload metadata.

## Notes

- `document_order`, paragraph/table indices, and derived table metadata are normalized after edits.
- For `replace_text` style updates, prefer `replace_text` over generic `replace` on paragraph text fields when run synchronization is required.
