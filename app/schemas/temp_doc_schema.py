"""Pydantic schemas for document extraction and generation requests/responses."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


# ==================== Extraction Schemas ====================


class ExtractedMediaItem(BaseModel):
    """Media item extracted from document."""

    relationship_id: str | None = None
    content_type: str | None = None
    file_name: str | None = None
    local_file_path: str | None = None
    local_url: str | None = None
    width_emu: int | None = None
    height_emu: int | None = None
    alt_text: str | None = None
    source: dict | None = None
    base64_data: str | None = None
    base64: str | None = None
    model_config = ConfigDict(extra="allow")


class ExtractedRun(BaseModel):
    """Text run (formatted text unit)."""

    index: int | None = None
    text: str | None = None
    bold: bool | None = None
    italic: bool | None = None
    underline: bool | None = None
    strikethrough: bool | None = None
    code: bool | None = None
    font_name: str | None = None
    font_size_pt: float | None = None
    color_rgb: str | None = None
    highlight_color: str | None = None
    hyperlink_url: str | None = None
    embedded_media: list[ExtractedMediaItem] = Field(default_factory=list)
    model_config = ConfigDict(extra="allow")


class ExtractedStyleFont(BaseModel):
    """Font style information."""

    name: str | None = None
    size_pt: float | None = None
    bold: bool | None = None
    italic: bool | None = None
    underline: bool | None = None
    color_rgb: str | None = None
    highlight_color: str | None = None
    model_config = ConfigDict(extra="allow")


class ExtractedStyle(BaseModel):
    """Document style definition."""

    style_id: str | None = None
    name: str | None = None
    type: str | None = None
    font: ExtractedStyleFont | None = None
    model_config = ConfigDict(extra="allow")


class ExtractedDocumentDefaults(BaseModel):
    """Default document formatting."""

    font_name: str | None = None
    font_size_pt: float | None = None
    color_rgb: str | None = None
    model_config = ConfigDict(extra="allow")


class ListInfo(BaseModel):
    """List formatting for a paragraph."""

    kind: Literal["bullet", "numbered"] | None = None
    numbering_format: str | None = None
    level: int = 0
    start: int | None = None
    model_config = ConfigDict(extra="allow")


class SourceInfo(BaseModel):
    """Source HTML element metadata for tracing back to the original markup."""

    format: str | None = None
    tag: str | None = None
    attrs: dict[str, Any] = Field(default_factory=dict)
    raw_html: str | None = None
    model_config = ConfigDict(extra="allow")


class ExtractedParagraph(BaseModel):
    """Extracted paragraph from document."""

    index: int
    text: str | None = None
    style: str | None = None
    code_fence_language: str | None = None
    is_bullet: bool | None = None
    is_numbered: bool | None = None
    list_info: ListInfo | None = None
    numbering_format: str | None = None
    list_level: int | None = None
    alignment: str | None = None
    direction: str | None = None
    runs: list[ExtractedRun] = Field(default_factory=list)
    source: SourceInfo | None = None
    model_config = ConfigDict(extra="allow")


class ExtractedTableCell(BaseModel):
    """Table cell content."""

    text: str | None = None
    paragraphs: list[ExtractedParagraph] = Field(default_factory=list)
    tables: list["ExtractedTable"] = Field(default_factory=list)
    is_header: bool | None = None
    cell_index: int | None = None
    colspan: int | None = None
    rowspan: int | None = None
    nested_table_indices: list[int] = Field(default_factory=list)
    source: SourceInfo | None = None
    model_config = ConfigDict(extra="allow")


class ExtractedTableRow(BaseModel):
    """Table row."""

    cells: list[ExtractedTableCell] = Field(default_factory=list)
    row_index: int | None = None
    model_config = ConfigDict(extra="allow")


class ExtractedTable(BaseModel):
    """Extracted table from document."""

    index: int
    row_count: int | None = None
    column_count: int | None = None
    style: str | None = None
    rows: list[ExtractedTableRow] = Field(default_factory=list)
    source: SourceInfo | None = None
    model_config = ConfigDict(extra="allow")


class ExtractedOrderItem(BaseModel):
    """Order item for document body."""

    type: Literal["paragraph", "table", "media"]
    index: int


class HtmlMetadata(BaseModel):
    """Rich metadata extracted from an HTML document."""

    source_type: str | None = None
    extraction_mode: str | None = None
    title: str | None = None
    doctype: str | None = None
    full_html: str | None = None
    head_html: str | None = None
    body_html: str | None = None
    html_attributes: dict[str, Any] = Field(default_factory=dict)
    body_attributes: dict[str, Any] = Field(default_factory=dict)
    style_blocks: list[str] = Field(default_factory=list)
    meta_tags: list[dict[str, Any]] = Field(default_factory=list)
    link_tags: list[dict[str, Any]] = Field(default_factory=list)
    script_blocks: list[dict[str, Any]] = Field(default_factory=list)
    model_config = ConfigDict(extra="allow")


class ExtractedData(BaseModel):
    """Complete extracted document data in JSON format."""

    metadata: HtmlMetadata | dict[str, Any] | None = None
    document_order: list[ExtractedOrderItem] = Field(default_factory=list)
    document_defaults: ExtractedDocumentDefaults | None = None
    styles: list[ExtractedStyle] = Field(default_factory=list)
    paragraphs: list[ExtractedParagraph] = Field(default_factory=list)
    tables: list[ExtractedTable] = Field(default_factory=list)
    media: list[ExtractedMediaItem] = Field(default_factory=list)
    model_config = ConfigDict(extra="allow")


# ==================== XML Format Schemas ====================


class ExtractedXmlPart(BaseModel):
    """XML part from document (e.g., word/document.xml)."""

    path: str
    xml: str
    model_config = ConfigDict(extra="allow")


class ExtractedXmlRun(BaseModel):
    """XML preserved run."""

    index: int | None = None
    text: str | None = None
    bold: bool | None = None
    italic: bool | None = None
    underline: bool | None = None
    strikethrough: bool | None = None
    font_name: str | None = None
    font_size_pt: float | None = None
    color_rgb: str | None = None
    hyperlink_url: str | None = None
    model_config = ConfigDict(extra="allow")


class ExtractedXmlParagraph(BaseModel):
    """XML preserved paragraph."""

    index: int
    text: str | None = None
    alignment: str | None = None
    runs: list[ExtractedXmlRun] = Field(default_factory=list)
    model_config = ConfigDict(extra="allow")


class ExtractedXmlData(BaseModel):
    """Complete extracted document data in XML format."""

    parts: list[ExtractedXmlPart] = Field(default_factory=list)
    paragraphs: list[ExtractedXmlParagraph] = Field(default_factory=list)
    document_defaults: ExtractedDocumentDefaults | None = None
    styles: list[ExtractedStyle] = Field(default_factory=list)
    parsed_body: bool | None = None
    model_config = ConfigDict(extra="allow")


class ExtractedPptData(BaseModel):
    """Extracted PowerPoint data."""

    format: str | None = None
    document_type: Literal["pptx"]
    metadata: dict[str, Any] | None = None

    # JSON-adapter shape
    document_order: list[ExtractedOrderItem] = Field(default_factory=list)
    styles: list[ExtractedStyle] = Field(default_factory=list)
    numbering: list[dict[str, Any]] = Field(default_factory=list)
    sections: list[dict[str, Any]] = Field(default_factory=list)
    slides: list[dict[str, Any]] = Field(default_factory=list)
    media: list[ExtractedMediaItem] = Field(default_factory=list)
    paragraphs: list[ExtractedParagraph] = Field(default_factory=list)
    tables: list[ExtractedTable] = Field(default_factory=list)

    # XML-pipeline shape
    content_types: dict[str, Any] | None = None
    presentation: dict[str, Any] | None = None
    presentation_relationships: dict[str, Any] | None = None
    parsed_slides: list[dict[str, Any]] = Field(default_factory=list)
    parts: list[ExtractedXmlPart] = Field(default_factory=list)
    binary_parts: list[dict[str, Any]] = Field(default_factory=list)

    # Additional metadata kept from extraction for compatibility
    source_xml_metadata: dict[str, Any] | None = None
    model_config = ConfigDict(extra="allow")


# ==================== API Request/Response Schemas ====================


class ExtractRequest(BaseModel):
    """Request model for extraction - implicit via form data."""


class ExtractResponse(BaseModel):
    """Response model for extraction."""

    extension: str
    output_format: Literal["json", "xml"]
    extracted_data: ExtractedData | ExtractedXmlData | ExtractedPptData


class GenerateRequest(BaseModel):
    """Request model for generation."""

    output_format: Literal["docx", "pdf", "pptx", "html", "markdown", "text"]
    extracted_data: ExtractedData | ExtractedXmlData | ExtractedPptData
    file_name: str = "document"
    title: str | None = None
    blocks: list[dict[str, Any]] = Field(default_factory=list)


class GenerateResponse(BaseModel):
    """Response wrapper for generated document."""

    file_name: str
    content_type: str
    extension: str


class ChunkItem(BaseModel):
    """Single chunk returned by chunking endpoint."""

    text: str


class ChunkResponse(BaseModel):
    """Response model for text chunks."""

    filename: str
    chunks: list[ChunkItem] = Field(default_factory=list)


class PatchInstruction(BaseModel):
    """Single JSON patch-like instruction for extracted payload editing."""

    op: Literal[
        "add",
        "replace",
        "remove",
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
    ]
    path: str | None = None
    value: Any | None = None
    index: int | None = None
    old_value: str | None = None
    new_value: str | None = None
    count: int | None = None


class EditRequest(BaseModel):
    """Request model for extracted JSON edit endpoint."""

    extracted_data: ExtractedData | ExtractResponse
    instructions: list[PatchInstruction] = Field(default_factory=list)


class EditResponse(BaseModel):
    """Response model for extracted JSON edits."""

    extension: Literal["docx", "html", "md", "txt"]
    output_format: Literal["json"] = "json"
    extracted_data: ExtractedData
    applied_instructions: int


class PptPatchInstruction(BaseModel):
    """Single patch instruction for PPT extracted payload editing."""

    op: Literal[
        # Generic JSON-pointer ops
        "add",
        "replace",
        "remove",
        # Shared structural ops (same semantics as DOCX)
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
        # PPT-specific ops
        "add_slide",
        "remove_slide",
        "replace_slide_title",
        "replace_slide_notes",
        "move_slide",
        # Complex PPT-specific ops
        "duplicate_slide",
        "swap_slides",
        "replace_text_in_slide",
        "set_paragraph_formatting",
        "set_run_formatting",
        "set_table_cell_text",
        "bulk_replace_text",
    ]
    path: str | None = None
    value: Any | None = None
    index: int | None = None
    old_value: str | None = None
    new_value: str | None = None
    count: int | None = None
    # destination for move_slide / second slide for swap_slides
    target_index: int | None = None
    row_index: int | None = None      # set_table_cell_text: 0-based row
    column_index: int | None = None   # set_table_cell_text: 0-based column


class PptEditRequest(BaseModel):
    """Request model for PPT extracted JSON edit endpoint."""

    extracted_data: ExtractedPptData | ExtractResponse
    instructions: list[PptPatchInstruction] = Field(default_factory=list)


class PptEditResponse(BaseModel):
    """Response model for PPT extracted JSON edits."""

    extension: Literal["pptx"] = "pptx"
    output_format: Literal["json"] = "json"
    extracted_data: ExtractedPptData
    applied_instructions: int
