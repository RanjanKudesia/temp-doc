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
    model_config = ConfigDict(extra="ignore")


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
    model_config = ConfigDict(extra="ignore")


class ExtractedStyleFont(BaseModel):
    """Font style information."""

    name: str | None = None
    size_pt: float | None = None
    bold: bool | None = None
    italic: bool | None = None
    underline: bool | None = None
    color_rgb: str | None = None
    highlight_color: str | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedStyle(BaseModel):
    """Document style definition."""

    style_id: str | None = None
    name: str | None = None
    type: str | None = None
    font: ExtractedStyleFont | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedDocumentDefaults(BaseModel):
    """Default document formatting."""

    font_name: str | None = None
    font_size_pt: float | None = None
    color_rgb: str | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedParagraph(BaseModel):
    """Extracted paragraph from document."""

    index: int
    text: str | None = None
    style: str | None = None
    is_bullet: bool | None = None
    is_numbered: bool | None = None
    list_info: dict | None = None
    numbering_format: str | None = None
    list_level: int | None = None
    alignment: str | None = None
    direction: str | None = None
    runs: list[ExtractedRun] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedTableCell(BaseModel):
    """Table cell content."""

    text: str | None = None
    paragraphs: list[ExtractedParagraph] = Field(default_factory=list)
    tables: list["ExtractedTable"] = Field(default_factory=list)
    is_header: bool | None = None
    colspan: int | None = None
    rowspan: int | None = None
    nested_table_indices: list[int] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedTableRow(BaseModel):
    """Table row."""

    cells: list[ExtractedTableCell] = Field(default_factory=list)
    row_index: int | None = None
    model_config = ConfigDict(extra="ignore")


class ExtractedTable(BaseModel):
    """Extracted table from document."""

    index: int
    row_count: int | None = None
    column_count: int | None = None
    style: str | None = None
    rows: list[ExtractedTableRow] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedOrderItem(BaseModel):
    """Order item for document body."""

    type: Literal["paragraph", "table", "media"]
    index: int


class ExtractedData(BaseModel):
    """Complete extracted document data in JSON format."""

    document_order: list[ExtractedOrderItem] = Field(default_factory=list)
    document_defaults: ExtractedDocumentDefaults | None = None
    styles: list[ExtractedStyle] = Field(default_factory=list)
    paragraphs: list[ExtractedParagraph] = Field(default_factory=list)
    tables: list[ExtractedTable] = Field(default_factory=list)
    media: list[ExtractedMediaItem] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


# ==================== XML Format Schemas ====================


class ExtractedXmlPart(BaseModel):
    """XML part from document (e.g., word/document.xml)."""

    path: str
    xml: str
    model_config = ConfigDict(extra="ignore")


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
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlParagraph(BaseModel):
    """XML preserved paragraph."""

    index: int
    text: str | None = None
    alignment: str | None = None
    runs: list[ExtractedXmlRun] = Field(default_factory=list)
    model_config = ConfigDict(extra="ignore")


class ExtractedXmlData(BaseModel):
    """Complete extracted document data in XML format."""

    parts: list[ExtractedXmlPart] = Field(default_factory=list)
    paragraphs: list[ExtractedXmlParagraph] = Field(default_factory=list)
    document_defaults: ExtractedDocumentDefaults | None = None
    styles: list[ExtractedStyle] = Field(default_factory=list)
    parsed_body: bool | None = None
    model_config = ConfigDict(extra="ignore")


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
    model_config = ConfigDict(extra="ignore")


# ==================== API Request/Response Schemas ====================


class ExtractRequest(BaseModel):
    """Request model for extraction - implicit via form data."""

    pass


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
