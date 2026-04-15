# Temp-Doc Service

A lightweight, stateless document processing service for extraction, generation, and chunking. No database or S3 storage required—all operations are in-memory.

## Features

- **Extract API**: Convert documents to JSON format
- **Generate API**: Convert JSON back to document formats
- **Chunks API**: Create semantic text chunks from extracted documents
- **Format Support**: DOCX, PDF, PPTX, HTML, Markdown, TXT
- **Stateless**: In-memory processing—no storage dependencies

## Quick Start

### Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Run

```bash
python main.py
```

Service will start on `http://localhost:8006`

## API Endpoints

### 1. Extract: Convert Document to JSON

**Request:**
```http
POST /extract
Content-Type: multipart/form-data

Body:
  file: <document file>
```

**Supported Formats:** DOCX, PDF, PPTX, HTML, MD, TXT

**Response:**
```json
{
  "extension": "docx",
  "output_format": "json",
  "extracted_data": {
    "title": "Document Title",
    "paragraphs": [...],
    "tables": [...],
    "styles": {...}
  }
}
```

---

### 2. Generate: Convert JSON to Document

**Request:**
```http
POST /generate
Content-Type: application/json

Body:
{
  "output_format": "docx",
  "extracted_data": {...},
  "file_name": "output",
  "title": "My Document"
}
```

**Parameters:**
- `output_format` (required): `docx`, `pdf`, `pptx`, `html`, `markdown`, or `text`
- `extracted_data` (required): Extracted document JSON from `/extract` endpoint
- `file_name` (optional): Base filename without extension (defaults to "document")
- `title` (optional): Document title for generation

**Response:**
- Binary file content with appropriate `Content-Type` header
- `Content-Disposition` header with filename

---

### 3. Chunks: Create Text Chunks from Document

**Request:**
```http
POST /chunks
Content-Type: application/json

Body:
{
  "extracted_data": {...},
  "extension": "docx",
  "file_name": "document"
}
```

**Parameters:**
- `extracted_data` (required): Extracted document JSON from `/extract`
- `extension` (optional): Source format for context (docx, pptx, pdf, markdown, text)
- `file_name` (optional): Document name for reference

**Response:**
```json
{
  "filename": "document.docx",
  "chunks": [
    { "text": "First chunk of text..." },
    { "text": "Second chunk of text..." }
  ]
}
```

**Supported Formats:** DOCX, PDF, PPTX, Markdown, TXT

---

## Architecture

```
temp-doc/
├── app/
│   ├── api/
│   │   └── routes.py          # HTTP route handlers (extract, generate, chunks)
│   ├── helper/
│   │   ├── extract/           # Document extraction logic
│   │   ├── generate/          # Document generation logic
│   │   └── chunks/            # Text chunking logic
│   ├── services/
│   │   └── chunking_service.py    # Chunking service (text splitting)
│   ├── schemas/
│   │   └── temp_doc_schema.py     # Pydantic models for I/O validation
│   └── config/
│       └── logging_config.py      # Logging configuration
├── main.py                    # Entry point and FastAPI app setup
└── requirements.txt           # Python dependencies
```

## Design Principles

- **Minimal**: Only extract, generate, and chunks functionality
- **Stateless**: No persistent state or dependencies on external services
- **Helper-Based**: Business logic centralized in helper modules (extract, generate, chunks)
- **Clean Routes**: HTTP endpoints are pure adapters (validate input, call helpers, return response)

## Example Usage

### Full Workflow

```bash
# 1. Extract a document
curl -X POST -F "file=@my_document.docx" http://localhost:8006/extract > extracted.json

# 2. Generate a new format from extraction
curl -X POST -H "Content-Type: application/json" \
  -d '{"output_format": "pdf", "extracted_data": $(cat extracted.json | jq .extracted_data)}' \
  http://localhost:8006/generate > output.pdf

# 3. Create chunks for embedding/semantic search
curl -X POST -H "Content-Type: application/json" \
  -d '{"extracted_data": $(cat extracted.json | jq .extracted_data)}' \
  http://localhost:8006/chunks > chunks.json
```

## Error Handling

All endpoints return appropriate HTTP status codes:
- `200 OK` - Successful operation
- `400 Bad Request` - Missing or invalid parameters
- `422 Unprocessable Entity` - Invalid payload structure
- `500 Internal Server Error` - Processing error

Error responses include a `detail` field with description:
```json
{
  "detail": "Missing 'extracted_data' in request body."
}
```

## Performance Notes

- Extraction and generation are CPU-intensive; use for moderate file sizes
- Chunking splits documents into semantically meaningful sections
- All operations are synchronous but can handle concurrent requests via FastAPI
- No caching; each request processes independently
