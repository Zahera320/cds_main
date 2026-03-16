# Multi-User Document Upload & Processing API

A secure, production-ready REST API for uploading, processing, and managing PDF documents with per-user isolation. Built with **FastAPI**, **PostgreSQL**, **PyMuPDF**, and **Docling**.

Includes intelligent page classification, VLM re-verification (Google Gemini), Docling-powered table extraction, luminaire fixture extraction with an 8-layer pipeline, and a full autocount takeoff pipeline for electrical engineering drawings.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [API Endpoints](#api-endpoints)
- [Document Processing Pipeline](#document-processing-pipeline)
- [Security](#security)
- [Database Schema](#database-schema)

---

## Features

### Core Platform
- **JWT Authentication** — Register/login with bcrypt password hashing and HS256 JWT tokens
- **Multi-User Isolation** — Every document operation is scoped to the authenticated user; users cannot access each other's data
- **File Upload** — Upload individual or batch PDF/ZIP files with validation (type, size, empty-file checks)
- **Deduplication** — SHA-256 content hashing prevents the same user from uploading identical files twice
- **Exception Handling** — All routes return structured JSON errors; internal details are never leaked to clients

### Document Parsing Engine
- **Page Processing** — Detect page count, loop through every page in parallel using a thread pool
- **Text Extraction** — Native embedded text extraction with intelligent VLM fallback (Gemini primary, Claude secondary) for scanned documents
- **Page Image Conversion** — Each page rendered as a 300-DPI PNG image stored on disk
- **Page Data Storage** — Per-page records in PostgreSQL: `extracted_text`, `text_length`, `vlm_used`, `image_path`
- **Status Lifecycle** — `uploaded → processing → completed | failed` tracked per document
- **Background Tasks** — Processing runs asynchronously via FastAPI BackgroundTasks so uploads return instantly

### Page Classification & Table Extraction
- **4-Priority Classifier** — Sheet Index lookup → Title Block analysis → Sheet Code prefix rules → Full-page keyword scan, with Gemini VLM re-verification
- **Relevance Tagging** — Pages labeled `LIGHTING_PLAN`, `SCHEDULE`, `SYMBOLS_LEGEND`, `COVER` are marked **relevant**; all other types are irrelevant
- **Classification Summary API** — Single endpoint returns `classification_done` flag, per-page type breakdown, and relevant/irrelevant page number lists
- **On-Demand Reclassification** — `POST /documents/{id}/reclassify` re-runs the classifier on existing extracted text/images without re-processing the PDF
- **Docling Extraction** — Full text, tables, and page images extracted via Docling with GPU acceleration (CUDA)
- **Table Extraction** — Docling-powered structured table extraction with CSV and JSON output
- **Frontend UI** — Classification summary panel, type breakdown bar chart, relevant/irrelevant page number chips, re-classify button, and extracted-tables viewer

### Autocount Pipeline
- **Luminaire Fixture Extraction** — 8-layer pipeline: Docling table detection → keyword filtering → header detection → row parsing → post-parse rejection → VLM fallback (Claude) → combo page handling → deduplication
- **Fixture Takeoff** — Automated fixture counting on lighting plan pages with bounding-box overlays
- **Overlay Images** — Annotated PNG overlays with red bounding boxes around detected fixtures
- **Schedule Export** — Extracted lighting schedules available as downloadable CSV files
- **Split PDF** — Lighting plan pages extracted into a standalone PDF
- **Batch Operations** — Cross-document batch takeoff aggregation for ZIP uploads

---

## Architecture

```
Client  ──▶  FastAPI (Uvicorn)  ──▶  PostgreSQL
                │                        │
                ├── Routes               ├── users
                ├── Services             ├── documents
                ├── Processing Pipeline  └── document_pages
                └── Storage (filesystem)
```

**Request flow:**
1. Client authenticates → receives JWT
2. Client uploads PDF → validated, saved to disk, DB record created
3. Background task fires → opens PDF with PyMuPDF, processes each page in parallel
4. Per-page: render 300-DPI PNG, extract text (native → VLM fallback)
5. Results batch-inserted into `document_pages`, document status → `completed`

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Framework | FastAPI 0.132 + Uvicorn |
| Database | PostgreSQL + SQLAlchemy 2.0 ORM |
| Auth | JWT (python-jose HS256) + bcrypt |
| PDF Processing | PyMuPDF (fitz) |
| Document Extraction | Docling (GPU-accelerated, CUDA) |
| OCR | RapidOCR (via Docling) |
| VLM Classification | Google Gemini 2.5 Flash |
| VLM Table Extraction | Anthropic Claude (fallback) |
| Validation | Pydantic v2 + EmailStr |

---

## Project Structure

```
app.py/                              # Application package
├── main.py                          # FastAPI app bootstrap, CORS, routers, health check
├── config.py                        # Centralized .env configuration
├── database.py                      # SQLAlchemy engine & session factory
├── models.py                        # ORM models (User, Document, DocumentPage)
├── schemas.py                       # Pydantic request/response schemas
├── auth.py                          # JWT creation/verification, bcrypt hashing
├── utils.py                         # FastAPI dependencies (get_db, get_current_user)
├── logging_config.py                # Logging configuration
├── routes/
│   ├── user.py                      # POST /auth/register, POST /auth/login
│   ├── documents.py                 # Document CRUD, upload, batch listing
│   ├── processing.py                # POST /documents/{id}/reprocess, /reclassify
│   ├── pages.py                     # GET /documents/{id}/pages, /page-image
│   ├── classification.py            # GET /documents/{id}/classification-summary
│   ├── tables.py                    # GET /documents/{id}/tables, fixture-schedules
│   ├── fixtures.py                  # GET /documents/{id}/fixtures
│   ├── takeoff.py                   # Takeoff pipeline (overlays, schedule, split-pdf, matrix)
│   └── viewer.py                    # Document viewer frontend
├── services/
│   ├── document_service.py          # DB operations for documents & pages
│   ├── storage_service.py           # Filesystem operations (save, locate, cleanup)
│   └── zip_service.py               # ZIP extraction with path-traversal protection
├── processing/
│   ├── document_processor.py        # Orchestrator — 8-step pipeline
│   ├── page_processor.py            # Single-page orchestrator (text + image + classify)
│   ├── text_extractor.py            # Native text + VLM fallback with quality analysis
│   ├── image_converter.py           # 300-DPI PNG rendering via PyMuPDF
│   ├── page_classifier.py           # 4-priority page classifier + VLM re-verification
│   ├── docling_extractor.py         # Docling-based full extraction (text, tables, images)
│   ├── full_extractor.py            # Full text + table extraction from all pages
│   ├── table_extractor.py           # Docling table extraction engine
│   ├── schedule_parser.py           # Lighting schedule identification and CSV export
│   ├── plan_splitter.py             # Lighting plan page extraction into split PDF
│   ├── takeoff_generator.py         # Fixture search, counting, bounding box overlay
│   └── vlm_classifier.py            # Gemini VLM classification + table verification
└── static/
    └── index.html                   # Frontend UI
storage/                             # Uploaded files, pages, pipeline outputs (gitignored)
requirements.txt                     # Python dependencies
.env                                 # Environment configuration
regression_test.sh                   # Automated regression test script
setup_ec2.sh                         # EC2 deployment helper
start_server.sh                      # Server startup script
```

---

## Setup & Installation

### Prerequisites
- Python 3.10+
- PostgreSQL 14+

### 1. Install dependencies

```bash
cd project
pip install -r requirements.txt
```

### 2. Configure environment

Create a `.env` file in the project root:

```env
DATABASE_URL=postgresql://docuser:docpass123@localhost:5432/docupload
SECRET_KEY=your-secure-random-key-here
ACCESS_TOKEN_EXPIRE_MINUTES=1440
STORAGE_PATH=storage
MAX_FILE_SIZE_MB=100
MAX_WORKERS=4
BASE_OCR_THRESHOLD=30
MIN_TEXT_DENSITY=2.0
MAX_GARBLED_RATIO=0.3
```

### 3. Set up the database

```bash
sudo -u postgres psql -c "CREATE USER docuser WITH PASSWORD 'docpass123';"
sudo -u postgres psql -c "CREATE DATABASE docupload OWNER docuser;"
```

Tables are auto-created on first startup via SQLAlchemy `Base.metadata.create_all()`.

### 4. Start the server

```bash
cd app.py
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
```

The API is now available at `http://localhost:8000`.  
Interactive docs: `http://localhost:8000/docs`

---

## Configuration

All settings are loaded from `.env` via `config.py`:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://...` | PostgreSQL connection string |
| `SECRET_KEY` | *(insecure default)* | JWT signing key — **must change in production** |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | `1440` | Token lifetime (24 hours) |
| `STORAGE_PATH` | `storage` | Root directory for uploaded files |
| `MAX_FILE_SIZE_MB` | `100` | Maximum upload size in megabytes |
| `MAX_WORKERS` | `4` | Thread pool size for parallel page processing |
| `BASE_OCR_THRESHOLD` | `30` | Minimum characters before VLM fallback triggers |
| `MIN_TEXT_DENSITY` | `2.0` | Expected characters per square inch for digital docs |
| `MAX_GARBLED_RATIO` | `0.3` | Max ratio of garbled characters before VLM is used |
| `VLM_VERIFY` | `true` | Enable/disable VLM re-verification layer |
| `GOOGLE_API_KEY` | — | Google AI API key for Gemini 2.5 Flash |
| `ANTHROPIC_API_KEY` | — | Anthropic API key for Claude table extraction (Layer 7 fallback) |

---

## API Endpoints

### Authentication

| Method | Path | Description | Auth |
|--------|------|-------------|------|
| `POST` | `/auth/register` | Register new user | No |
| `POST` | `/auth/login` | Login, receive JWT | No |

**Register:**
```bash
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name": "John Doe", "email": "john@example.com", "password": "password123"}'
```

**Login:**
```bash
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "john@example.com", "password": "password123"}'
```

Returns: `{ "access_token": "eyJ...", "token_type": "bearer", "user": {...} }`

### Documents

All document endpoints require `Authorization: Bearer <token>`.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/documents/upload` | Upload a single PDF or ZIP |
| `POST` | `/documents/upload-multiple` | Upload multiple files in one request |
| `GET` | `/documents/` | List user's documents (paginated) |
| `GET` | `/documents/{document_id}` | Get single document metadata |
| `GET` | `/documents/{document_id}/pages` | Get parsed pages (filter with `?relevance=relevant\|irrelevant`) |
| `GET` | `/documents/{document_id}/classification-summary` | Classification status, type breakdown, page lists |
| `POST` | `/documents/{document_id}/reclassify` | Re-run classifier on existing page data |
| `POST` | `/documents/{document_id}/reprocess` | Re-run full text extraction + image rendering |
| `GET` | `/documents/{document_id}/tables` | Extract tables from relevant pages (PyMuPDF) |
| `DELETE` | `/documents/{document_id}` | Delete document and all associated data |

**Upload:**
```bash
curl -X POST http://localhost:8000/documents/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@document.pdf"
```

**Batch Upload:**
```bash
curl -X POST http://localhost:8000/documents/upload-multiple \
  -H "Authorization: Bearer $TOKEN" \
  -F "files=@doc1.pdf" -F "files=@doc2.pdf"
```

**List Documents:**
```bash
curl http://localhost:8000/documents/?skip=0&limit=50 \
  -H "Authorization: Bearer $TOKEN"
```

**Get Pages (all):**
```bash
curl http://localhost:8000/documents/{document_id}/pages \
  -H "Authorization: Bearer $TOKEN"
```

**Get Relevant Pages Only:**
```bash
curl "http://localhost:8000/documents/{document_id}/pages?relevance=relevant" \
  -H "Authorization: Bearer $TOKEN"
```

Returns:
```json
{
  "document_id": "uuid",
  "total_pages": 8,
  "relevant_pages": 5,
  "irrelevant_pages": 3,
  "pages": [
    {
      "id": 1,
      "page_number": 1,
      "extracted_text": "Full page text...",
      "text_length": 1234,
      "ocr_used": false,
      "image_path": "/storage/1/uuid/pages/page_0001.png",
      "page_type": "COVER",
      "is_relevant": true,
      "confidence_source": "title_block",
      "sheet_code": "CS",
      "created_at": "2025-01-01T00:00:00"
    }
  ]
}
```

**Classification Summary:**
```bash
curl http://localhost:8000/documents/{document_id}/classification-summary \
  -H "Authorization: Bearer $TOKEN"
```

Returns:
```json
{
  "document_id": "uuid",
  "status": "completed",
  "classification_done": true,
  "total_pages": 8,
  "relevant_pages": 5,
  "irrelevant_pages": 3,
  "relevant_page_numbers": [1, 2, 4, 6, 8],
  "irrelevant_page_numbers": [3, 5, 7],
  "type_breakdown": {"COVER": 1, "LIGHTING_PLAN": 2, "SCHEDULE": 2, "DEMOLITION_PLAN": 3},
  "relevant_types": ["COVER", "LIGHTING_PLAN", "SCHEDULE"],
  "irrelevant_types": ["DEMOLITION_PLAN"]
}
```

**Reclassify (re-run classifier without re-processing):**
```bash
curl -X POST http://localhost:8000/documents/{document_id}/reclassify \
  -H "Authorization: Bearer $TOKEN"
```

**Extract Tables from Relevant Pages:**
```bash
curl http://localhost:8000/documents/{document_id}/tables \
  -H "Authorization: Bearer $TOKEN"
```

Returns:
```json
{
  "document_id": "uuid",
  "total_tables": 3,
  "pages": [
    {
      "page_number": 2,
      "page_type": "SCHEDULE",
      "table_count": 1,
      "tables": [
        [
          ["Room", "Fixture", "Qty", "Watts"],
          ["Office", "LED Panel", "4", "40"]
        ]
      ]
    }
  ]
}
```

### Utility

| Method | Path | Description | Auth |
|--------|------|-------------|------|
| `GET` | `/page-image?path=...` | Serve a 300-DPI page PNG | Yes |
| `GET` | `/health` | Health check with DB status | No |
| `GET` | `/` | Frontend UI | No |

---

## Document Processing Pipeline

When a PDF is uploaded, an 8-step background pipeline runs automatically:

```
Upload PDF
  │
  ▼
Status: "uploaded"
  │
  ▼
Background Task Starts → Status: "processing"
  │
  ├── Step 1: Open PDF with PyMuPDF, detect page count
  │
  ├── Step 2: Parallel page processing (ThreadPoolExecutor)
  │     ├── Render 300-DPI PNG image
  │     ├── Extract text (native → VLM fallback)
  │     └── Classify page type (4-priority + VLM re-verification)
  │
  ├── Step 3: Batch-insert all DocumentPage rows
  │
  ├── Step 4: Docling full extraction (GPU-accelerated)
  │     ├── Extract all page images
  │     ├── Extract full text
  │     └── Extract all tables as CSV
  │
  ├── Step 5: Identify lighting fixture schedules
  │     ├── Keyword-based table classification
  │     ├── VLM table verification (Gemini)
  │     └── Export as lighting_schedule.csv
  │
  ├── Step 6: Split lighting plan pages into standalone PDF
  │
  ├── Step 7: Fixture takeoff (autocount)
  │     ├── Search fixture codes on lighting plan pages
  │     ├── Count instances and record bounding boxes
  │     └── Generate annotated overlay PNGs
  │
  ├── Step 8: VLM schedule extraction fallback
  │     └── If no schedule found, try Gemini vision on schedule pages
  │
  ▼
Status: "completed"  (or "failed" on hard error)
```

### Page Classification Types

| Type | Relevant | Description |
|------|----------|-------------|
| `LIGHTING_PLAN` | ✅ | Electrical lighting layout drawings |
| `SCHEDULE` | ✅ | Fixture/equipment schedule tables |
| `SYMBOLS_LEGEND` | ✅ | Symbol legend and notes pages |
| `COVER` | ✅ | Cover sheet / title sheet |
| `DEMOLITION_PLAN` | ❌ | Demo / removal drawings |
| `POWER_PLAN` | ❌ | Power distribution drawings |
| `SITE_PLAN` | ❌ | Site / civil drawings |
| `FIRE_ALARM` | ❌ | Fire alarm system drawings |
| `RISER` | ❌ | Riser / one-line diagrams |
| `DETAIL` | ❌ | Detail / connection sheets |
| `OTHER` | ❌ | Unclassified or miscellaneous |

### Table Extraction (Docling)

`GET /documents/{id}/tables` returns structured table data extracted by the Docling engine from all relevant pages, available as JSON or CSV. `GET /documents/{id}/tables/fixture-schedules` filters to only fixture-schedule tables.

### Text Extraction Strategy

1. **Native extraction** — Read embedded text directly from the PDF page
2. **Quality analysis** — Check text density, garbled character ratio, page dimensions
3. **VLM fallback** — If native text is insufficient or garbled, send page image to Gemini VLM (primary) or Claude VLM (secondary fallback)
4. **Result** — `extracted_text` (full text), `text_length` (character count), `vlm_used` (boolean flag)

### File Storage Layout

```
storage/
└── {user_id}/
    └── {document_id}/
        ├── original/
        │   └── document.pdf
        ├── pages/
        │   ├── page_0001.png   (300 DPI)
        │   ├── page_0002.png
        │   └── ...
        └── pipeline/
            ├── images/                     # Docling page renders
            ├── text/
            │   └── full_text.txt            # Full extracted text
            ├── tables/                     # All extracted table CSVs
            │   ├── table_001.csv
            │   └── ...
            ├── relevant_tables/            # Identified schedule tables
            │   └── light_fixture_schedule.csv
            ├── fixture_results/            # Takeoff pipeline results
            │   ├── fixture_counts.json
            │   └── overlays/
            │       ├── output_overlay_page_1.png
            │       └── ...
            ├── lighting_schedule.csv
            ├── lighting_panel_plans.pdf
            └── tables_all.json
```

---

## Security

- **JWT Authentication** — All document endpoints require a valid Bearer token
- **Multi-User Isolation** — Queries always filter by `owner_id`; users cannot access other users' documents or page images
- **Path Traversal Protection** — Page image endpoint validates that the requested path resolves under the user's storage directory
- **Error Sanitization** — Internal error details (stack traces, DB errors) are logged server-side but never exposed in API responses
- **Content Deduplication** — SHA-256 hash prevents re-upload of identical content (per-user scope)
- **Input Validation** — Pydantic schemas enforce email format, password length (≥6), name length (≥2), file type (.pdf/.zip), and file size limits
- **Secret Key Warning** — Server emits a startup warning if the default insecure SECRET_KEY is still in use

---

## Database Schema

### `users`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | INTEGER | Primary key, auto-increment |
| `name` | VARCHAR | NOT NULL |
| `email` | VARCHAR | UNIQUE, NOT NULL |
| `hashed_password` | VARCHAR | NOT NULL |
| `role` | VARCHAR | NOT NULL, default `"user"` |
| `created_at` | DATETIME | UTC timestamp |

### `documents`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | INTEGER | Primary key, auto-increment |
| `document_id` | VARCHAR | UNIQUE, NOT NULL (UUID) |
| `filename` | VARCHAR | NOT NULL |
| `file_type` | VARCHAR | NOT NULL (`pdf` or `zip`) |
| `file_size` | INTEGER | NOT NULL (bytes) |
| `page_count` | INTEGER | Nullable (set on completion) |
| `content_hash` | VARCHAR(64) | SHA-256, indexed, partial unique with `owner_id` |
| `batch_id` | VARCHAR | Nullable (UUID, set for ZIP-extracted documents) |
| `owner_id` | INTEGER | FK → `users.id`, NOT NULL |
| `upload_time` | DATETIME | UTC timestamp |
| `status` | VARCHAR | `uploaded` / `processing` / `completed` / `failed` |

### `document_pages`
| Column | Type | Constraints |
|--------|------|-------------|
| `id` | INTEGER | Primary key, auto-increment |
| `document_id` | VARCHAR | FK → `documents.document_id`, NOT NULL |
| `page_number` | INTEGER | NOT NULL |
| `extracted_text` | VARCHAR | Nullable (full page text) |
| `text_length` | INTEGER | NOT NULL, default 0 |
| `ocr_used` | BOOLEAN | NOT NULL, default false |
| `image_path` | VARCHAR | Nullable (path to 300-DPI PNG) |
| `page_type` | VARCHAR | Nullable — `LIGHTING_PLAN`, `SCHEDULE`, etc. |
| `is_relevant` | BOOLEAN | Nullable — true for relevant page types |
| `confidence_source` | VARCHAR | Nullable — `sheet_index`, `title_block`, `sheet_code`, `full_text`, `vlm`, `default` |
| `sheet_code` | VARCHAR | Nullable — e.g. `E100`, `CS`, `LD201` |
| `vlm_classification` | VARCHAR | Nullable — VLM override classification |
| `vlm_confidence` | VARCHAR | Nullable — VLM confidence level |
| `vlm_reasoning` | TEXT | Nullable — VLM classification reasoning |
| `created_at` | DATETIME | UTC timestamp |

Unique constraint: `(document_id, page_number)` — prevents duplicate page rows.

---

## Error Handling

All endpoints return consistent JSON error responses:

```json
{
  "detail": "Human-readable error message"
}
```

| Status Code | Meaning |
|-------------|---------|
| `400` | Bad request (invalid input, empty file, duplicate email) |
| `401` | Unauthorized (missing/invalid/expired JWT) |
| `404` | Resource not found or access denied |
| `409` | Conflict (duplicate file upload) |
| `413` | File too large |
| `422` | Validation error (Pydantic schema rejection) |
| `500` | Internal server error (details logged, not exposed) |
