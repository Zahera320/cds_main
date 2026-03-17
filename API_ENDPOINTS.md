# API Endpoints Reference

> **Base URL:** `http://localhost:8000`

---

## 0. Document Viewer (Frontend UI)

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| `GET`  | `/viewer` | No   | Serve the single-page HTML Document Viewer web app |
| `GET`  | `/viewer/{document_id}` | No   | Serve the viewer pre-loaded with a specific document |
| `GET`  | `/`      | No   | Redirect to /viewer |

### GET `/viewer/{document_id}`

Serves the same single-page HTML application as `/viewer`. The frontend JavaScript reads the `document_id` from the URL path and automatically opens that document once the user is authenticated.

**Path Param:** `document_id` — UUID of the document to pre-load.

**File:** `app.py/routes/viewer.py`

---

## 1. Health Check

| Method | Endpoint  | Auth | Description |
|--------|-----------|------|-------------|
| `GET`  | `/health` | No   | Service health status + database connectivity check |

**Response:**
```json
{
  "status": "healthy | degraded",
  "database": "ok | error: <message>",
  "vlm": "enabled | disabled",
  "timestamp": "2026-03-05T12:00:00+00:00"
}
```

**File:** `app.py/main.py`

---

## 2. Authentication Service

| Method | Endpoint          | Auth | Description |
|--------|-------------------|------|-------------|
| `POST` | `/auth/register`  | No   | Register a new user |
| `POST` | `/auth/login`     | No   | Login and get JWT access token |

### POST `/auth/register`

**Request Body:**
```json
{
  "name": "John Doe",
  "email": "john@example.com",
  "password": "securePass123"
}
```

**Response (201):** `UserResponse`
```json
{
  "id": 1,
  "name": "John Doe",
  "email": "john@example.com",
  "created_at": "2026-03-05T12:00:00"
}
```

### POST `/auth/login`

**Request Body:**
```json
{
  "email": "john@example.com",
  "password": "securePass123"
}
```

**Response (200):** `Token`
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "token_type": "bearer",
  "user": {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com",
    "created_at": "2026-03-05T12:00:00"
  }
}
```

**File:** `app.py/routes/user.py`

---

## 3. Document Upload Service

| Method | Endpoint                      | Auth | Description |
|--------|-------------------------------|------|-------------|
| `POST` | `/documents/upload`           | JWT  | Upload a single PDF or ZIP file |
| `POST` | `/documents/upload-multiple`  | JWT  | Upload multiple PDF/ZIP files in one request |

### POST `/documents/upload`

**Request:** `multipart/form-data` — field `file` (PDF or ZIP, max 100 MB)

**Response (200):** `UploadResponse`
```json
{
  "document_id": "uuid-here",
  "filename": "plans.pdf",
  "file_type": "pdf",
  "file_size": 1234567,
  "status": "processing",
  "extracted_pdf_count": 0,
  "duplicate": false,
  "message": "File uploaded successfully. Status: processing"
}
```

**Error Codes:**
- `400` — No file / wrong extension / empty file / file too large / ZIP with no PDFs
- `409` — Duplicate file (same SHA-256 hash for this user)
- `500` — Save failure

### POST `/documents/upload-multiple`

**Request:** `multipart/form-data` — field `files` (multiple PDF/ZIP files)

**Response (200):** `BatchUploadResponse`
```json
{
  "total": 3,
  "succeeded": 2,
  "failed": 1,
  "results": [ /* array of UploadResponse */ ],
  "errors": [ { "filename": "bad.txt", "error": "Only PDF and ZIP files are allowed" } ]
}
```

**File:** `app.py/routes/documents.py`

---

## 4. Document Management Service

| Method   | Endpoint                     | Auth | Description |
|----------|------------------------------|------|-------------|
| `GET`    | `/documents/`                | JWT  | List user's documents (paginated) |
| `GET`    | `/documents/{document_id}`   | JWT  | Get a single document by UUID |
| `DELETE` | `/documents/{document_id}`   | JWT  | Delete a document and all associated data |

### GET `/documents/`

**Query Params:**
| Param  | Type | Default | Description |
|--------|------|---------|-------------|
| `skip` | int  | 0       | Records to skip |
| `limit`| int  | 50      | Max records (1–200) |

**Response (200):** `List[DocumentResponse]`
```json
[
  {
    "id": 1,
    "document_id": "uuid-here",
    "filename": "plans.pdf",
    "file_type": "pdf",
    "file_size": 1234567,
    "upload_time": "2026-03-05T12:00:00",
    "status": "completed",
    "content_hash": "sha256hex..."
  }
]
```

### GET `/documents/{document_id}`

**Response (200):** `DocumentResponse` (same schema as above, single object)

### DELETE `/documents/{document_id}`

**Response (200):** `DeleteResponse`
```json
{
  "document_id": "uuid-here",
  "message": "Document deleted successfully"
}
```

**File:** `app.py/routes/documents.py`

---

## 5. Document Processing Service

| Method | Endpoint                              | Auth | Description |
|--------|---------------------------------------|------|-------------|
| `POST` | `/documents/{document_id}/reprocess`  | JWT  | Re-trigger page processing (for stuck/failed docs) |
| `POST` | `/documents/{document_id}/reclassify` | JWT  | Re-run page classification only (no re-extraction) |
| `GET`  | `/documents/{document_id}/logs`       | JWT  | Retrieve the per-document processing log |

### POST `/documents/{document_id}/reprocess`

**Response (200):**
```json
{
  "document_id": "uuid-here",
  "message": "Reprocessing started"
}
```

**Error Codes:**
- `400` — Not a PDF document
- `404` — Document not found
- `409` — Already processing

### POST `/documents/{document_id}/reclassify`

**Response (200):**
```json
{
  "document_id": "uuid-here",
  "message": "Reclassification started"
}
```

**Error Codes:**
- `400` — Not a PDF document
- `404` — Document not found
- `409` — Still processing / not yet processed

### GET `/documents/{document_id}/logs`

Returns the per-document `processing.log` as plain text. Captures every pipeline step: text extraction, page classification, VLM verification, autocount, and any errors.

**Response:** `text/plain` — full processing log contents

**Error Codes:**
- `404` — Document not found or no log file exists yet

**File:** `app.py/routes/processing.py` (reprocess, logs), `app.py/routes/classification.py` (reclassify)

---

## 6. Page Viewer Service

| Method | Endpoint                                          | Auth | Description |
|--------|---------------------------------------------------|------|-------------|
| `GET`  | `/documents/{document_id}/pages`                  | JWT  | Get parsed pages (paginated, filterable) |
| `GET`  | `/page-image`                                     | JWT  | Serve a 300-DPI page PNG image |
| `GET`  | `/documents/{document_id}/classification-summary` | JWT  | Page classification breakdown |

### GET `/documents/{document_id}/pages`

**Query Params:**
| Param      | Type   | Default | Description |
|------------|--------|---------|-------------|
| `skip`     | int    | 0       | Pages to skip |
| `limit`    | int    | 50      | Max pages (1–500) |
| `relevance`| string | null    | `"relevant"`, `"irrelevant"`, or omit for all |

**Response (200):** `DocumentPagesListResponse`
```json
{
  "document_id": "uuid-here",
  "total_pages": 24,
  "relevant_pages": 8,
  "irrelevant_pages": 16,
  "pages": [
    {
      "id": 1,
      "document_id": "uuid-here",
      "page_number": 1,
      "extracted_text": "LIGHTING PLAN ...",
      "text_length": 1234,
      "vlm_used": false,
      "image_path": "/storage/10/uuid/pages/page_001.png",
      "page_type": "LIGHTING_PLAN",
      "is_relevant": true,
      "confidence_source": "sheet_code",
      "sheet_code": "E101",
      "created_at": "2026-03-05T12:00:00"
    }
  ]
}
```

### GET `/documents/{document_id}/classification-summary`

**Response (200):** `ClassificationSummaryResponse`
```json
{
  "document_id": "uuid-here",
  "status": "completed",
  "classification_done": true,
  "total_pages": 24,
  "relevant_pages": 8,
  "irrelevant_pages": 16,
  "relevant_page_numbers": [2, 5, 6, 8, 10, 12, 15, 20],
  "irrelevant_page_numbers": [1, 3, 4, 7, 9, 11, 13, 14, 16, 17, 18, 19, 21, 22, 23, 24],
  "type_breakdown": {
    "LIGHTING_PLAN": 5,
    "SCHEDULE": 2,
    "COVER": 1,
    "OTHER": 16
  },
  "relevant_types": ["COVER", "LIGHTING_PLAN", "SCHEDULE"],
  "irrelevant_types": ["OTHER"]
}
```

### GET `/page-image`

**Query Params:**
| Param   | Type   | Required | Description |
|---------|--------|----------|-------------|
| `path`  | string | Yes      | Absolute path to the page PNG |
| `token` | string | No       | JWT token (for `<img src>` requests) |

**Response:** `image/png` binary stream

**Security:** Path must resolve inside `storage/{user_id}/` — cross-user access is blocked.

**File:** `app.py/routes/pages.py` (pages, page-image), `app.py/routes/classification.py` (classification-summary)

---

## 7. Table Extraction Service (Table Parser)

| Method | Endpoint                                              | Auth | Description |
|--------|-------------------------------------------------------|------|-------------|
| `GET`  | `/documents/{document_id}/tables`                     | JWT  | Extract tables from all relevant pages (JSON) |
| `GET`  | `/documents/{document_id}/tables/csv`                 | JWT  | Extract tables from all relevant pages (CSV download) |
| `GET`  | `/documents/{document_id}/tables/fixture-schedules`     | JWT  | Extract only fixture schedule tables (JSON) |
| `GET`  | `/documents/{document_id}/tables/fixture-schedules/csv` | JWT  | Extract only fixture schedule tables (CSV download) |

### GET `/documents/{document_id}/tables`

Extracts structured table data from relevant PDF pages using PyMuPDF `find_tables()`.

**Prerequisites:** Document `status` must be `"completed"` or `"failed"`.

**Response (200):**
```json
{
  "document_id": "uuid-here",
  "total_tables": 3,
  "pages": [
    {
      "page_number": 2,
      "page_type": "SCHEDULE",
      "table_count": 1,
      "tables": [
        [
          ["Room", "Fixture", "Qty"],
          ["Office", "LED Panel", "4"],
          ["Lobby", "Downlight", "12"]
        ]
      ]
    }
  ]
}
```

**Error Codes:**
- `400` — Document processing not yet completed
- `404` — Document not found / PDF file not found on disk
- `500` — Extraction failure

### GET `/documents/{document_id}/tables/csv`

Same extraction logic as the JSON endpoint above, but streams the result as a downloadable **CSV file**.

**Response:** `text/csv` — `Content-Disposition: attachment; filename="tables_{document_id}.csv"`

**CSV structure:**
- Each table is preceded by a section header row: `Page <N> — <page_type> — Table <M>`
- The first data row of each table is treated as column headers.
- An empty row separates consecutive tables.
- If no tables are found, a single row `No tables found on relevant pages` is returned.

**Query Params:**
| Param   | Type   | Required | Description |
|---------|--------|----------|-------------|
| `token` | string | No       | JWT token (for direct browser download links) |

**Error Codes:**
- `400` — Document processing not yet completed
- `404` — Document not found / PDF file not found on disk
- `500` — Extraction failure

**File:** `app.py/routes/tables.py`

### GET `/documents/{document_id}/tables/fixture-schedules`

Filters extracted tables to return **only** those classified as `LIGHT_FIXTURE_SCHEDULE`. Each result includes structured fixture inventory records.

**Response (200):**
```json
{
  "document_id": "uuid",
  "total_tables": 1,
  "tables": [
    {
      "header_label": "LIGHT FIXTURE SCHEDULE",
      "page_number": 9,
      "page_type": "SCHEDULE",
      "rows": [["Code", "Description", "Voltage", "Mounting"], ["A1", "LED Troffer", "120/277", "Recessed"]],
      "fixtures": []
    }
  ],
  "engine": "docling"
}
```

### GET `/documents/{document_id}/tables/fixture-schedules/csv`

Same as above but returns a downloadable CSV file containing only fixture schedule tables.

**Query Params:**
| Param   | Type   | Required | Description |
|---------|--------|----------|-------------|
| `token` | string | No       | JWT token (for direct browser download links) |

**Response:** `text/csv` — `Content-Disposition: attachment; filename="fixture_schedules_{document_id}.csv"`

**Error Codes:**
- `400` — Document processing not yet completed
- `404` — Document not found / PDF file not found on disk / no fixture schedules found
- `500` — Extraction failure

**File:** `app.py/routes/tables.py`

---

## 9. Takeoff / Autocount Pipeline

| Method | Endpoint                                          | Auth | Description |
|--------|---------------------------------------------------|------|-------------|
| `GET`  | `/documents/{document_id}/takeoff`                | JWT  | Takeoff results — fixture counts, overlays, pipeline status |
| `GET`  | `/documents/{document_id}/takeoff/overlay`        | JWT  | Serve annotated overlay image (PNG) by sheet ID |
| `GET`  | `/documents/{document_id}/takeoff/matrix-csv`     | JWT  | Download fixture takeoff matrix CSV |
| `GET`  | `/documents/{document_id}/takeoff/schedule-csv`   | JWT  | Download extracted lighting schedule CSV |
| `GET`  | `/documents/{document_id}/takeoff/docling-outputs`| JWT  | Get Docling extraction metadata (JSON) |
| `GET`  | `/documents/{document_id}/takeoff/tables-json`    | JWT  | Download extracted tables JSON |
| `GET`  | `/documents/{document_id}/takeoff/split-pdf`      | JWT  | Download split PDF (lighting plan pages only) |
| `POST` | `/documents/{document_id}/takeoff/rerun`          | JWT  | Re-run the autocount pipeline |

The takeoff pipeline runs automatically after document processing completes. It performs 4 steps:

1. **Full Extraction** — Extracts all text and tables from every PDF page into a combined text file
2. **Schedule Isolation** — Identifies the lighting fixture schedule table and exports it as CSV
3. **Plan Splitting** — Extracts lighting plan pages into a separate PDF based on page classification
4. **Takeoff Generation** — Searches the split PDF for fixture letter codes (e.g. A1, B6, E3), draws red bounding boxes, and outputs annotated overlay images + a JSON fixture count tally

### GET `/documents/{document_id}/takeoff`

Returns the full takeoff results including fixture counts, overlay image paths, and pipeline status.

**Response (200):**
```json
{
  "document_id": "uuid",
  "batch_id": null,
  "fixture_counts": {
    "A1": 48,
    "A6": 14,
    "B1": 4,
    "B6": 26
  },
  "total_fixtures_found": 92,
  "available_sheets": ["1", "2", "3"],
  "matrix_csv_available": false,
  "schedule_csv_available": true,
  "split_pdf_available": true,
  "pipeline_status": "completed",
  "source": "document"
}
```

**Pipeline status values:**
- `completed` — All steps succeeded, fixture counts available
- `partial` — Some outputs exist (CSV/PDF) but no fixture counts
- `failed` — Pipeline ran but produced no usable outputs
- `not_run` — Pipeline has not been executed yet

### GET `/documents/{document_id}/takeoff/overlay`

Serves an annotated overlay PNG image with red bounding boxes drawn around detected fixtures.

**Query Params:**
| Param      | Type   | Required | Description |
|------------|--------|----------|-------------|
| `sheet_id` | string | Yes      | Sheet ID identifier (e.g., `E000`, `E101`, `1`, `2`) |
| `token`    | string | No       | JWT token (for `<img src>` requests) |

**Response:** `image/png` binary stream

**Error Codes:**
- `404` — Document or overlay image not found

### GET `/documents/{document_id}/takeoff/matrix-csv`

Downloads the computed fixture takeoff matrix as a CSV file. Contains per-sheet fixture counts.

**Query Params:**
| Param   | Type   | Required | Description |
|---------|--------|----------|-------------|
| `token` | string | No       | JWT token (for direct download links) |

**Response:** `text/csv` — `Content-Disposition: attachment; filename="fixture_takeoff_matrix_{document_id}.csv"`

**Error Codes:**
- `404` — Document or matrix CSV not found

### GET `/documents/{document_id}/takeoff/schedule-csv`

Downloads the extracted lighting fixture schedule as a CSV file.

**Query Params:**
| Param   | Type   | Required | Description |
|---------|--------|----------|-------------|
| `token` | string | No       | JWT token (for direct download links) |

**Response:** `text/csv` — `Content-Disposition: attachment; filename="lighting_schedule_{document_id}.csv"`

**Error Codes:**
- `404` — Document or schedule CSV not found

### GET `/documents/{document_id}/takeoff/split-pdf`

Downloads the split PDF containing only the lighting plan pages extracted from the original document.

**Query Params:**
| Param   | Type   | Required | Description |
|---------|--------|----------|-------------|
| `token` | string | No       | JWT token (for direct download links) |

**Response:** `application/pdf` — `Content-Disposition: attachment; filename="lighting_panel_plans_{document_id}.pdf"`

**Error Codes:**
- `404` — Document or split PDF not found

### GET `/documents/{document_id}/takeoff/docling-outputs`

Returns paths and status of all Docling extraction outputs for a document.

**Response (200):**
```json
{
  "document_id": "uuid",
  "images_dir": "/storage/.../pipeline/images",
  "image_count": 15,
  "text_path": "/storage/.../pipeline/text/full_text.txt",
  "tables_dir": "/storage/.../pipeline/tables",
  "table_count": 22,
  "tables_json_url": "/documents/{document_id}/takeoff/tables-json",
  "schedule_csv_url": "/documents/{document_id}/takeoff/schedule-csv",
  "schedule_found": true,
  "page_count": 15,
  "engine": "docling"
}
```

### GET `/documents/{document_id}/takeoff/tables-json`

Downloads the Docling-extracted tables as a JSON file.

**Query Params:**
| Param   | Type   | Required | Description |
|---------|--------|----------|-------------|
| `token` | string | No       | JWT token (for direct download links) |

**Response:** `application/json` — `Content-Disposition: attachment; filename="tables_{document_id}.json"`

**Error Codes:**
- `404` — Document or tables JSON not found

### POST `/documents/{document_id}/takeoff/rerun`

Re-runs the autocount pipeline on a previously processed document. Uses existing page classifications from the database. Runs as a background task. If the document belongs to a batch, the batch pipeline is also re-run.

**Response (200):**
```json
{
  "document_id": "uuid",
  "batch_id": "uuid-or-null",
  "message": "Takeoff pipeline rerun started"
}
```

**Error Codes:**
- `400` — Document not yet processed / ZIP documents must use batch rerun
- `404` — Document not found

**File:** `app.py/routes/takeoff.py`

---

## 8. Fixture Extraction Service

| Method | Endpoint                                      | Auth | Description |
|--------|-----------------------------------------------|------|-------------|
| `GET`  | `/documents/{document_id}/fixtures`           | JWT  | Extract luminaire fixture data from schedule pages |

### GET `/documents/{document_id}/fixtures`

Extracts structured luminaire fixture data from PDF schedule pages using Docling table extraction with an 8-layer pipeline:
1. **Table Detection** — Docling extracts tables from schedule pages
2. **Table Filtering** — Keyword-based classification (positive: "luminaire schedule", negative: "panel schedule")
3. **Header Detection** — Score rows against column aliases (code, description, voltage, lumens, etc.)
4. **Row Parsing** — Validate fixture codes and extract embedded data
5. **Post-Parse Rejection** — Reject panel schedules (>60% purely numeric codes)
6. **VLM Fallback** — Claude vision extraction when Docling finds no schedule
7. **Combo Page Handling** — Detect schedules embedded in lighting plan pages
8. **Deduplication** — Merge results from all layers

**Response (200):** `FixtureExtractionResponse`
```json
{
  "document_id": "uuid",
  "total_fixtures": 41,
  "schedule_pages_scanned": 3,
  "plan_pages_scanned": 2,
  "vlm_used": false,
  "schedule_sheet_codes": ["E6.00", "E7.00", "E7.01"],
  "fixtures": [
    {
      "code": "A1",
      "description": "2'x2' LED TROFFER. 2400 LUMEN PACKAGE.",
      "mounting": "",
      "fixture_style": "22CZ2-24-HRP-UNV-B2750-W2A-1-U",
      "voltage": "",
      "lumens": "",
      "cct": "",
      "dimming": "",
      "max_va": ""
    }
  ]
}
```

**Error Codes:**
- `400` — Document processing not yet completed
- `404` — Document not found / PDF file not found on disk
- `500` — Extraction failure

**File:** `app.py/routes/fixtures.py`

---

## 10. Batch Operations

Multi-document batch endpoints — used when uploading a ZIP (which creates multiple documents sharing a `batch_id`) or when using `upload-multiple`.

### 10a. Batch Document Listing

| Method | Endpoint                        | Auth | Description |
|--------|---------------------------------|------|-------------|
| `GET`  | `/batch/{batch_id}/documents`   | JWT  | List all documents in a batch (owner-scoped) |

#### GET `/batch/{batch_id}/documents`

**Response (200):** `BatchDocumentsResponse`
```json
{
  "batch_id": "uuid",
  "total": 3,
  "all_completed": true,
  "documents": [ /* array of DocumentResponse */ ]
}
```

**Error Codes:**
- `404` — Batch not found or empty

**File:** `app.py/routes/documents.py`

### 10b. Batch Takeoff Pipeline

| Method | Endpoint                                    | Auth | Description |
|--------|---------------------------------------------|------|-------------|
| `GET`  | `/batch/{batch_id}/takeoff`                 | JWT  | Batch-level takeoff results (JSON) |
| `GET`  | `/batch/{batch_id}/takeoff/overlay`         | JWT  | Serve batch overlay image (PNG) |
| `GET`  | `/batch/{batch_id}/takeoff/schedule-csv`    | JWT  | Download batch schedule CSV |
| `GET`  | `/batch/{batch_id}/takeoff/split-pdf`       | JWT  | Download batch split PDF |
| `POST` | `/batch/{batch_id}/takeoff/rerun`           | JWT  | Re-run batch takeoff pipeline |

#### GET `/batch/{batch_id}/takeoff`

Returns cross-document takeoff results for an entire batch — aggregated fixture counts, overlay images, and pipeline status.

**Response (200):** `BatchTakeoffResponse`
```json
{
  "batch_id": "uuid",
  "document_ids": ["uuid-1", "uuid-2"],
  "fixture_counts": { "A1": 48, "B6": 26 },
  "total_fixtures_found": 74,
  "available_sheets": ["1", "2"],
  "matrix_csv_available": false,
  "schedule_csv_available": true,
  "split_pdf_available": true,
  "pipeline_status": "completed",
  "source": "batch"
}
```

**Pipeline status values:** `completed`, `running`, `partial`, `failed`, `not_run`

#### GET `/batch/{batch_id}/takeoff/overlay`

Serves a batch-level takeoff overlay image.

**Query Params:**
| Param      | Type   | Required | Description |
|------------|--------|----------|-------------|
| `sheet_id` | string | Yes      | Sheet ID identifier (e.g., `E000`, `1`, `2`) |
| `token`    | string | No       | JWT token (for `<img src>` requests) |

**Response:** `image/png` binary stream

#### GET `/batch/{batch_id}/takeoff/schedule-csv`

Downloads the batch-level extracted lighting schedule CSV.

**Query Params:**
| Param   | Type   | Required | Description |
|---------|--------|----------|-------------|
| `token` | string | No       | JWT token (for direct download) |

**Response:** `text/csv` — `Content-Disposition: attachment; filename="lighting_schedule_batch_{batch_id}.csv"`

#### GET `/batch/{batch_id}/takeoff/split-pdf`

Downloads the batch-level merged split PDF (lighting plan pages from all documents).

**Query Params:**
| Param   | Type   | Required | Description |
|---------|--------|----------|-------------|
| `token` | string | No       | JWT token (for direct download) |

**Response:** `application/pdf` — `Content-Disposition: attachment; filename="lighting_panel_plans_batch_{batch_id}.pdf"`

#### GET `/batch/{batch_id}/takeoff/matrix-csv`

Downloads the batch-level fixture takeoff matrix CSV.

**Query Params:**
| Param   | Type   | Required | Description |
|---------|--------|----------|-------------|
| `token` | string | No       | JWT token (for direct download) |

**Response:** `text/csv` — `Content-Disposition: attachment; filename="fixture_takeoff_matrix_batch_{batch_id}.csv"`

#### POST `/batch/{batch_id}/takeoff/rerun`

Re-runs the cross-document batch takeoff pipeline. All documents in the batch must be in `completed` or `failed` status.

**Response (200):**
```json
{
  "batch_id": "uuid",
  "message": "Batch takeoff pipeline rerun started"
}
```

**Error Codes:**
- `400` — No PDF documents in batch / documents still processing
- `404` — Batch not found

**File:** `app.py/routes/takeoff.py` (batch endpoints), `app.py/routes/documents.py` (batch listing)

---

## Quick Reference — All Endpoints

| #  | Method   | Endpoint                                              | Auth | Service |
|----|----------|-------------------------------------------------------|------|---------|
| 1  | `GET`    | `/viewer`                                             | No   | Document Viewer |
| 2  | `GET`    | `/viewer/{document_id}`                               | No   | Document Viewer (specific doc) |
| 3  | `GET`    | `/`                                                   | No   | Document Viewer (redirect) |
| 4  | `GET`    | `/health`                                             | No   | Health Check |
| 5  | `POST`   | `/auth/register`                                      | No   | Authentication |
| 6  | `POST`   | `/auth/login`                                         | No   | Authentication |
| 7  | `POST`   | `/documents/upload`                                   | JWT  | Document Upload |
| 8  | `POST`   | `/documents/upload-multiple`                          | JWT  | Document Upload |
| 9  | `GET`    | `/documents/`                                         | JWT  | Document Management |
| 10 | `GET`    | `/documents/{document_id}`                            | JWT  | Document Management |
| 11 | `DELETE` | `/documents/{document_id}`                            | JWT  | Document Management |
| 12 | `POST`   | `/documents/{document_id}/reprocess`                  | JWT  | Document Processing |
| 13 | `POST`   | `/documents/{document_id}/reclassify`                 | JWT  | Document Processing |
| 14 | `GET`    | `/documents/{document_id}/logs`                       | JWT  | Processing Logs |
| 15 | `GET`    | `/documents/{document_id}/pages`                      | JWT  | Page Viewer |
| 16 | `GET`    | `/page-image`                                         | JWT  | Page Viewer |
| 17 | `GET`    | `/documents/{document_id}/classification-summary`     | JWT  | Page Classification |
| 18 | `GET`    | `/documents/{document_id}/tables`                     | JWT  | Table Extraction (JSON) |
| 19 | `GET`    | `/documents/{document_id}/tables/csv`                 | JWT  | Table Extraction (CSV) |
| 20 | `GET`    | `/documents/{document_id}/tables/fixture-schedules`     | JWT  | Fixture Schedules (JSON) |
| 21 | `GET`    | `/documents/{document_id}/tables/fixture-schedules/csv` | JWT  | Fixture Schedules (CSV) |
| 22 | `GET`    | `/documents/{document_id}/fixtures`                   | JWT  | Fixture Extraction |
| 23 | `GET`    | `/documents/{document_id}/takeoff`                    | JWT  | Takeoff Results |
| 24 | `GET`    | `/documents/{document_id}/takeoff/overlay`            | JWT  | Takeoff Overlay Image |
| 25 | `GET`    | `/documents/{document_id}/takeoff/matrix-csv`         | JWT  | Takeoff Matrix CSV |
| 26 | `GET`    | `/documents/{document_id}/takeoff/schedule-csv`       | JWT  | Takeoff Schedule CSV |
| 27 | `GET`    | `/documents/{document_id}/takeoff/docling-outputs`    | JWT  | Docling Extraction Metadata |
| 28 | `GET`    | `/documents/{document_id}/takeoff/tables-json`        | JWT  | Extracted Tables JSON |
| 29 | `GET`    | `/documents/{document_id}/takeoff/split-pdf`          | JWT  | Takeoff Split PDF |
| 30 | `POST`   | `/documents/{document_id}/takeoff/rerun`              | JWT  | Re-run Takeoff Pipeline |
| 31 | `GET`    | `/batch/{batch_id}/documents`                         | JWT  | Batch Document Listing |
| 32 | `GET`    | `/batch/{batch_id}/takeoff`                           | JWT  | Batch Takeoff Results |
| 33 | `GET`    | `/batch/{batch_id}/takeoff/overlay`                   | JWT  | Batch Overlay Image |
| 34 | `GET`    | `/batch/{batch_id}/takeoff/matrix-csv`                | JWT  | Batch Matrix CSV |
| 35 | `GET`    | `/batch/{batch_id}/takeoff/schedule-csv`              | JWT  | Batch Schedule CSV |
| 36 | `GET`    | `/batch/{batch_id}/takeoff/split-pdf`                 | JWT  | Batch Split PDF |
| 37 | `POST`   | `/batch/{batch_id}/takeoff/rerun`                     | JWT  | Re-run Batch Takeoff |
| 38 | `GET`    | `/documents/{document_id}/keynotes`                   | JWT  | Extract Key Notes |

---

### Key Notes

**`GET /documents/{document_id}/keynotes`**

Extracts KEY NOTES, GENERAL NOTES, ELECTRICAL NOTES, and LIGHTING NOTES
from the extracted text of document pages.

**Query Parameters:**
| Parameter   | Default    | Description |
|-------------|------------|-------------|
| `relevance` | `relevant` | `relevant` = only relevant pages, `all` = all pages |

**Response:**
```json
{
  "document_id": "uuid",
  "total_notes": 12,
  "pages": [
    {
      "page_number": 4,
      "sheet_code": "E1.01",
      "page_type": "LIGHTING_PLAN",
      "notes": [
        {"number": "1", "text": "...", "section": "KEY NOTES"},
        {"number": "A", "text": "...", "section": "GENERAL NOTES"}
      ]
    }
  ]
}
```

---

## Authentication

All `JWT`-protected endpoints require header:
```
Authorization: Bearer <access_token>
```

The `/page-image` endpoint also accepts `?token=<access_token>` as a query parameter (for `<img src>` usage).

---

## Source Files

| File | Purpose |
|------|---------|
| `app.py/main.py` | FastAPI app bootstrap, CORS, health check, router registration |
| `app.py/routes/viewer.py` | Document Viewer frontend (serves `index.html`) |
| `app.py/routes/user.py` | Authentication endpoints (register, login) |
| `app.py/routes/documents.py` | All document CRUD, upload endpoints |
| `app.py/routes/processing.py` | Document reprocess endpoint |
| `app.py/routes/pages.py` | Page listing and page-image endpoints |
| `app.py/routes/classification.py` | Classification summary and reclassify endpoints |
| `app.py/routes/tables.py` | Table extraction and fixture schedule endpoints |
| `app.py/routes/fixtures.py` | Luminaire fixture extraction endpoint |
| `app.py/routes/takeoff.py` | Takeoff pipeline results, overlays, schedule CSV, split PDF, rerun |
| `app.py/processing/document_processor.py` | Main processing orchestrator (8-step pipeline) |
| `app.py/processing/full_extractor.py` | Full text + table extraction from all PDF pages |
| `app.py/processing/schedule_parser.py` | Lighting schedule identification and CSV export |
| `app.py/processing/plan_splitter.py` | Lighting plan page extraction into split PDF |
| `app.py/processing/takeoff_generator.py` | Fixture search, counting, and bounding box overlay |
| `app.py/processing/page_classifier.py` | Page classification (sheet index, text patterns, VLM) |
| `app.py/processing/text_extractor.py` | Per-page text extraction |
| `app.py/processing/page_processor.py` | Per-page processing (images, text, classification) |
| `app.py/processing/table_extractor.py` | Table extraction with Docling engine |
| `app.py/processing/image_converter.py` | PDF page to PNG image conversion |
| `app.py/processing/vlm_classifier.py` | VLM (Gemini) page classification verification, table verification, and Gemini table extraction fallback |
| `app.py/processing/docling_extractor.py` | Docling-based full extraction (text, tables, images) with VLM fallback |
| `app.py/services/document_service.py` | Database operations for documents & pages |
| `app.py/services/storage_service.py` | File-system storage helpers |
| `app.py/services/zip_service.py` | ZIP extraction with path-traversal protection |
| `app.py/schemas.py` | Pydantic request/response models |
| `app.py/models.py` | SQLAlchemy ORM models (User, Document, DocumentPage) |
| `app.py/auth.py` | JWT token creation/verification, bcrypt password hashing |
| `app.py/config.py` | Centralized configuration from `.env` |
| `app.py/utils.py` | Dependency helpers (`get_db`, `get_current_user`) |

---

## VLM Re-Verification Layer

The pipeline includes an optional VLM (Vision Language Model) re-verification layer powered by Google Gemini 2.5 Flash. When `VLM_VERIFY=true` and `GOOGLE_API_KEY` is set, the following additional checks run:

### Page Classification Override
After rule-based classification, each page image is sent to Gemini for re-verification. When the VLM disagrees with **high confidence**, it **overrides** the rule-based classification. The VLM is the final authority on page type.

### Table Verification (Step 8b-vlm)
When Docling identifies a Light Fixture Schedule, the VLM spot-checks SCHEDULE-classified pages to confirm the table is indeed a fixture schedule. If VLM says it's not a fixture schedule (with high confidence), the Docling CSV is discarded.

### Gemini Table Extraction Fallback
When Docling fails to extract a schedule (or finds no tables that classify as a fixture schedule):
1. VLM extraction is attempted on pages with tables (in `docling_extractor.py`)
2. VLM extraction is also attempted on SCHEDULE-classified pages (in `document_processor.py`)

This uses Gemini's vision to read table data directly from page images with `max_output_tokens=8192`.

### Scanned Page Detection
Pages with `text_length < 50` or `ocr_used=True` are flagged as scanned pages. This information is logged and available for downstream processing decisions.

### Output Folder Structure
```
pipeline/
├── images/                     # Docling page renders
├── text/                       # Full extracted text
│   └── full_text.txt
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
├── lighting_schedule.csv       # Original schedule (backward compat)
├── lighting_panel_plans.pdf    # Split lighting plan pages
├── fixture_counts.json         # Original counts (backward compat)
└── output_overlay_page_*.png   # Original overlays (backward compat)
```

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `VLM_VERIFY` | `true` | Enable/disable VLM re-verification layer |
| `GOOGLE_API_KEY` | — | Google AI API key for Gemini 2.5 Flash |
| `ANTHROPIC_API_KEY` | — | Anthropic API key for Claude table extraction (Layer 7 fallback) |
