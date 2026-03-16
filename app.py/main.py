"""
FastAPI Application Entry Point
===================================
Bootstraps the app: CORS, routers, static files, health check.
No business logic lives here.
"""

import logging
import os
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from sqlalchemy import text

from database import engine, Base, SessionLocal
from config import CORS_ORIGINS, SECRET_KEY
from logging_config import setup_logging
from routes import user, documents
from routes.pages import router as pages_router, page_image_router
from routes.documents import batch_router
from routes import classification, processing as processing_routes, tables, fixtures, viewer, takeoff
from routes.takeoff import batch_takeoff_router
from routes import pdf_render
from routes import img_render
from processing.vlm_classifier import is_vlm_available

logger = logging.getLogger(__name__)

# ── Initialize file-based logging ─────────────────────────────────────────────
setup_logging()

# ── Guard: refuse to start with the default insecure secret key ───────────────
_DEFAULT_SECRET = "supersecret-change-me-in-production"
if SECRET_KEY == _DEFAULT_SECRET:
    import warnings
    warnings.warn(
        "\n\n"
        "  [SECURITY] SECRET_KEY is set to the default insecure value.\n"
        "  Any JWT token can be forged.  Set SECRET_KEY in your .env file\n"
        "  before deploying to production.\n",
        stacklevel=1,
    )

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="DocuParse — Document Processing API",
    description="""
## DocuParse — Intelligent PDF Processing & Takeoff System

A multi-user FastAPI backend that processes electrical/lighting construction PDFs through an
**8-step autocount pipeline**:

1. **Text Extraction** — native PyMuPDF text per page
2. **Image Rendering** — 300 DPI PNG per page
3. **Page Classification** — sheet-index, text patterns, sheet codes
4. **VLM Verification** — optional Gemini vision check
5. **Persist Results** — store to PostgreSQL
6. **Mark Complete** — update document status
7. **Full Text+Table Extraction** — combined text file with all 57+ tables
8. **Autocount Pipeline** — schedule isolation → plan splitting → fixture takeoff

---

### Authentication
All protected endpoints require a **Bearer JWT token**:
```
Authorization: Bearer <token>
```
Obtain a token via `POST /auth/login`.

---

### Key Features
- Multi-user isolation — each user sees only their documents
- Background processing — uploads return immediately, processing runs async
- Page classification — LIGHTING_PLAN, SCHEDULE, POWER_PLAN, DEMOLITION_PLAN, etc.
- Fixture takeoff — detects fixture codes (A1, B6, E3...) on lighting plan pages
- Annotated overlays — bounding-box PNG images for each matched fixture
""",
    version="2.0.0",
    openapi_tags=[
        {
            "name": "0. Document Viewer",
            "description": "Serves the single-page HTML Document Viewer frontend (`/viewer`).",
        },
        {
            "name": "1. Health",
            "description": "Service health check — database connectivity and VLM status.",
        },
        {
            "name": "2. Authentication",
            "description": "Register a new account or log in to receive a JWT access token.",
        },
        {
            "name": "3. Documents",
            "description": "Upload, list, retrieve, and delete documents. Accepts PDF or ZIP files up to 100 MB.",
        },
        {
            "name": "4. Processing",
            "description": "Re-trigger background processing or page re-classification on an existing document.",
        },
        {
            "name": "5. Pages",
            "description": "Retrieve extracted pages with text, images, and classification metadata. Also serves page PNG images.",
        },
        {
            "name": "6. Tables",
            "description": "Extract structured table data from relevant pages. Includes fixture-schedule filter and CSV export.",
        },
        {
            "name": "6b. Fixtures",
            "description": "Extract luminaire fixture records from schedule pages using an 8-layer pipeline (pdfplumber, keyword filter, VLM fallback).",
        },
        {
            "name": "7. Takeoff",
            "description": """**Autocount Pipeline** — runs after document processing completes.

Steps:
1. Extract all text + tables into a combined file
2. Isolate the lighting fixture schedule table → `lighting_schedule.csv`
3. Split lighting plan pages into a separate PDF → `lighting_panel_plans.pdf`
4. Search for fixture letter codes on plan pages, draw bounding boxes, export overlay PNGs + `fixture_counts.json`

Endpoints serve the results: counts JSON, overlay images, schedule CSV, split PDF, and pipeline rerun.""",
        },
    ],
    docs_url="/docs",
    redoc_url="/redoc",
    swagger_ui_parameters={
        "defaultModelsExpandDepth": -1,
        "docExpansion": "none",
        "operationsSorter": "alpha",
        "tagsSorter": "alpha",
        "tryItOutEnabled": True,
        "persistAuthorization": True,
    },
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=3600,
)


@app.exception_handler(413)
async def request_entity_too_large_handler(request: Request, exc):
    return JSONResponse(status_code=413, content={"detail": "File too large. Maximum upload size is 100MB."})


# Include routers
app.include_router(viewer.router)
app.include_router(user.router)
app.include_router(pages_router)
app.include_router(documents.router)
app.include_router(batch_router)
app.include_router(processing_routes.router)
app.include_router(classification.router)
app.include_router(tables.router)
app.include_router(fixtures.router)
app.include_router(takeoff.router)
app.include_router(batch_takeoff_router)
app.include_router(page_image_router)
app.include_router(pdf_render.router)
app.include_router(img_render.router)

# Serve frontend
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
def read_root():
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.get("/health", tags=["1. Health"])
def health_check():
    """Return service status and confirm the database is reachable."""
    db_ok = False
    db_error = None
    try:
        with SessionLocal() as session:
            session.execute(text("SELECT 1"))
        db_ok = True
    except Exception as exc:
        db_error = str(exc)
        logger.error("Health check — DB unreachable: %s", exc)

    payload = {
        "status": "healthy" if db_ok else "degraded",
        "database": "ok" if db_ok else f"error: {db_error}",
        "vlm": "enabled" if is_vlm_available() else "disabled",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    status_code = 200 if db_ok else 503
    return JSONResponse(content=payload, status_code=status_code)


# ── Custom OpenAPI schema — adds Bearer security scheme globally ───────────────
def _custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        tags=app.openapi_tags,
        routes=app.routes,
    )
    # Inject BearerAuth security scheme
    schema.setdefault("components", {}).setdefault("securitySchemes", {})["BearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": "Paste the JWT token from `POST /auth/login` → `access_token`",
    }
    # Apply it as global security (all endpoints require auth by default in UI)
    schema["security"] = [{"BearerAuth": []}]
    app.openapi_schema = schema
    return schema


app.openapi = _custom_openapi