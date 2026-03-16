"""
Document Viewer Routes
=======================
Serves the single-page HTML frontend application.

Service:  Document Viewer (Frontend UI)
Tag:      0. Document Viewer

Endpoints:
    GET  /viewer                    Full single-page HTML document viewer
    GET  /viewer/{document_id}      View a specific document by its ID
    GET  /                          Redirect to /viewer
"""

import os

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

router = APIRouter(tags=["0. Document Viewer"])

# Resolve path to static/index.html relative to this file's location
_static_dir = os.path.join(os.path.dirname(__file__), "..", "app.py", "static")


def _wants_json(request: Request) -> bool:
    """Return True if the client prefers JSON (e.g. Swagger UI / curl)."""
    accept = request.headers.get("accept", "")
    referer = request.headers.get("referer", "")
    # Swagger "Try it out" originates from the /docs page
    if "/docs" in referer or "/redoc" in referer:
        return True
    return "application/json" in accept and "text/html" not in accept


@router.get("/viewer", include_in_schema=True, response_class=HTMLResponse)
def document_viewer(request: Request):
    """Serve the full-featured Document Viewer web application.

    **Open this URL directly in your browser** — Swagger UI cannot render
    the HTML application inline.

    Single-page HTML/CSS/JS application that provides:
    - User registration & login (JWT)
    - PDF/ZIP file upload with drag-and-drop
    - Document list with status badges
    - Per-page viewer with 300-DPI thumbnail images
    - Text extraction display (native + OCR)
    - Page classification panel (relevant / irrelevant / type breakdown)
    - Re-classify button for unclassified documents
    - Extracted tables viewer (pdfplumber-powered from relevant pages)
    """
    if _wants_json(request):
        return JSONResponse({
            "message": "Document Viewer is an HTML application. Open the URL in your browser.",
            "viewer_url": "/viewer",
        })
    index_path = os.path.abspath(os.path.join(_static_dir, "index.html"))
    return FileResponse(index_path, media_type="text/html")


@router.get("/viewer/{document_id}", include_in_schema=True, response_class=HTMLResponse)
def document_viewer_by_id(document_id: str, request: Request):
    """Serve the Document Viewer pre-loaded with a specific document.

    **Open this URL directly in your browser** — Swagger UI cannot render
    the HTML application inline.

    The viewer will automatically open the document identified by
    ``document_id`` once the user is logged in.  The document ID is
    read from the URL path by the frontend JavaScript.
    """
    if _wants_json(request):
        return JSONResponse({
            "message": "Document Viewer is an HTML application. Open the URL in your browser.",
            "viewer_url": f"/viewer/{document_id}",
            "document_id": document_id,
        })
    index_path = os.path.abspath(os.path.join(_static_dir, "index.html"))
    return FileResponse(index_path, media_type="text/html")


@router.get("/", include_in_schema=False)
def root_redirect():
    """Redirect root to the Document Viewer."""
    return RedirectResponse(url="/viewer")
