"""
Page Viewer Routes
===================
Endpoints for viewing parsed document pages and serving page images.

Service:  Page Viewer
Prefix:   /documents  (pages)  |  /page-image  (image serving)
Tag:      3. Page Viewer

Endpoints:
    GET  /documents/{document_id}/pages   Paginated page listing with relevance filter
    GET  /page-image                      Serve a 300-DPI page PNG by path
"""

import logging
import traceback

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from utils import get_current_user, get_current_user_flexible, get_db
import models
import schemas
from services.document_service import DocumentService
from services.storage_service import StorageService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["5. Pages"])


@router.get("/{document_id}/pages", response_model=schemas.DocumentPagesListResponse)
def get_document_pages(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    skip: int = Query(default=0, ge=0, description="Number of pages to skip"),
    limit: int = Query(default=50, ge=1, le=500, description="Max pages to return"),
    relevance: str = Query(
        default=None,
        description="Filter by relevance: 'relevant', 'irrelevant', or omit for all pages",
    ),
):
    """Return parsed pages for a document (owner-scoped, paginated).

    Use `?relevance=relevant` to get only relevant pages (LIGHTING_PLAN,
    SCHEDULE, SYMBOLS_LEGEND, COVER) or `?relevance=irrelevant` for the rest.
    """
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")

        if relevance and relevance not in ("relevant", "irrelevant"):
            raise HTTPException(
                status_code=400,
                detail="relevance must be 'relevant', 'irrelevant', or omitted",
            )

        total_pages = DocumentService.count_pages(db, document_id)
        relevant_count = DocumentService.count_pages(db, document_id, relevance="relevant")
        irrelevant_count = total_pages - relevant_count
        pages = DocumentService.get_pages(
            db, document_id, skip=skip, limit=limit, relevance=relevance,
        )
        return schemas.DocumentPagesListResponse(
            document_id=document_id,
            total_pages=total_pages,
            relevant_pages=relevant_count,
            irrelevant_pages=irrelevant_count,
            pages=pages,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching pages for document %s: %s\n%s", document_id, e, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to retrieve document pages")


# ── Standalone page-image router (mounted at root, not under /documents/) ────

page_image_router = APIRouter(tags=["5. Pages"])


@page_image_router.get("/page-image")
def serve_page_image(
    path: str,
    token: str = Query(default=None, description="JWT token (for img src requests)"),
    current_user: models.User = Depends(get_current_user_flexible),
):
    """Stream a 300-DPI page PNG by absolute path.

    Security:
      - Requires a valid JWT (Authorization header OR ?token= query param).
      - Path must resolve to a file under storage/{current_user.id}/ — users
        cannot access other users' page images even if they know the path.
    """
    try:
        validated = StorageService.validate_user_image_path(path, current_user.id)
        if not validated:
            raise HTTPException(status_code=404, detail="Image not found or access denied")

        return FileResponse(validated, media_type="image/png")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error serving page image: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to serve page image")
