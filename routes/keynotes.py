"""
Key Notes Routes
=================
Endpoint for extracting key notes from Lightning (LIGHTING_PLAN) pages
using VLM (Gemini Vision) exclusively.

Service:  Key Notes
Prefix:   /documents
Tag:      Key Notes

Endpoints:
    GET  /documents/{document_id}/keynotes   Extract key notes from lighting pages
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from utils import get_current_user, get_db
import models
import schemas
from services.document_service import DocumentService

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from processing.keynote_extractor import extract_keynotes_vlm

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["Key Notes"])


@router.get("/{document_id}/keynotes", response_model=schemas.KeynotesResponse)
def get_document_keynotes(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    relevance: str = Query(
        default="relevant",
        description="Filter pages: 'relevant', 'all', or omit for relevant only",
    ),
):
    """Extract key notes from Lightning (LIGHTING_PLAN) pages using VLM.

    Returns structured key notes extracted via Gemini Vision from the
    top-left corner of each LIGHTING_PLAN page.
    """
    try:
        doc = DocumentService.get_user_document(db, document_id, current_user.id)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")

        query = db.query(models.DocumentPage).filter(
            models.DocumentPage.document_id == document_id,
            models.DocumentPage.page_type == "LIGHTING_PLAN",
        )
        if relevance != "all":
            query = query.filter(models.DocumentPage.is_relevant == True)

        pages = query.order_by(models.DocumentPage.page_number).all()

        # VLM-only extraction from LIGHTING_PLAN pages
        page_notes_map = extract_keynotes_vlm(pages)

        page_keynotes = []
        total_notes = 0
        for page in pages:
            notes = page_notes_map.get(page.page_number, [])
            if notes:
                total_notes += len(notes)
                page_keynotes.append({
                    "page_number": page.page_number,
                    "sheet_code": page.sheet_code or None,
                    "page_type": page.page_type or "UNKNOWN",
                    "notes": notes,
                })

        return schemas.KeynotesResponse(
            document_id=document_id,
            total_notes=total_notes,
            pages=page_keynotes,
        )

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Keynotes extraction failed for %s: %s", document_id, exc)
        raise HTTPException(status_code=500, detail=str(exc))
