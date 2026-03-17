"""
Key Notes Routes
=================
Endpoint for extracting key / general notes from document pages.

Service:  Key Notes
Prefix:   /documents
Tag:      Key Notes

Endpoints:
    GET  /documents/{document_id}/keynotes   Extract notes from relevant pages
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
from processing.keynote_extractor import extract_keynotes

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
    """Extract key notes and general notes from document pages.

    Returns structured notes parsed from the extracted text of each page
    that contains a KEY NOTES, GENERAL NOTES, ELECTRICAL NOTES, or
    LIGHTING NOTES section.
    """
    try:
        doc = DocumentService.get_user_document(db, document_id, current_user.id)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")

        query = db.query(models.DocumentPage).filter(
            models.DocumentPage.document_id == document_id,
            models.DocumentPage.extracted_text.isnot(None),
        )
        if relevance != "all":
            query = query.filter(models.DocumentPage.is_relevant == True)

        pages = query.order_by(models.DocumentPage.page_number).all()

        page_keynotes = []
        total_notes = 0
        for page in pages:
            notes = extract_keynotes(page.extracted_text or "")
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
