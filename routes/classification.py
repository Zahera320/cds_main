"""
Page Classification Routes
============================
Endpoints for viewing and re-running page classification results.

Service:  Page Classification
Prefix:   /documents
Tag:      4. Page Classification

Endpoints:
    GET   /documents/{document_id}/classification-summary   Classification breakdown
    POST  /documents/{document_id}/reclassify               Re-run classifier only
"""

import logging
import traceback

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session

from utils import get_current_user, get_db
import models
import schemas
from processing import reclassify_document_pages
from services.document_service import DocumentService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["4. Processing"])


@router.get("/{document_id}/classification-summary", response_model=schemas.ClassificationSummaryResponse)
def get_classification_summary(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Check whether page classification is complete and get a breakdown of
    relevant vs irrelevant pages with their page numbers and types.

    Returns page counts, page-number lists, type breakdown, and lists of
    distinct types in each relevance category.
    """
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")

        all_pages = DocumentService.get_all_pages(db, document_id)

        # Classification is considered done when every page has a page_type set
        classification_done = (
            document.status == "completed"
            and len(all_pages) > 0
            and all(p.page_type is not None for p in all_pages)
        )

        relevant_page_numbers = [p.page_number for p in all_pages if p.is_relevant]
        irrelevant_page_numbers = [p.page_number for p in all_pages if not p.is_relevant]

        # Type breakdown
        type_breakdown: dict = {}
        for p in all_pages:
            t = p.page_type or "UNCLASSIFIED"
            type_breakdown[t] = type_breakdown.get(t, 0) + 1

        # Distinct types in each category
        relevant_types = sorted({p.page_type for p in all_pages if p.is_relevant and p.page_type})
        irrelevant_types = sorted({p.page_type for p in all_pages if not p.is_relevant and p.page_type})

        # Build page_number → sheet_code mapping
        page_sheet_codes = {
            p.page_number: p.sheet_code
            for p in all_pages
            if getattr(p, "sheet_code", None)
        }

        return schemas.ClassificationSummaryResponse(
            document_id=document_id,
            status=document.status,
            classification_done=classification_done,
            classification_message=getattr(document, "classification_message", None),
            total_pages=len(all_pages),
            relevant_pages=len(relevant_page_numbers),
            irrelevant_pages=len(irrelevant_page_numbers),
            relevant_page_numbers=relevant_page_numbers,
            irrelevant_page_numbers=irrelevant_page_numbers,
            type_breakdown=type_breakdown,
            relevant_types=relevant_types,
            irrelevant_types=irrelevant_types,
            page_sheet_codes=page_sheet_codes,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Error fetching classification summary for %s: %s\n%s",
            document_id, e, traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail="Failed to retrieve classification summary")


@router.post("/{document_id}/reclassify")
def reclassify_document(
    document_id: str,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Re-run page classification on an already-processed document.

    Does NOT re-extract text or re-render images — only re-runs the
    4-priority classifier on the existing extracted text and images.
    Works on documents with status ``completed`` or ``failed``.
    """
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")
        if document.file_type != "pdf":
            raise HTTPException(status_code=400, detail="Only PDF documents can be reclassified")
        if document.status == "processing":
            raise HTTPException(status_code=409, detail="Document is still processing")
        if document.status == "uploaded":
            raise HTTPException(status_code=409, detail="Document has not been processed yet")

        background_tasks.add_task(
            reclassify_document_pages,
            document_id=document_id,
            user_id=current_user.id,
        )
        return {"document_id": document_id, "message": "Reclassification started"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error reclassifying document %s: %s\n%s", document_id, e, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to start reclassification")
