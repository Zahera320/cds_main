"""
Document Processing Routes
============================
Endpoints for re-triggering document processing pipelines.

Service:  Document Processing
Prefix:   /documents
Tag:      2. Document Processing

Endpoints:
    POST  /documents/{document_id}/reprocess   Re-trigger full page processing
    GET   /documents/{document_id}/logs         Retrieve processing log
"""

import logging
import os
import traceback

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session

from utils import get_current_user, get_db
import models
from processing import process_document_pages
from services.document_service import DocumentService
from logging_config import get_document_log_path

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["4. Processing"])


@router.post("/{document_id}/reprocess")
def reprocess_document(
    document_id: str,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Re-trigger page processing for a document that is stuck or failed.

    Resets the document status to ``processing`` and launches the full
    extraction + classification pipeline in the background.
    Only PDF documents can be reprocessed.
    """
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")
        if document.file_type != "pdf":
            raise HTTPException(status_code=400, detail="Only PDF documents can be reprocessed")
        if document.status == "processing":
            raise HTTPException(status_code=409, detail="Document is already being processed")

        DocumentService.update_status(db, document, "processing")
        # Delete existing pages so re-processing won't hit UniqueConstraint
        DocumentService.delete_pages(db, document_id)
        background_tasks.add_task(
            process_document_pages,
            document_id=document_id,
            user_id=current_user.id,
        )
        return {"document_id": document_id, "message": "Reprocessing started"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error reprocessing document %s: %s\n%s", document_id, e, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to start reprocessing")


@router.get("/{document_id}/logs", response_class=PlainTextResponse)
def get_processing_logs(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Retrieve the processing log for a specific document.

    Returns the per-document ``processing.log`` that captures every step
    of the pipeline: text extraction, page classification, VLM verification,
    autocount, and any errors that occurred.
    """
    document = DocumentService.get_user_document(db, document_id, current_user.id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found or access denied")

    log_path = get_document_log_path(current_user.id, document_id)
    if not os.path.isfile(log_path):
        raise HTTPException(status_code=404, detail="No processing log found for this document. It may not have been processed yet.")

    with open(log_path, "r", encoding="utf-8") as f:
        return f.read()
