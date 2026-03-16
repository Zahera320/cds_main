"""
Image Render Routes
====================
Serves pipeline images (page renders) for a given document.

Service:  Image Render
Tag:      8. Image Render

Endpoints:
    GET  /img-render/{document_id}/{image_number}
         Returns the PNG image for the given page number.
         image_number is a 1-based integer (e.g. 4 → page_0004.png).
"""

import os

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from utils import get_current_user, get_db
import models
from services.document_service import DocumentService
from config import STORAGE_PATH

router = APIRouter(prefix="/img-render", tags=["8. Image Render"])


@router.get("/{document_id}/{image_number}")
def render_pipeline_image(
    document_id: str,
    image_number: int,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Return a single pipeline image PNG for the given document and page number.

    - **document_id**: UUID of the document
    - **image_number**: 1-based page number (e.g. `4` → `page_0004.png`)

    Images are read from:
    ``storage/{user_id}/{document_id}/pipeline/images/page_XXXX.png``
    """
    # Verify the document belongs to the current user
    document = DocumentService.get_user_document(db, document_id, current_user.id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found or access denied")

    # Build filename: page_0004.png style (zero-padded to 4 digits)
    filename = f"page_{image_number:04d}.png"

    image_path = os.path.join(
        STORAGE_PATH,
        str(current_user.id),
        document_id,
        "pipeline",
        "images",
        filename,
    )

    # Security: make sure resolved path is under STORAGE_PATH
    abs_storage = os.path.realpath(STORAGE_PATH)
    abs_image = os.path.realpath(image_path)
    if not abs_image.startswith(abs_storage + os.sep):
        raise HTTPException(status_code=403, detail="Access denied")

    if not os.path.isfile(abs_image):
        raise HTTPException(
            status_code=404,
            detail=f"Image not found: {filename} for document {document_id}",
        )

    return FileResponse(abs_image, media_type="image/png", filename=filename)
