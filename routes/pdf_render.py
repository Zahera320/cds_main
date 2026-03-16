import os
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.orm import Session

from utils import get_current_user, get_db
import models
from services.document_service import DocumentService
from services.storage_service import StorageService

router = APIRouter(prefix="/pdf-render", tags=["React PDF Viewer"])

@router.get("/{document_id}")
def render_pdf_document(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Endpoint for a React app to render the actual PDF document file.
    Handles both single PDF uploads and ZIP uploads.
    """
    document = DocumentService.get_user_document(db, document_id, current_user.id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    if document.file_type == "pdf":
        # For a single PDF, return the file directly to be rendered
        try:
            file_path = StorageService.locate_original_file(current_user.id, document_id, "pdf")
            return FileResponse(file_path, media_type="application/pdf", filename=document.filename)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="PDF file not found on disk")

    elif document.file_type == "zip":
        # For a ZIP, fetch all extracted PDFs from the same batch
        if not document.batch_id:
            raise HTTPException(status_code=500, detail="ZIP document missing batch_id")
            
        child_docs = DocumentService.get_batch_documents(db, document.batch_id, current_user.id)
        
        # Filter out the parent zip itself to only get the extracted PDFs
        pdf_docs = [d for d in child_docs if d.file_type == "pdf"]
        
        # True if extraction is fully complete and background tasks are done
        all_completed = all(d.status == "completed" for d in pdf_docs)
        
        response_data = {
            "is_zip": True,
            "filename": document.filename,
            "batch_id": document.batch_id,
            "extraction_done": len(pdf_docs) > 0,
            "processing_completed": all_completed,
            "extracted_pdfs": [
                {
                    "document_id": d.document_id,
                    "filename": d.filename,
                    "status": d.status,
                    "view_url": f"/pdf-render/{d.document_id}"
                }
                for d in pdf_docs
            ]
        }
        return JSONResponse(content=response_data)
        
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {document.file_type}")
