"""
Document Routes
================
Slim route handlers that delegate to the service layer.

Endpoints:
    POST   /documents/upload              Upload PDF or ZIP
    GET    /documents/                     List user's documents
    GET    /documents/{document_id}        Get single document
    GET    /documents/{document_id}/pages  Get parsed pages
    GET    /documents/page-image           Serve a 300-DPI page PNG
"""

import hashlib
import logging
import os
import traceback
import uuid
from datetime import datetime, timezone
from typing import List

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from utils import get_current_user, get_db
import models
import schemas
from config import MAX_FILE_SIZE, ALLOWED_EXTENSIONS
from processing import process_document_pages
from services.storage_service import StorageService
from services.zip_service import ZipService
from services.document_service import DocumentService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["3. Documents"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _file_extension(filename: str) -> str:
    return os.path.splitext(filename.lower())[1]


# ── Upload ────────────────────────────────────────────────────────────────────

@router.post("/upload", response_model=schemas.UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Upload a PDF or ZIP file with validation, storage, and background parsing."""
    # ── Validate presence + extension ─────────────────────────────────────
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    ext = _file_extension(file.filename)
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Only PDF and ZIP files are allowed. Got: {ext}")

    # ── Read + validate size ──────────────────────────────────────────────
    try:
        contents = await file.read()
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to read uploaded file")

    if len(contents) == 0:
        raise HTTPException(status_code=400, detail="Empty file not allowed")
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail=f"File too large. Maximum size: {MAX_FILE_SIZE // (1024*1024)}MB")

    # ── Deduplication check ────────────────────────────────────────────
    # Compute BEFORE writing anything to disk so we abort cheaply.
    content_hash = hashlib.sha256(contents).hexdigest()
    existing = DocumentService.find_duplicate(db, current_user.id, content_hash)
    if existing:
        logger.info(
            "Duplicate upload rejected for user %d: hash=%s existing_doc=%s",
            current_user.id, content_hash, existing.document_id,
        )
        raise HTTPException(
            status_code=409,
            detail={
                "error": "duplicate_file",
                "message": "You have already uploaded this exact file.",
                "existing_document_id": existing.document_id,
                "existing_filename": existing.filename,
                "existing_status": existing.status,
            },
        )

    # ── Persist to disk ───────────────────────────────────────────────────
    document_id = str(uuid.uuid4())
    safe_name = StorageService.sanitize_filename(file.filename)

    try:
        original_dir = StorageService.original_dir(current_user.id, document_id)
        file_path = StorageService.save_file(original_dir, safe_name, contents)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to save uploaded file")

    file_type = "pdf" if ext == ".pdf" else "zip"
    status_msg = "uploaded"

    # ── ZIP handling ──────────────────────────────────────────────────────
    # When a ZIP is uploaded, extract its PDFs and create SEPARATE Document
    # records for each, all sharing the same batch_id.  The original ZIP
    # record acts as a parent/placeholder with status='completed'.
    if ext == ".zip":
        batch_id = str(uuid.uuid4())
        try:
            pdf_files = ZipService.extract_pdfs(file_path, original_dir)
            if not pdf_files:
                StorageService.cleanup_document_dir(current_user.id, document_id)
                raise HTTPException(status_code=400, detail="ZIP file contains no PDF files")
        except HTTPException:
            raise
        except Exception:
            StorageService.cleanup_document_dir(current_user.id, document_id)
            raise HTTPException(status_code=500, detail="Failed to extract ZIP file")

        # Save ZIP record as completed (parent placeholder)
        try:
            document = DocumentService.create_document(
                db,
                document_id=document_id,
                filename=file.filename,
                file_type="zip",
                file_size=len(contents),
                content_hash=content_hash,
                owner_id=current_user.id,
                upload_time=datetime.now(timezone.utc).replace(tzinfo=None),
                status="completed",
                batch_id=batch_id,
            )
        except Exception:
            db.rollback()
            StorageService.cleanup_document_dir(current_user.id, document_id)
            raise HTTPException(status_code=500, detail="Failed to save document metadata")

        # Create a separate Document for each extracted PDF
        child_doc_ids = []
        for pdf_path_on_disk in pdf_files:
            child_doc_id = str(uuid.uuid4())
            child_filename = os.path.basename(pdf_path_on_disk)
            # Move the extracted PDF into its own document directory
            child_original_dir = StorageService.original_dir(current_user.id, child_doc_id)
            import shutil
            dest = os.path.join(child_original_dir, child_filename)
            shutil.move(pdf_path_on_disk, dest)

            child_size = os.path.getsize(dest)
            with open(dest, "rb") as fh:
                child_hash = hashlib.sha256(fh.read()).hexdigest()

            try:
                child_doc = DocumentService.create_document(
                    db,
                    document_id=child_doc_id,
                    filename=child_filename,
                    file_type="pdf",
                    file_size=child_size,
                    content_hash=child_hash,
                    owner_id=current_user.id,
                    upload_time=datetime.now(timezone.utc).replace(tzinfo=None),
                    status="uploaded",
                    batch_id=batch_id,
                )
                background_tasks.add_task(
                    process_document_pages,
                    document_id=child_doc_id,
                    user_id=current_user.id,
                )
                DocumentService.update_status(db, child_doc, "processing")
                child_doc_ids.append(child_doc_id)
            except Exception:
                db.rollback()
                logger.error("Failed to create child doc for %s", child_filename)

        return schemas.UploadResponse(
            document_id=document_id,
            filename=file.filename,
            file_type="zip",
            file_size=len(contents),
            status="completed",
            batch_id=batch_id,
            extracted_pdf_count=len(child_doc_ids),
            message=f"ZIP uploaded — {len(child_doc_ids)} PDFs extracted into batch {batch_id}",
        )

    # ── Save DB record ────────────────────────────────────────────────────
    try:
        document = DocumentService.create_document(
            db,
            document_id=document_id,
            filename=file.filename,
            file_type=file_type,
            file_size=len(contents),
            content_hash=content_hash,
            owner_id=current_user.id,
            upload_time=datetime.now(timezone.utc).replace(tzinfo=None),
            status=status_msg,
        )
    except Exception:
        db.rollback()
        StorageService.cleanup_file(file_path)
        raise HTTPException(status_code=500, detail="Failed to save document metadata")

    # ── Trigger background parsing for PDFs ───────────────────────────────
    if file_type == "pdf" and status_msg == "uploaded":
        # IMPORTANT: do NOT pass the request-scoped `db` session to the
        # background task.  FastAPI closes that session as soon as the
        # endpoint returns (get_db finally-block), so the task would
        # receive an already-closed connection and crash.
        # The background task creates its own session internally.
        background_tasks.add_task(
            process_document_pages,
            document_id=document_id,
            user_id=current_user.id,
        )
        status_msg = "processing"
        DocumentService.update_status(db, document, "processing")

    return schemas.UploadResponse(
        document_id=document_id,
        filename=file.filename,
        file_type=file_type,
        file_size=len(contents),
        status=status_msg,
        message=f"File uploaded successfully. Status: {status_msg}",
    )


# ── Batch Upload ──────────────────────────────────────────────────────────────

@router.post("/upload-multiple", response_model=schemas.BatchUploadResponse)
async def upload_multiple_files(
    files: List[UploadFile] = File(...),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Upload multiple PDF/ZIP files in one request — all share a single batch_id.

    Every PDF (whether uploaded directly or extracted from a ZIP) becomes its
    own Document record.  All documents are linked by the returned ``batch_id``
    so downstream endpoints can treat them as one project context.
    """
    batch_id = str(uuid.uuid4())
    results: List[schemas.UploadResponse] = []
    errors: List[dict] = []
    succeeded = 0
    failed_count = 0

    for file in files:
        try:
            if not file or not file.filename:
                errors.append({"filename": "unknown", "error": "No file provided"})
                failed_count += 1
                continue

            ext = _file_extension(file.filename)
            if ext not in ALLOWED_EXTENSIONS:
                errors.append({"filename": file.filename, "error": f"Only PDF and ZIP files are allowed. Got: {ext}"})
                failed_count += 1
                continue

            try:
                contents = await file.read()
            except Exception:
                errors.append({"filename": file.filename, "error": "Failed to read file"})
                failed_count += 1
                continue

            if len(contents) == 0:
                errors.append({"filename": file.filename, "error": "Empty file not allowed"})
                failed_count += 1
                continue

            if len(contents) > MAX_FILE_SIZE:
                errors.append({"filename": file.filename, "error": f"File too large. Maximum size: {MAX_FILE_SIZE // (1024*1024)}MB"})
                failed_count += 1
                continue

            # Dedup check
            content_hash = hashlib.sha256(contents).hexdigest()
            existing = DocumentService.find_duplicate(db, current_user.id, content_hash)
            if existing:
                errors.append({"filename": file.filename, "error": "Duplicate file"})
                failed_count += 1
                continue

            if ext == ".zip":
                # ── ZIP in multi-upload: extract child PDFs ───────────────
                zip_doc_id = str(uuid.uuid4())
                safe_name = StorageService.sanitize_filename(file.filename)
                try:
                    zip_original_dir = StorageService.original_dir(current_user.id, zip_doc_id)
                    zip_path = StorageService.save_file(zip_original_dir, safe_name, contents)
                except Exception:
                    errors.append({"filename": file.filename, "error": "Failed to save file"})
                    failed_count += 1
                    continue

                try:
                    pdf_files = ZipService.extract_pdfs(zip_path, zip_original_dir)
                    if not pdf_files:
                        StorageService.cleanup_document_dir(current_user.id, zip_doc_id)
                        errors.append({"filename": file.filename, "error": "ZIP contains no PDFs"})
                        failed_count += 1
                        continue
                except Exception:
                    StorageService.cleanup_document_dir(current_user.id, zip_doc_id)
                    errors.append({"filename": file.filename, "error": "Failed to extract ZIP"})
                    failed_count += 1
                    continue

                # ZIP parent record
                try:
                    DocumentService.create_document(
                        db,
                        document_id=zip_doc_id,
                        filename=file.filename,
                        file_type="zip",
                        file_size=len(contents),
                        content_hash=content_hash,
                        owner_id=current_user.id,
                        upload_time=datetime.now(timezone.utc).replace(tzinfo=None),
                        status="completed",
                        batch_id=batch_id,
                    )
                except Exception:
                    db.rollback()
                    continue

                import shutil as _shutil
                for pdf_on_disk in pdf_files:
                    child_doc_id = str(uuid.uuid4())
                    child_filename = os.path.basename(pdf_on_disk)
                    child_dir = StorageService.original_dir(current_user.id, child_doc_id)
                    dest = os.path.join(child_dir, child_filename)
                    _shutil.move(pdf_on_disk, dest)
                    child_size = os.path.getsize(dest)
                    with open(dest, "rb") as fh:
                        child_hash = hashlib.sha256(fh.read()).hexdigest()
                    try:
                        child_doc = DocumentService.create_document(
                            db,
                            document_id=child_doc_id,
                            filename=child_filename,
                            file_type="pdf",
                            file_size=child_size,
                            content_hash=child_hash,
                            owner_id=current_user.id,
                            upload_time=datetime.now(timezone.utc).replace(tzinfo=None),
                            status="uploaded",
                            batch_id=batch_id,
                        )
                        background_tasks.add_task(
                            process_document_pages,
                            document_id=child_doc_id,
                            user_id=current_user.id,
                        )
                        DocumentService.update_status(db, child_doc, "processing")
                        results.append(schemas.UploadResponse(
                            document_id=child_doc_id,
                            filename=child_filename,
                            file_type="pdf",
                            file_size=child_size,
                            status="processing",
                            batch_id=batch_id,
                            message=f"Extracted from {file.filename}",
                        ))
                        succeeded += 1
                    except Exception:
                        db.rollback()
                        logger.error("Failed to create child doc for %s", child_filename)

            else:
                # ── Direct PDF in multi-upload ────────────────────────────
                document_id = str(uuid.uuid4())
                safe_name = StorageService.sanitize_filename(file.filename)
                try:
                    original_dir = StorageService.original_dir(current_user.id, document_id)
                    file_path = StorageService.save_file(original_dir, safe_name, contents)
                except Exception:
                    errors.append({"filename": file.filename, "error": "Failed to save file"})
                    failed_count += 1
                    continue

                try:
                    document = DocumentService.create_document(
                        db,
                        document_id=document_id,
                        filename=file.filename,
                        file_type="pdf",
                        file_size=len(contents),
                        content_hash=content_hash,
                        owner_id=current_user.id,
                        upload_time=datetime.now(timezone.utc).replace(tzinfo=None),
                        status="uploaded",
                        batch_id=batch_id,
                    )
                except Exception:
                    db.rollback()
                    StorageService.cleanup_file(file_path)
                    errors.append({"filename": file.filename, "error": "Failed to save metadata"})
                    failed_count += 1
                    continue

                background_tasks.add_task(
                    process_document_pages,
                    document_id=document_id,
                    user_id=current_user.id,
                )
                DocumentService.update_status(db, document, "processing")

                results.append(schemas.UploadResponse(
                    document_id=document_id,
                    filename=file.filename,
                    file_type="pdf",
                    file_size=len(contents),
                    status="processing",
                    batch_id=batch_id,
                    message=f"File uploaded successfully.",
                ))
                succeeded += 1

        except Exception as e:
            logger.error("Batch upload error for file %s: %s\\n%s", getattr(file, 'filename', 'unknown'), e, traceback.format_exc())
            errors.append({"filename": getattr(file, 'filename', 'unknown'), "error": "Unexpected error"})
            failed_count += 1

    return schemas.BatchUploadResponse(
        batch_id=batch_id,
        total=len(files),
        succeeded=succeeded,
        failed=failed_count,
        results=results,
        errors=errors,
    )


# ── Batch document listing ────────────────────────────────────────────────────

batch_router = APIRouter(prefix="/batch", tags=["3. Documents"])


@batch_router.get("/{batch_id}/documents", response_model=schemas.BatchDocumentsResponse)
def get_batch_documents(
    batch_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """List all documents belonging to a batch (owner-scoped)."""
    docs = DocumentService.get_batch_documents(db, batch_id, current_user.id)
    if not docs:
        raise HTTPException(status_code=404, detail="Batch not found or empty")
    all_done = all(d.status == "completed" for d in docs)
    return schemas.BatchDocumentsResponse(
        batch_id=batch_id,
        total=len(docs),
        all_completed=all_done,
        documents=docs,
    )


# ── Delete document ──────────────────────────────────────────────────────────

@router.delete("/{document_id}", response_model=schemas.DeleteResponse)
def delete_document(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Delete a document and its associated data (owner-scoped)."""
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")

        # Delete from DB (cascades to pages)
        DocumentService.delete_document(db, document)

        # Clean up files on disk
        StorageService.cleanup_document_dir(current_user.id, document_id)

        return schemas.DeleteResponse(
            document_id=document_id,
            message="Document deleted successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error("Error deleting document %s: %s\\n%s", document_id, e, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to delete document")


# ── List documents ────────────────────────────────────────────────────────────

@router.get("/", response_model=List[schemas.DocumentResponse])
def get_documents(
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
    skip: int = Query(default=0, ge=0, description="Number of records to skip"),
    limit: int = Query(default=50, ge=1, le=200, description="Max records to return"),
):
    """Return documents belonging to the authenticated user (paginated)."""
    try:
        return DocumentService.list_user_documents(db, current_user.id, skip=skip, limit=limit)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error listing documents for user %d: %s\n%s", current_user.id, e, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to retrieve documents")


# ── Single document ──────────────────────────────────────────────────────────

@router.get("/{document_id}", response_model=schemas.DocumentResponse)
def get_document(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return a single document by UUID (owner-scoped)."""
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")
        return document
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching document %s: %s\n%s", document_id, e, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to retrieve document")
