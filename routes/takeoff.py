"""
Takeoff Routes
===============
Endpoints for accessing autocount pipeline results — fixture counts,
bounding-box overlay images, schedule CSV, matrix CSV, and split PDF.

Service:  Takeoff / Autocount Pipeline
Prefix:   /documents
Tag:      7. Takeoff

Endpoints:
    GET   /documents/{document_id}/takeoff                  Takeoff results (JSON)
    GET   /documents/{document_id}/takeoff/overlay          Serve overlay image (by sheet_id)
    GET   /documents/{document_id}/takeoff/matrix-csv       Download matrix CSV
    GET   /documents/{document_id}/takeoff/schedule-csv     Download schedule CSV
    GET   /documents/{document_id}/takeoff/docling-outputs  Get Docling extraction metadata
    GET   /documents/{document_id}/takeoff/tables-json      Download extracted tables JSON
    GET   /documents/{document_id}/takeoff/split-pdf        Download split PDF
    POST  /documents/{document_id}/takeoff/rerun            Re-run autocount pipeline
"""

import json
import logging
import os
import traceback

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from utils import get_current_user, get_current_user_flexible, get_db
import models
import schemas
from services.document_service import DocumentService
from services.storage_service import StorageService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["7. Takeoff"])


def _pipeline_dir_for_user(user_id: int, document_id: str) -> str:
    """Return the pipeline output directory (without auto-creating)."""
    from config import STORAGE_PATH
    return os.path.join(STORAGE_PATH, str(user_id), document_id, "pipeline")


def _read_pipeline_results(pipeline_dir: str) -> dict:
    """Read takeoff pipeline results from the pipeline directory.
    Checks both the `fixture_results` subfolder (new layout) and the
    pipeline root (TakeoffGenerator default layout).
    """
    if not os.path.isdir(pipeline_dir):
        return {"fixture_counts": {}, "available_sheets": [], "matrix_csv_available": False,
                "csv_available": False, "split_pdf_available": False, "total_found": 0, "status": "not_run"}

    results_dir = os.path.join(pipeline_dir, "fixture_results")

    fixture_counts = {}
    # Try fixture_results/ first, then pipeline root
    counts_path = os.path.join(results_dir, "fixture_counts.json")
    if not os.path.isfile(counts_path):
        counts_path = os.path.join(pipeline_dir, "fixture_counts.json")
    if os.path.isfile(counts_path):
        with open(counts_path, "r") as f:
            fixture_counts = json.load(f)

    # Extract sheet IDs from overlay filenames (use set to avoid duplicates)
    available_sheets_set = set()
    overlays_dir = os.path.join(results_dir, "overlays")
    if not os.path.isdir(overlays_dir):
        overlays_dir = None
    # Also check pipeline root for output_overlay_page_*.png
    for search_dir in ([overlays_dir] if overlays_dir else []) + [pipeline_dir]:
        for fname in os.listdir(search_dir):
            if fname.startswith("overlay_") and fname.endswith(".png"):
                sheet_id = fname.replace("overlay_", "").replace(".png", "")
                available_sheets_set.add(sheet_id)
            elif fname.startswith("output_overlay_page_") and fname.endswith(".png"):
                sheet_id = fname.replace("output_overlay_page_", "").replace(".png", "")
                available_sheets_set.add(sheet_id)
    available_sheets = sorted(available_sheets_set)

    matrix_csv_available = (
        os.path.isfile(os.path.join(results_dir, "fixture_takeoff_matrix.csv"))
        or os.path.isfile(os.path.join(pipeline_dir, "fixture_takeoff_matrix.csv"))
    )
    csv_available = (
        os.path.isfile(os.path.join(pipeline_dir, "lighting_schedule.csv"))
        or os.path.isfile(os.path.join(pipeline_dir, "relevant_tables", "light_fixture_schedule.csv"))
    )
    split_pdf_available = os.path.isfile(os.path.join(pipeline_dir, "lighting_panel_plans.pdf"))
    
    # total_found: supports both flat {code: N} and nested {code: {sheet: N}} formats
    total_found = 0
    for v in fixture_counts.values():
        if isinstance(v, dict):
            total_found += sum(v.values())
        else:
            total_found += v

    running_flag = os.path.isfile(os.path.join(pipeline_dir, "_running"))
    if running_flag:
        status = "running"
    elif fixture_counts:
        status = "completed"
    elif csv_available or split_pdf_available:
        status = "partial"
    else:
        status = "not_run" if not os.listdir(pipeline_dir) else "failed"

    return {
        "fixture_counts": fixture_counts, 
        "available_sheets": available_sheets,
        "matrix_csv_available": matrix_csv_available,
        "csv_available": csv_available, 
        "split_pdf_available": split_pdf_available,
        "total_found": total_found, 
        "status": status
    }


@router.get("/{document_id}/takeoff")
def get_takeoff_results(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get takeoff results metadata — JSON tally, available sheets, and status."""
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")

        # Try per-document pipeline first
        pipeline_dir = _pipeline_dir_for_user(current_user.id, document_id)
        results = _read_pipeline_results(pipeline_dir)

        if results["status"] in ("completed", "running", "partial"):
            return {
                "document_id": document_id,
                "batch_id": document.batch_id,
                "fixture_counts": results["fixture_counts"],
                "total_fixtures_found": results["total_found"],
                "available_sheets": results["available_sheets"],
                "matrix_csv_available": results["matrix_csv_available"],
                "schedule_csv_available": results["csv_available"],
                "split_pdf_available": results["split_pdf_available"],
                "pipeline_status": results["status"],
                "source": "document"
            }

        # Fall back to batch pipeline if document belongs to a batch
        if document.batch_id:
            batch_dir = _batch_pipeline_dir_for_user(current_user.id, document.batch_id)
            batch_results = _read_pipeline_results(batch_dir)
            if batch_results["status"] in ("completed", "running", "partial"):
                return {
                    "document_id": document_id,
                    "batch_id": document.batch_id,
                    "fixture_counts": batch_results["fixture_counts"],
                    "total_fixtures_found": batch_results["total_found"],
                    "available_sheets": batch_results["available_sheets"],
                    "matrix_csv_available": batch_results["matrix_csv_available"],
                    "schedule_csv_available": batch_results["csv_available"],
                    "split_pdf_available": batch_results["split_pdf_available"],
                    "pipeline_status": batch_results["status"],
                    "source": "batch"
                }

        # No results anywhere
        return {
            "document_id": document_id,
            "batch_id": document.batch_id,
            "pipeline_status": results["status"],
            "source": "document"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching takeoff results: %s", e)
        raise HTTPException(status_code=500, detail="Failed to retrieve takeoff results")


@router.get("/{document_id}/takeoff/overlay")
def serve_overlay_image(
    document_id: str,
    sheet_id: str = Query(..., description="The sheet ID identifier (e.g., 'E000', 'E101')"),
    token: str = Query(default=None, description="JWT token (for img src requests)"),
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Serve a takeoff overlay image by its Sheet ID."""
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")

        # Try per-document pipeline first
        pipeline_dir = _pipeline_dir_for_user(current_user.id, document_id)
        # Check new layout first, then TakeoffGenerator default layout
        image_path = os.path.join(pipeline_dir, "fixture_results", "overlays", f"overlay_{sheet_id}.png")
        validated = StorageService.validate_user_image_path(image_path, current_user.id)
        if not validated:
            image_path = os.path.join(pipeline_dir, f"output_overlay_page_{sheet_id}.png")
            validated = StorageService.validate_user_image_path(image_path, current_user.id)

        # Fall back to batch pipeline
        if not validated and document.batch_id:
            batch_dir = _batch_pipeline_dir_for_user(current_user.id, document.batch_id)
            image_path = os.path.join(batch_dir, "fixture_results", "overlays", f"overlay_{sheet_id}.png")
            validated = StorageService.validate_user_image_path(image_path, current_user.id)
            if not validated:
                image_path = os.path.join(batch_dir, f"output_overlay_page_{sheet_id}.png")
                validated = StorageService.validate_user_image_path(image_path, current_user.id)

        if not validated:
            raise HTTPException(status_code=404, detail=f"Overlay image for sheet '{sheet_id}' not found")

        return FileResponse(validated, media_type="image/png")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error serving overlay image: %s", e)
        raise HTTPException(status_code=500, detail="Failed to serve overlay image")


@router.get("/{document_id}/takeoff/matrix-csv")
def download_matrix_csv(
    document_id: str,
    token: str = Query(default=None),
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Download the final computed Fixture Takeoff Matrix CSV."""
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")

        pipeline_dir = _pipeline_dir_for_user(current_user.id, document_id)
        csv_path = os.path.join(pipeline_dir, "fixture_results", "fixture_takeoff_matrix.csv")
        validated = StorageService.validate_user_image_path(csv_path, current_user.id) if os.path.isfile(csv_path) else None

        if not validated and document.batch_id:
            batch_dir = _batch_pipeline_dir_for_user(current_user.id, document.batch_id)
            csv_path = os.path.join(batch_dir, "fixture_results", "fixture_takeoff_matrix.csv")
            validated = StorageService.validate_user_image_path(csv_path, current_user.id) if os.path.isfile(csv_path) else None

        if not validated:
            raise HTTPException(status_code=404, detail="Takeoff Matrix CSV not available")

        return FileResponse(validated, media_type="text/csv", filename=f"fixture_takeoff_matrix_{document_id}.csv")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error serving matrix CSV: %s", e)
        raise HTTPException(status_code=500, detail="Failed to serve matrix CSV")


@router.get("/{document_id}/takeoff/schedule-csv")
def download_schedule_csv(
    document_id: str,
    token: str = Query(default=None),
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Download the originally extracted lighting schedule CSV."""
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")

        pipeline_dir = _pipeline_dir_for_user(current_user.id, document_id)
        # Check relevant_tables/ (new layout) then pipeline root
        csv_path = os.path.join(pipeline_dir, "relevant_tables", "light_fixture_schedule.csv")
        validated = StorageService.validate_user_image_path(csv_path, current_user.id) if os.path.isfile(csv_path) else None
        if not validated:
            csv_path = os.path.join(pipeline_dir, "lighting_schedule.csv")
            validated = StorageService.validate_user_image_path(csv_path, current_user.id) if os.path.isfile(csv_path) else None

        if not validated and document.batch_id:
            batch_dir = _batch_pipeline_dir_for_user(current_user.id, document.batch_id)
            csv_path = os.path.join(batch_dir, "relevant_tables", "light_fixture_schedule.csv")
            validated = StorageService.validate_user_image_path(csv_path, current_user.id) if os.path.isfile(csv_path) else None
            if not validated:
                csv_path = os.path.join(batch_dir, "lighting_schedule.csv")
                validated = StorageService.validate_user_image_path(csv_path, current_user.id) if os.path.isfile(csv_path) else None

        if not validated:
            raise HTTPException(status_code=404, detail="Schedule CSV not available")

        return FileResponse(validated, media_type="text/csv", filename=f"lighting_schedule_{document_id}.csv")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error serving schedule CSV: %s\n%s", e, traceback.format_exc())
        raise HTTPException(status_code=500, detail="Failed to serve schedule CSV")


@router.get("/{document_id}/takeoff/docling-outputs")
def get_docling_outputs(
    document_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return paths and status of all Docling extraction outputs."""
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")

        pipeline_dir = _pipeline_dir_for_user(current_user.id, document_id)

        images_dir = os.path.join(pipeline_dir, "images")
        text_path = os.path.join(pipeline_dir, "text", "full_text.txt")
        tables_dir = os.path.join(pipeline_dir, "tables")
        tables_json = os.path.join(pipeline_dir, "tables_all.json")
        schedule_csv = os.path.join(pipeline_dir, "lighting_schedule.csv")

        table_count = 0
        if os.path.isfile(tables_json):
            try:
                with open(tables_json, "r") as f:
                    table_count = len(json.load(f))
            except Exception:
                pass

        image_count = 0
        if os.path.isdir(images_dir):
            image_count = len([f for f in os.listdir(images_dir) if f.endswith(".png")])

        return {
            "document_id": document_id,
            "images_dir": images_dir if os.path.isdir(images_dir) else None,
            "image_count": image_count,
            "text_path": text_path if os.path.isfile(text_path) else None,
            "tables_dir": tables_dir if os.path.isdir(tables_dir) else None,
            "table_count": table_count,
            "tables_json_url": f"/documents/{document_id}/takeoff/tables-json" if os.path.isfile(tables_json) else None,
            "schedule_csv_url": f"/documents/{document_id}/takeoff/schedule-csv" if os.path.isfile(schedule_csv) else None,
            "schedule_found": os.path.isfile(schedule_csv),
            "page_count": document.page_count or 0,
            "engine": "docling" if os.path.isfile(tables_json) else "legacy",
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to retrieve extraction outputs")


@router.get("/{document_id}/takeoff/tables-json")
def download_tables_json(
    document_id: str,
    token: str = Query(default=None),
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Download the Docling-extracted tables as a JSON file."""
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")

        pipeline_dir = _pipeline_dir_for_user(current_user.id, document_id)
        json_path = os.path.join(pipeline_dir, "tables_all.json")

        if not os.path.isfile(json_path):
            raise HTTPException(status_code=404, detail="Tables JSON not available")

        validated = StorageService.validate_user_image_path(json_path, current_user.id)
        if not validated:
            raise HTTPException(status_code=404, detail="Tables JSON not found")

        return FileResponse(validated, media_type="application/json", filename=f"tables_{document_id}.json")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to serve tables JSON")


@router.get("/{document_id}/takeoff/split-pdf")
def download_split_pdf(
    document_id: str,
    token: str = Query(default=None),
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Download the split PDF containing only lighting plan pages."""
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")

        pipeline_dir = _pipeline_dir_for_user(current_user.id, document_id)
        pdf_path = os.path.join(pipeline_dir, "lighting_panel_plans.pdf")
        validated = StorageService.validate_user_image_path(pdf_path, current_user.id) if os.path.isfile(pdf_path) else None

        if not validated and document.batch_id:
            batch_dir = _batch_pipeline_dir_for_user(current_user.id, document.batch_id)
            pdf_path = os.path.join(batch_dir, "lighting_panel_plans.pdf")
            validated = StorageService.validate_user_image_path(pdf_path, current_user.id) if os.path.isfile(pdf_path) else None

        if not validated:
            raise HTTPException(status_code=404, detail="Split PDF not available")

        return FileResponse(validated, media_type="application/pdf", filename=f"lighting_panel_plans_{document_id}.pdf")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to serve split PDF")


@router.post("/{document_id}/takeoff/rerun")
def rerun_takeoff_pipeline(
    document_id: str,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Re-run the autocount pipeline on a completed document."""
    try:
        document = DocumentService.get_user_document(db, document_id, current_user.id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found or access denied")

        if document.file_type == "zip":
            if document.batch_id:
                # ZIP uploaded as part of a batch — rerun batch pipeline
                from processing.document_processor import run_batch_autocount_pipeline
                uid = current_user.id
                doc_batch_id = document.batch_id

                def _rerun_zip_batch():
                    batch_dir = _batch_pipeline_dir_for_user(uid, doc_batch_id)
                    os.makedirs(batch_dir, exist_ok=True)
                    batch_flag = os.path.join(batch_dir, "_running")
                    try:
                        with open(batch_flag, "w") as f:
                            f.write("running")
                        run_batch_autocount_pipeline(doc_batch_id, uid)
                    except Exception as exc:
                        logger.error("Batch pipeline rerun from ZIP failed for %s: %s", doc_batch_id, exc)
                    finally:
                        if os.path.isfile(batch_flag):
                            os.remove(batch_flag)

                background_tasks.add_task(_rerun_zip_batch)
                return {"document_id": document_id, "batch_id": doc_batch_id, "message": "Batch takeoff pipeline rerun started"}
            # else: ZIP uploaded as single file (merged_from_zip.pdf) — fall through to regular pipeline

        if document.status not in ("completed", "failed"):
            raise HTTPException(status_code=400, detail="Document must be processed first")

        from processing.document_processor import _run_autocount_pipeline, run_batch_autocount_pipeline
        all_pages = DocumentService.get_all_pages(db, document_id)
        total_pages = len(all_pages)
        classifications = {p.page_number: {"page_type": p.page_type, "is_relevant": p.is_relevant} for p in all_pages}
        
        uid = current_user.id
        doc_batch_id = document.batch_id

        def _rerun_task():
            pipeline_dir = _pipeline_dir_for_user(uid, document_id)
            os.makedirs(pipeline_dir, exist_ok=True)
            running_flag = os.path.join(pipeline_dir, "_running")
            try:
                with open(running_flag, "w") as f:
                    f.write("running")
                pdf_path = StorageService.locate_pdf(uid, document_id)
                _run_autocount_pipeline(pdf_path, uid, document_id, classifications, total_pages)
            except Exception as exc:
                logger.error("Takeoff rerun failed for %s: %s", document_id, exc)
            finally:
                if os.path.isfile(running_flag):
                    os.remove(running_flag)

            # Rerun the batch pipeline if applicable
            if doc_batch_id:
                batch_dir = _batch_pipeline_dir_for_user(uid, doc_batch_id)
                os.makedirs(batch_dir, exist_ok=True)
                batch_flag = os.path.join(batch_dir, "_running")
                try:
                    with open(batch_flag, "w") as f:
                        f.write("running")
                    run_batch_autocount_pipeline(doc_batch_id, uid)
                except Exception as exc:
                    logger.error("Batch pipeline rerun failed for %s: %s", doc_batch_id, exc)
                finally:
                    if os.path.isfile(batch_flag):
                        os.remove(batch_flag)

        background_tasks.add_task(_rerun_task)
        msg = "Takeoff pipeline rerun started"
        if doc_batch_id:
            msg += f" (batch pipeline will also rerun for batch {doc_batch_id})"
        return {"document_id": document_id, "batch_id": doc_batch_id, "message": msg}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to start takeoff rerun")


# ── Batch-level takeoff routes ────────────────────────────────────────────────

batch_takeoff_router = APIRouter(prefix="/batch", tags=["7. Takeoff"])


def _batch_pipeline_dir_for_user(user_id: int, batch_id: str) -> str:
    """Return the batch pipeline output directory."""
    from config import STORAGE_PATH
    return os.path.join(STORAGE_PATH, str(user_id), f"batch_{batch_id}", "pipeline")


@batch_takeoff_router.get("/{batch_id}/takeoff")
def get_batch_takeoff_results(
    batch_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get cross-document takeoff results for an entire batch."""
    try:
        docs = DocumentService.get_batch_documents(db, batch_id, current_user.id)
        if not docs:
            raise HTTPException(status_code=404, detail="Batch not found or empty")

        document_ids = [d.document_id for d in docs if d.file_type == "pdf"]
        pipeline_dir = _batch_pipeline_dir_for_user(current_user.id, batch_id)

        if not os.path.isdir(pipeline_dir):
            return {
                "batch_id": batch_id,
                "document_ids": document_ids,
                "pipeline_status": "not_run",
            }

        results = _read_pipeline_results(pipeline_dir)

        return {
            "batch_id": batch_id,
            "document_ids": document_ids,
            "fixture_counts": results["fixture_counts"],
            "total_fixtures_found": results["total_found"],
            "available_sheets": results["available_sheets"],
            "matrix_csv_available": results["matrix_csv_available"],
            "schedule_csv_available": results["csv_available"],
            "split_pdf_available": results["split_pdf_available"],
            "pipeline_status": results["status"],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error fetching batch takeoff for %s: %s", batch_id, e)
        raise HTTPException(status_code=500, detail="Failed to retrieve batch takeoff results")


@batch_takeoff_router.get("/{batch_id}/takeoff/overlay")
def serve_batch_overlay_image(
    batch_id: str,
    sheet_id: str = Query(..., description="The sheet ID identifier (e.g., 'E000', 'E101')"),
    token: str = Query(default=None, description="JWT token (for img src requests)"),
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Serve a batch-level takeoff overlay image."""
    try:
        docs = DocumentService.get_batch_documents(db, batch_id, current_user.id)
        if not docs:
            raise HTTPException(status_code=404, detail="Batch not found")

        pipeline_dir = _batch_pipeline_dir_for_user(current_user.id, batch_id)
        image_path = os.path.join(pipeline_dir, "fixture_results", "overlays", f"overlay_{sheet_id}.png")

        validated = StorageService.validate_user_image_path(image_path, current_user.id)
        if not validated:
            raise HTTPException(status_code=404, detail="Overlay image not found")

        return FileResponse(validated, media_type="image/png")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to serve overlay image")


@batch_takeoff_router.get("/{batch_id}/takeoff/matrix-csv")
def download_batch_matrix_csv(
    batch_id: str,
    token: str = Query(default=None),
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Download the batch-level Fixture Takeoff Matrix CSV."""
    try:
        docs = DocumentService.get_batch_documents(db, batch_id, current_user.id)
        if not docs:
            raise HTTPException(status_code=404, detail="Batch not found")

        pipeline_dir = _batch_pipeline_dir_for_user(current_user.id, batch_id)
        csv_path = os.path.join(pipeline_dir, "fixture_results", "fixture_takeoff_matrix.csv")

        validated = StorageService.validate_user_image_path(csv_path, current_user.id)
        if not validated:
            raise HTTPException(status_code=404, detail="Matrix CSV not available")

        return FileResponse(validated, media_type="text/csv", filename=f"fixture_takeoff_matrix_batch_{batch_id}.csv")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to serve matrix CSV")


@batch_takeoff_router.get("/{batch_id}/takeoff/schedule-csv")
def download_batch_schedule_csv(
    batch_id: str,
    token: str = Query(default=None),
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Download the batch-level extracted lighting schedule CSV."""
    try:
        docs = DocumentService.get_batch_documents(db, batch_id, current_user.id)
        if not docs:
            raise HTTPException(status_code=404, detail="Batch not found")

        pipeline_dir = _batch_pipeline_dir_for_user(current_user.id, batch_id)
        csv_path = os.path.join(pipeline_dir, "lighting_schedule.csv")

        validated = StorageService.validate_user_image_path(csv_path, current_user.id)
        if not validated:
            raise HTTPException(status_code=404, detail="Schedule CSV not available")

        return FileResponse(validated, media_type="text/csv", filename=f"lighting_schedule_batch_{batch_id}.csv")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to serve schedule CSV")


@batch_takeoff_router.get("/{batch_id}/takeoff/split-pdf")
def download_batch_split_pdf(
    batch_id: str,
    token: str = Query(default=None),
    current_user: models.User = Depends(get_current_user_flexible),
    db: Session = Depends(get_db),
):
    """Download the batch-level merged split PDF."""
    try:
        docs = DocumentService.get_batch_documents(db, batch_id, current_user.id)
        if not docs:
            raise HTTPException(status_code=404, detail="Batch not found")

        pipeline_dir = _batch_pipeline_dir_for_user(current_user.id, batch_id)
        pdf_path = os.path.join(pipeline_dir, "lighting_panel_plans.pdf")

        validated = StorageService.validate_user_image_path(pdf_path, current_user.id)
        if not validated:
            raise HTTPException(status_code=404, detail="Split PDF not available")

        return FileResponse(validated, media_type="application/pdf", filename=f"lighting_panel_plans_batch_{batch_id}.pdf")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to serve split PDF")


@batch_takeoff_router.post("/{batch_id}/takeoff/rerun")
def rerun_batch_takeoff_pipeline(
    batch_id: str,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Re-run the cross-document batch takeoff pipeline."""
    try:
        docs = DocumentService.get_batch_documents(db, batch_id, current_user.id)
        if not docs:
            raise HTTPException(status_code=404, detail="Batch not found")

        pdf_docs = [d for d in docs if d.file_type == "pdf"]
        if not pdf_docs:
            raise HTTPException(status_code=400, detail="No PDF documents in this batch")

        not_done = [d for d in pdf_docs if d.status not in ("completed", "failed")]
        if not_done:
            raise HTTPException(
                status_code=400,
                detail=f"{len(not_done)} document(s) still processing — wait for completion",
            )

        uid = current_user.id
        from processing.document_processor import run_batch_autocount_pipeline

        def _rerun_batch():
            pipeline_dir = _batch_pipeline_dir_for_user(uid, batch_id)
            os.makedirs(pipeline_dir, exist_ok=True)
            running_flag = os.path.join(pipeline_dir, "_running")
            try:
                with open(running_flag, "w") as f:
                    f.write("running")
                run_batch_autocount_pipeline(batch_id, uid)
            except Exception as exc:
                logger.error("Batch takeoff rerun failed for %s: %s", batch_id, exc)
            finally:
                if os.path.isfile(running_flag):
                    os.remove(running_flag)

        background_tasks.add_task(_rerun_batch)
        return {"batch_id": batch_id, "message": "Batch takeoff pipeline rerun started"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to start batch takeoff rerun")