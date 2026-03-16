"""
Document Processor  (Orchestrator)
====================================
Drives full PDF decomposition for one document:

    uploaded → processing → completed  (or failed)

Called as a FastAPI background task after a successful upload.

Uses a thread pool to process multiple pages **in parallel**, which
dramatically speeds up image rendering (300-DPI) and OCR.  Each worker
thread opens its own copy of the PDF so there are no thread-safety issues
with shared PyMuPDF objects.

Public API:
    process_document_pages(document_id, user_id)
"""

import logging
import gc
import ctypes
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import fitz  # PyMuPDF
from sqlalchemy.orm import Session

from config import MAX_WORKERS, USE_AWS_TEXTRACT_FOR_TABLES
from database import SessionLocal
from services.storage_service import StorageService
from services.document_service import DocumentService
from .page_processor import process_page_from_pdf
from .page_classifier import classify_all_pages
from .vlm_classifier import (
    vlm_verify_all_pages, is_vlm_available,
    detect_scanned_pages, vlm_verify_table, vlm_extract_table,
)
from .full_extractor import DocumentExtractor
from .docling_extractor import DoclingExtractor
from .schedule_parser import ScheduleIsolator
from .plan_splitter import PlanSplitter
from .takeoff_generator import TakeoffGenerator
from logging_config import get_document_logger

logger = logging.getLogger(__name__)

# ── Memory helpers ────────────────────────────────────────────────────────────

def _release_memory(label: str = "") -> None:
    """
    Force Python GC and then call malloc_trim(0) to return freed heap
    pages back to the OS.  Without malloc_trim, glibc holds onto freed
    memory indefinitely, making the process RSS stay high even after
    large objects are deleted.
    """
    gc.collect()
    try:
        ctypes.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
        logger.debug("malloc_trim: released freed heap to OS (%s)", label)
    except Exception:
        pass  # Non-Linux or missing libc — skip silently


# ── Public API ────────────────────────────────────────────────────────────────

def process_document_pages(document_id: str, user_id: int) -> None:
    """
    Background task: decompose a PDF into per-page text + images.

    Pipeline:
        1. Open a NEW database session (never reuse the request-scoped one).
        2. Fetch document record, set status → 'processing'.
        3. Locate the PDF on disk  (via StorageService).
        4. Open with PyMuPDF (briefly) to detect page count, then close.
        5. Submit every page to a ThreadPoolExecutor
           (each worker opens its own PDF copy):
              a. Extract text  (native → OCR fallback)
              b. Convert to 300-DPI PNG
        6. Collect results as they complete and persist DocumentPage rows
           using a single batched commit.
        7. Set status → 'completed'  (or 'failed' on hard error).

    Per-page errors are logged and skipped so a single bad page
    does not abort the entire document.
    """
    logger.info("=== Page processing started: document %s ===", document_id)

    # ── Per-document log file ─────────────────────────────────────────────────
    # Attach a FileHandler to the root logger so ALL modules (page_classifier,
    # vlm_classifier, etc.) write to this document's processing.log.
    doc_log_handler, doc_log_path = get_document_logger(user_id, document_id)
    logger.info("Document processing log: %s", doc_log_path)

    # ── Step 1: Create a fresh DB session owned by this background task ───────
    # The request-scoped session is closed by FastAPI before this task runs;
    # always allocate a dedicated session here.
    db: Session = SessionLocal()

    try:
        _run_processing(db, document_id, user_id)
    finally:
        db.close()
        # Remove the per-document handler to avoid handler accumulation
        logging.getLogger().removeHandler(doc_log_handler)
        doc_log_handler.close()


def _run_processing(db: Session, document_id: str, user_id: int) -> None:
    """Inner implementation so the session lifecycle stays in one place."""

    # ── Step 2: Fetch + mark processing ──────────────────────────────────────
    document = DocumentService.get_document(db, document_id)
    if not document:
        logger.error("Document not found in DB: %s", document_id)
        return

    DocumentService.update_status(db, document, "processing")

    try:
        _run_processing_inner(db, document, document_id, user_id)
    except Exception as exc:
        logger.error(
            "Unhandled error in processing pipeline for document %s: %s",
            document_id, exc, exc_info=True,
        )
        # Always recover status so the document isn't stuck as 'processing'
        try:
            DocumentService.update_status(db, document, "failed")
        except Exception:
            pass


def _run_processing_inner(db: Session, document, document_id: str, user_id: int) -> None:
    """Core processing logic — called inside an outer try/except in _run_processing."""

    # ── Step 3: Locate PDF ────────────────────────────────────────────────────
    try:
        pdf_path = StorageService.locate_pdf(user_id, document_id)
        logger.info("PDF located: %s", pdf_path)
    except FileNotFoundError as exc:
        logger.error(str(exc))
        DocumentService.update_status(db, document, "failed")
        return

    # ── Step 4: Detect page count (quick open/close) ─────────────────────────
    try:
        tmp_doc = fitz.open(pdf_path)
        total_pages = tmp_doc.page_count
        tmp_doc.close()
        logger.info("Document %s — total pages: %d", document_id, total_pages)
    except Exception as exc:
        logger.error("Cannot open PDF %s: %s", pdf_path, exc)
        DocumentService.update_status(db, document, "failed")
        return

    # Output directory for page images
    pages_dir = StorageService.pages_dir(user_id, document_id)

    # ── Step 5 + 6: Parallel page processing ─────────────────────────────────
    workers = min(MAX_WORKERS, total_pages)
    pages_saved = 0

    logger.info(
        "Processing %d pages with %d parallel workers",
        total_pages, workers,
    )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all pages at once
        future_to_page = {
            executor.submit(
                process_page_from_pdf, pdf_path, pn, document_id, pages_dir
            ): pn
            for pn in range(1, total_pages + 1)
        }

        # Collect results as they finish (order does not matter for DB)
        page_results = []
        for future in as_completed(future_to_page):
            page_number = future_to_page[future]
            try:
                page_data = future.result()
                page_results.append(page_data)
                pages_saved += 1

                logger.info(
                    "  [%d/%d] text_len=%d | ocr=%s | image=%s",
                    page_number,
                    total_pages,
                    page_data["text_length"],
                    page_data["ocr_used"],
                    "ok" if page_data["image_path"] else "FAILED",
                )

            except Exception as exc:
                logger.error(
                    "  [%d/%d] ERROR — skipping page: %s",
                    page_number, total_pages, exc,
                )

    # Release memory after parallel extraction — images and text buffers
    # from PyMuPDF workers may still be resident.
    _release_memory("post-parallel-extraction")

    # ── Step 6: Classify pages ─────────────────────────────────────────────
    # Build {page_number: extracted_text} map for the classifier.
    page_texts = {
        pr["page_number"]: (pr.get("extracted_text") or "")
        for pr in page_results
    }

    classifications = {}
    try:
        classifications = classify_all_pages(pdf_path, page_texts, total_pages)

        # ── Step 6b: VLM verification (selective) ─────────────────────────
        # Only call VLM on pages that need verification:
        #   - Scanned pages (low text) where rule-based is unreliable
        #   - Pages classified via content scan (lowest confidence)
        #   - Pages classified as OTHER (uncertain)
        # Skip VLM for high-confidence results (sheet_index, title_block)
        from config import VLM_VERIFY
        if VLM_VERIFY and is_vlm_available():
            # Identify pages that need VLM verification
            pages_needing_vlm = set()
            for pn, cls_info in classifications.items():
                source = cls_info.get("confidence_source", "")
                page_type = cls_info.get("page_type", "OTHER")
                text_len = len(page_texts.get(pn, ""))

                # Always verify scanned pages (text < 100 chars)
                if text_len < 100:
                    pages_needing_vlm.add(pn)
                # Verify low-confidence classifications
                elif source in ("full_text", "filename"):
                    pages_needing_vlm.add(pn)
                # Verify uncertain results
                elif page_type == "OTHER":
                    pages_needing_vlm.add(pn)
                # Verify SCHEDULE pages to confirm luminaire vs panel
                elif page_type == "SCHEDULE":
                    pages_needing_vlm.add(pn)

            if pages_needing_vlm:
                logger.info(
                    "VLM verification: %d/%d pages need checking (skipping %d high-confidence)",
                    len(pages_needing_vlm), total_pages,
                    total_pages - len(pages_needing_vlm),
                )
                # Collect existing image paths {page_num: image_path}
                page_image_paths = {
                    pr["page_number"]: pr.get("image_path")
                    for pr in page_results
                    if pr.get("image_path")
                }
                classifications = vlm_verify_all_pages(
                    pdf_path, total_pages, classifications, page_image_paths,
                    pages_to_verify=pages_needing_vlm,
                )
            else:
                logger.info(
                    "VLM verification: all %d pages have high-confidence — skipping VLM",
                    total_pages,
                )
                # Still populate VLM fields as None for consistency
                for pn in classifications:
                    classifications[pn]["vlm_page_type"] = None
                    classifications[pn]["vlm_confidence"] = None
                    classifications[pn]["vlm_agrees"] = None

        # Merge classification data into each page result dict
        for pr in page_results:
            pn = pr["page_number"]
            cls_info = classifications.get(pn, {})
            pr["page_type"]      = cls_info.get("page_type")
            pr["is_relevant"]    = cls_info.get("is_relevant")
            pr["sheet_code"]     = cls_info.get("sheet_code")
            pr["confidence_source"] = cls_info.get("confidence_source")
            pr["vlm_page_type"]  = cls_info.get("vlm_page_type")
            pr["vlm_confidence"] = cls_info.get("vlm_confidence")
            pr["vlm_agrees"]     = cls_info.get("vlm_agrees")
    except Exception as exc:
        # Classification failure is non-fatal — pages are still persisted
        # without type/relevance data.
        logger.warning(
            "Page classification failed for document %s (non-fatal): %s",
            document_id, exc,
        )
        for pr in page_results:
            pr.setdefault("page_type", None)
            pr.setdefault("is_relevant", None)
            pr.setdefault("sheet_code", None)
            pr.setdefault("confidence_source", None)
            pr.setdefault("vlm_page_type", None)
            pr.setdefault("vlm_confidence", None)
            pr.setdefault("vlm_agrees", None)

    # Batch-persist all pages in a single transaction instead of one commit
    # per page, which is dramatically faster and avoids N round-trips.
    try:
        DocumentService.persist_pages_batch(db, document_id, page_results)
    except Exception as exc:
        logger.error(
            "Failed to persist pages for document %s: %s", document_id, exc
        )
        DocumentService.update_status(db, document, "failed")
        return

    # ── Step 7: Mark completed ────────────────────────────────────────────────
    document.page_count = total_pages
    DocumentService.update_status(db, document, "completed")
    logger.info(
        "=== Processing complete: document %s — %d/%d pages saved ===",
        document_id, pages_saved, total_pages,
    )

    # ── Step 7b: Detect scanned pages ─────────────────────────────────────
    scanned_pages = detect_scanned_pages(page_results)
    if scanned_pages:
        logger.info(
            "Scanned pages detected: %s (%d of %d pages)",
            scanned_pages, len(scanned_pages), total_pages,
        )

    # ── Step 8: Autocount Pipeline (non-fatal) ────────────────────────────
    # Run the full text+table extraction → schedule isolation →
    # plan splitting → takeoff generation pipeline.
    try:
        _run_autocount_pipeline(
            pdf_path, user_id, document_id, classifications, total_pages
        )
    except Exception as exc:
        logger.warning(
            "Autocount pipeline failed for document %s (non-fatal): %s",
            document_id, exc,
        )
    finally:
        _release_memory("post-autocount")

    # ── Step 9: Batch Pipeline (if applicable) ────────────────────────────
    # When this document belongs to a batch, check whether ALL sibling
    # documents have finished processing.  If so, kick off the cross-
    # document batch pipeline that merges results.
    if document.batch_id:
        try:
            if DocumentService.all_batch_completed(db, document.batch_id, user_id):
                logger.info(
                    "All batch docs completed — launching batch pipeline for %s",
                    document.batch_id,
                )
                run_batch_autocount_pipeline(document.batch_id, user_id)
        except Exception as exc:
            logger.warning(
                "Batch pipeline trigger failed for batch %s (non-fatal): %s",
                document.batch_id, exc,
            )


def _vlm_table_pipeline(
    pdf_path: str,
    pipeline_dir: str,
    csv_path: str | None,
    classifications: dict,
) -> str | None:
    """
    VLM table verification + extraction fallback.

    1. If Docling found a schedule CSV, ask VLM to *verify* the pages
       that were identified as containing schedule tables.
    2. If no schedule was found (csv_path is None), look for SCHEDULE-
       classified pages and try VLM-based table extraction as a fallback.

    Returns the csv_path (possibly updated) or None.
    """
    import csv as csv_mod

    # ── Case 1: Verify the existing schedule ──────────────────────────────
    if csv_path:
        # Find which pages were classified as SCHEDULE
        schedule_pages = [
            pn for pn, info in classifications.items()
            if info.get("page_type") in ("SCHEDULE", "LIGHTING_SCHEDULE")
        ]
        if schedule_pages:
            # Spot-check the first schedule page to confirm it's a fixture schedule
            pn = schedule_pages[0]
            vlm_result = vlm_verify_table(pdf_path, pn)
            if vlm_result and not vlm_result["has_fixture_schedule"]:
                logger.warning(
                    "VLM says page %d is NOT a Light Fixture Schedule "
                    "(conf=%s) — discarding Docling CSV",
                    pn, vlm_result["confidence"],
                )
                csv_path = None  # fall through to extraction below
            else:
                logger.info(
                    "VLM confirmed page %d has a Light Fixture Schedule",
                    pn,
                )
        return csv_path

    # ── Case 2: No schedule found — try VLM extraction on SCHEDULE pages ──
    schedule_pages = sorted(
        pn for pn, info in classifications.items()
        if info.get("page_type") in ("SCHEDULE", "LIGHTING_SCHEDULE")
    )
    if not schedule_pages:
        logger.info("VLM table pipeline: no SCHEDULE pages to attempt extraction on")
        return None

    logger.info(
        ">>> Step 8b-vlm: Attempting Gemini table extraction on pages %s",
        schedule_pages,
    )

    all_fixtures = []
    for pn in schedule_pages:
        vlm_data = vlm_extract_table(pdf_path, pn)
        if vlm_data and vlm_data.get("fixtures"):
            fixtures = vlm_data["fixtures"]
            all_fixtures.extend(fixtures)
            logger.info(
                "VLM extracted %d fixtures from page %d",
                len(fixtures), pn,
            )

    if all_fixtures:
        # Write structured fixtures as CSV
        fallback_csv = os.path.join(pipeline_dir, "lighting_schedule.csv")
        fieldnames = ["code", "description", "fixture_style",
                      "voltage", "mounting", "lumens",
                      "cct", "dimming", "max_va"]
        with open(fallback_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv_mod.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for fix in all_fixtures:
                writer.writerow({k: fix.get(k, "") for k in fieldnames})

        logger.info(
            "VLM table pipeline complete: %d total fixtures → %s",
            len(all_fixtures), fallback_csv,
        )
        return fallback_csv

    logger.info("VLM table pipeline: no fixture schedule extracted from SCHEDULE pages")
    return None


def _run_autocount_pipeline(
    pdf_path: str,
    user_id: int,
    document_id: str,
    classifications: dict,
    total_pages: int,
) -> None:
    """
    Run the integrated autocount pipeline:
        1. Full text + table extraction → combined text file
        2. Schedule isolation → lighting_schedule.csv
        3. Plan splitting → lighting_panel_plans.pdf
        4. Takeoff generation → overlay images + fixture_counts.json
    """
    logger.info("=== Autocount pipeline started: document %s ===", document_id)

    pipeline_dir = StorageService.pipeline_dir(user_id, document_id)

    # Derive SCHEDULE-classified page numbers from existing classifications so
    # Textract only processes those pages (faster + cheaper).
    schedule_pages = sorted(
        pn for pn, info in (classifications or {}).items()
        if info.get("page_type") in ("SCHEDULE", "LIGHTING_SCHEDULE")
    ) or None  # None → fall back to processing all pages

    if schedule_pages:
        logger.info(
            ">>> Step 8a: Textract targeted at %d SCHEDULE page(s): %s",
            len(schedule_pages), schedule_pages,
        )
    else:
        logger.info(">>> Step 8a: No SCHEDULE pages classified — Textract will scan all pages")

    # Step 8a: Full extraction via Docling or AWS Textract (text + tables + images)
    engine_msg = "AWS Textract" if USE_AWS_TEXTRACT_FOR_TABLES else "Docling"
    logger.info(">>> Step 8a: %s extraction (text, tables, images)...", engine_msg)
    docling_ext = DoclingExtractor(
        pdf_path, pipeline_dir,
        use_aws_textract=USE_AWS_TEXTRACT_FOR_TABLES,
        schedule_page_numbers=schedule_pages,
    )
    docling_result = docling_ext.run()
    extracted_txt_path = docling_result.combined_txt_path

    # Release Docling objects immediately — results are saved to disk
    del docling_ext
    _release_memory("post-docling")

    # Step 8b: Schedule isolation
    # Prefer the Docling-identified schedule; fall back to ScheduleIsolator
    csv_path = docling_result.schedule_csv_path
    if not csv_path and extracted_txt_path:
        logger.info(">>> Step 8b: Docling schedule not found — running ScheduleIsolator...")
        parser = ScheduleIsolator(extracted_txt_path, pipeline_dir)
        csv_path = parser.create_schedule_csv()
    else:
        logger.info(">>> Step 8b: Light Fixture Schedule identified by Docling")

    # Release docling_result now that we've extracted what we need
    del docling_result

    # Step 8b-vlm: VLM table verification + fallback extraction
    # If VLM is available, verify the schedule and fall back to VLM extraction
    # when Docling produced no schedule from SCHEDULE-classified pages.
    if is_vlm_available():
        csv_path = _vlm_table_pipeline(
            pdf_path, pipeline_dir, csv_path, classifications,
        )

    # Step 8c: Plan splitting
    logger.info(">>> Step 8c: Splitting Lighting Panel plans...")
    splitter = PlanSplitter(pdf_path, pipeline_dir)
    split_pdf_path = splitter.extract_panel_pages(
        classified_pages=classifications if classifications else None
    )

    # Step 8d: Takeoff generation
    if csv_path and split_pdf_path:
        logger.info(">>> Step 8d: Generating Bounding Boxes and Counts...")
        takeoff = TakeoffGenerator(csv_path, split_pdf_path, pipeline_dir,
                                   page_sheet_labels=splitter.page_sheet_labels)
        takeoff.generate()
    elif not csv_path and split_pdf_path:
        # Fallback: extract fixture type codes directly from lighting plan pages
        logger.info(">>> Step 8d (fallback): Extracting fixture codes from plan pages...")
        csv_path = _extract_fixture_codes_from_plans(split_pdf_path, pipeline_dir)
        if csv_path:
            logger.info(">>> Step 8d: Generating Bounding Boxes and Counts...")
            takeoff = TakeoffGenerator(csv_path, split_pdf_path, pipeline_dir,
                                       page_sheet_labels=splitter.page_sheet_labels)
            takeoff.generate()
        else:
            logger.warning("Skipped takeoff generation: no fixture codes found on plan pages")
    else:
        logger.warning(
            "Skipped takeoff generation: missing %s",
            "CSV" if not csv_path else "split PDF",
        )

    logger.info("=== Autocount pipeline complete: document %s ===", document_id)

    # Step 8e: Organize outputs into structured subfolders
    _organize_pipeline_outputs(pipeline_dir, csv_path)

    _release_memory("autocount")


# def _organize_pipeline_outputs(pipeline_dir: str, csv_path: str | None) -> None:
#     """
#     Copy key pipeline outputs into structured subfolders for easier consumption.

#     Creates:
#         pipeline/relevant_tables/light_fixture_schedule.csv  — schedule CSV
#         pipeline/fixture_results/fixture_counts.json         — counts JSON
#         pipeline/fixture_results/overlays/overlay_*.png      — overlay images
    
#     Original files are kept in place for backward compatibility.
#     """
#     import shutil

#     # ── relevant_tables/ ──────────────────────────────────────────────────
#     if csv_path and os.path.isfile(csv_path):
#         rel_dir = os.path.join(pipeline_dir, "relevant_tables")
#         os.makedirs(rel_dir, exist_ok=True)
#         dst = os.path.join(rel_dir, "light_fixture_schedule.csv")
#         shutil.copy2(csv_path, dst)
#         logger.info("Organized: schedule CSV → relevant_tables/")

#     # ── fixture_results/ ──────────────────────────────────────────────────
#     counts_src = os.path.join(pipeline_dir, "fixture_counts.json")
#     if os.path.isfile(counts_src):
#         fr_dir = os.path.join(pipeline_dir, "fixture_results")
#         os.makedirs(fr_dir, exist_ok=True)
#         shutil.copy2(counts_src, os.path.join(fr_dir, "fixture_counts.json"))

#         # ── fixture_results/overlays/ ─────────────────────────────────────
#         overlays_dir = os.path.join(fr_dir, "overlays")
#         os.makedirs(overlays_dir, exist_ok=True)
#         for fname in os.listdir(pipeline_dir):
#             if fname.startswith("output_overlay_page_") and fname.endswith(".png"):
#                 shutil.copy2(
#                     os.path.join(pipeline_dir, fname),
#                     os.path.join(overlays_dir, fname),
#                 )
#         logger.info("Organized: fixture_counts + overlays → fixture_results/")

def _organize_pipeline_outputs(pipeline_dir: str, csv_path: str | None) -> None:
    """
    Copy key pipeline outputs into structured subfolders for easier consumption.
    """
    import shutil
    import os

    # ── relevant_tables/ ──────────────────────────────────────────────────
    if csv_path and os.path.isfile(csv_path):
        rel_dir = os.path.join(pipeline_dir, "relevant_tables")
        os.makedirs(rel_dir, exist_ok=True)
        dst = os.path.join(rel_dir, "light_fixture_schedule.csv")
        shutil.copy2(csv_path, dst)
        logger.info("Organized: schedule CSV → relevant_tables/")
# ── Reclassify (no re-extraction) ────────────────────────────────────────────

def reclassify_document_pages(document_id: str, user_id: int) -> None:
    """
    Re-run page classification on an already-processed document.

    Does NOT re-extract text or re-render images — only re-runs the
    classifier on existing extracted text, then re-runs the autocount
    pipeline with the new classification results.
    """
    logger.info("=== Reclassification started: document %s ===", document_id)

    db: Session = SessionLocal()
    try:
        document = DocumentService.get_document(db, document_id)
        if not document:
            logger.error("Document not found in DB: %s", document_id)
            return

        DocumentService.update_status(db, document, "processing")

        try:
            # Locate PDF
            try:
                pdf_path = StorageService.locate_pdf(user_id, document_id)
            except FileNotFoundError as exc:
                logger.error(str(exc))
                DocumentService.update_status(db, document, "failed")
                return

            # Get existing pages
            all_pages = DocumentService.get_all_pages(db, document_id)
            if not all_pages:
                logger.error("No pages found for document %s", document_id)
                DocumentService.update_status(db, document, "failed")
                return

            total_pages = len(all_pages)
            page_texts = {p.page_number: (p.extracted_text or "") for p in all_pages}

            # Re-run classification
            classifications = classify_all_pages(pdf_path, page_texts, total_pages)

            # VLM verification (optional)
            from config import VLM_VERIFY
            if VLM_VERIFY and is_vlm_available():
                page_image_paths = {
                    p.page_number: p.image_path
                    for p in all_pages
                    if p.image_path
                }
                classifications = vlm_verify_all_pages(
                    pdf_path, total_pages, classifications, page_image_paths
                )

            # Update DB with new classification results
            cls_list = []
            for pn, info in classifications.items():
                cls_list.append({
                    "page_number": pn,
                    "page_type": info.get("page_type"),
                    "is_relevant": info.get("is_relevant"),
                })
            DocumentService.update_page_classifications(db, document_id, cls_list)

            # Re-run autocount pipeline with new classifications
            try:
                _run_autocount_pipeline(
                    pdf_path, user_id, document_id, classifications, total_pages
                )
            except Exception as exc:
                logger.warning(
                    "Autocount pipeline failed during reclassification for %s: %s",
                    document_id, exc,
                )

            DocumentService.update_status(db, document, "completed")
            logger.info("=== Reclassification complete: document %s ===", document_id)

        except Exception as exc:
            logger.error(
                "Unhandled error during reclassification for %s: %s",
                document_id, exc, exc_info=True,
            )
            try:
                DocumentService.update_status(db, document, "failed")
            except Exception:
                pass

    finally:
        db.close()


# ── Batch (cross-document) autocount pipeline ────────────────────────────────

def run_batch_autocount_pipeline(batch_id: str, user_id: int) -> None:
    """
    Cross-document batch pipeline: combines text+tables and lighting plan
    pages from ALL PDFs in a batch, then runs schedule isolation and takeoff
    on the merged data.

    Called automatically when the last document in a batch finishes processing,
    or manually via the batch takeoff rerun endpoint.
    """
    import os

    logger.info("=== Batch pipeline started: batch %s ===", batch_id)

    db: Session = SessionLocal()
    try:
        docs = DocumentService.get_batch_documents(db, batch_id, user_id)
        pdf_docs = [d for d in docs if d.file_type == "pdf"]
        if not pdf_docs:
            logger.warning("No PDF documents in batch %s", batch_id)
            return

        batch_dir = StorageService.batch_pipeline_dir(user_id, batch_id)

        # ── Collect per-document PDF paths + classifications ──────────────
        doc_infos = []  # list of (pdf_path, classifications, total_pages)
        for doc in pdf_docs:
            try:
                pdf_path = StorageService.locate_pdf(user_id, doc.document_id)
            except FileNotFoundError:
                logger.warning("PDF not found for doc %s — skipping", doc.document_id)
                continue

            all_pages = DocumentService.get_all_pages(db, doc.document_id)
            classifications = {}
            for p in all_pages:
                classifications[p.page_number] = {
                    "page_type": p.page_type,
                    "is_relevant": p.is_relevant,
                }
            doc_infos.append((pdf_path, classifications, len(all_pages)))

        if not doc_infos:
            logger.warning("No accessible PDFs found for batch %s", batch_id)
            return

        # ── Step 1: Docling extraction for each PDF (parallel) ─────────────
        logger.info(">>> Batch step 1: Docling extraction for all PDFs (parallel)...")
        combined_txt_path = os.path.join(batch_dir, "combined_text_table.txt")
        batch_schedule_csv = None

        def _extract_single_pdf(pdf_path: str) -> tuple:
            """Run extraction on one PDF (Docling or AWS Textract) — designed for parallel execution."""
            docling_ext = DoclingExtractor(pdf_path, batch_dir, use_aws_textract=USE_AWS_TEXTRACT_FOR_TABLES)
            docling_result = docling_ext.run()
            txt_path = docling_result.combined_txt_path
            csv_path = docling_result.schedule_csv_path
            del docling_ext, docling_result
            _release_memory(f"batch-docling-{os.path.basename(pdf_path)}")
            return pdf_path, txt_path, csv_path

        batch_workers = min(MAX_WORKERS, len(doc_infos))
        logger.info("Batch Docling extraction: %d PDFs with %d parallel workers",
                    len(doc_infos), batch_workers)

        extraction_results = []
        with ThreadPoolExecutor(max_workers=batch_workers) as executor:
            futures = {
                executor.submit(_extract_single_pdf, pdf_path): pdf_path
                for pdf_path, _, _ in doc_infos
            }
            for future in as_completed(futures):
                pdf_path = futures[future]
                try:
                    extraction_results.append(future.result())
                except Exception as exc:
                    logger.error("Docling extraction failed for %s: %s",
                                 os.path.basename(pdf_path), exc)

        # Combine results into single text file (sequential — fast I/O)
        with open(combined_txt_path, "w", encoding="utf-8") as out:
            for pdf_path, single_txt, csv_path in sorted(extraction_results, key=lambda x: x[0]):
                if not batch_schedule_csv and csv_path:
                    batch_schedule_csv = csv_path
                out.write(f"\n{'#' * 60}\n")
                out.write(f"# SOURCE: {os.path.basename(pdf_path)}\n")
                out.write(f"{'#' * 60}\n\n")
                if single_txt and os.path.isfile(single_txt):
                    with open(single_txt, "r", encoding="utf-8") as inp:
                        out.write(inp.read())

        # ── Step 2: Schedule isolation from combined text ─────────────────
        logger.info(">>> Batch step 2: Isolating Lighting Schedule...")
        csv_path = batch_schedule_csv
        if not csv_path:
            parser = ScheduleIsolator(combined_txt_path, batch_dir)
            csv_path = parser.create_schedule_csv()

        # ── Step 3: Merge lighting plan pages from ALL PDFs ───────────────
        logger.info(">>> Batch step 3: Merging Lighting Panel pages...")
        split_pdf_path = _merge_lighting_plans(doc_infos, batch_dir)

        # ── Step 4: Takeoff generation on merged data ─────────────────────
        if csv_path and split_pdf_path:
            logger.info(">>> Batch step 4: Generating Takeoff...")
            takeoff = TakeoffGenerator(csv_path, split_pdf_path, batch_dir)
            takeoff.generate()
        elif not csv_path and split_pdf_path:
            # Fallback: extract fixture type codes from merged lighting plan pages
            logger.info(">>> Batch step 4 (fallback): Extracting fixture codes from plan pages...")
            csv_path = _extract_fixture_codes_from_plans(split_pdf_path, batch_dir)
            if csv_path:
                logger.info(">>> Batch step 4: Generating Takeoff...")
                takeoff = TakeoffGenerator(csv_path, split_pdf_path, batch_dir)
                takeoff.generate()
            else:
                logger.warning("Skipped batch takeoff: no fixture codes found on plan pages")
        else:
            logger.warning(
                "Skipped batch takeoff: missing %s",
                "CSV" if not csv_path else "merged split PDF",
            )

        logger.info("=== Batch pipeline complete: batch %s ===", batch_id)
        _release_memory("batch")
    except Exception as exc:
        logger.error(
            "Batch pipeline failed for batch %s: %s", batch_id, exc,
        )
    finally:
        db.close()


def _merge_lighting_plans(doc_infos, output_dir):
    """Merge LIGHTING_PLAN pages from multiple PDFs into one split PDF.

    Args:
        doc_infos: list of (pdf_path, classifications, total_pages)
        output_dir: directory where the merged PDF will be written

    Returns:
        Path to the merged PDF, or None if no lighting plan pages found.
    """
    import os

    merged = fitz.open()
    found = 0

    for pdf_path, classifications, _ in doc_infos:
        try:
            doc = fitz.open(pdf_path)
        except Exception:
            continue

        # Use classifications if available, else text-based fallback
        if classifications:
            for pn, info in sorted(classifications.items()):
                if info.get("page_type") == "LIGHTING_PLAN":
                    merged.insert_pdf(doc, from_page=pn - 1, to_page=pn - 1)
                    found += 1
        else:
            import re
            patterns = [
                re.compile(r"lighting\s+plan", re.IGNORECASE),
                re.compile(r"lighting\s+layout", re.IGNORECASE),
                re.compile(r"electrical\s+lighting", re.IGNORECASE),
            ]
            for page_idx in range(len(doc)):
                text = doc[page_idx].get_text("text")
                if any(p.search(text) for p in patterns):
                    merged.insert_pdf(doc, from_page=page_idx, to_page=page_idx)
                    found += 1

        doc.close()

    if found == 0:
        merged.close()
        logger.warning("No lighting plan pages found across batch documents")
        return None

    out_path = os.path.join(output_dir, "lighting_panel_plans.pdf")
    merged.save(out_path)
    merged.close()
    logger.info("Merged %d lighting plan pages into %s", found, out_path)
    return out_path


# ── Fallback fixture code extraction ─────────────────────────────────────────

# Fixture code pattern: 1-4 uppercase letters + 1-2 digits, with a /digit or
# -digit suffix that indicates circuit/switch (e.g. "G1/5" → type "G1").
_FIXTURE_CODE_RE = re.compile(
    r'\b([A-Z]{1,4}\d{1,2})(?:[/-]\d+[A-Z]?)\b'
)

# Common construction abbreviations that are NOT fixture types.
_NOT_FIXTURE_CODES = frozenset({
    "CD", "NO", "EP", "MN", "OS", "FS", "DT",
    "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9",
    "P1", "P2", "U1", "U2", "S3", "N1",
    "DT1", "DT2", "A3", "B2",
})


def _extract_fixture_codes_from_plans(split_pdf_path: str, output_dir: str) -> str | None:
    """Fallback: scan the lighting plan PDF for fixture type codes.

    When no formal Lighting Schedule table exists, fixture types can still
    be identified from the plan pages themselves.  Codes like "G1/5" are
    parsed as fixture type "G1" on switch 5.

    Returns the path to a generated lighting_schedule.csv, or None if no
    codes were found.
    """
    try:
        doc = fitz.open(split_pdf_path)
    except Exception as exc:
        logger.warning("Cannot open split PDF for fixture extraction: %s", exc)
        return None

    all_codes: set[str] = set()
    try:
        for page_idx in range(len(doc)):
            text = doc[page_idx].get_text("text")
            codes = set(_FIXTURE_CODE_RE.findall(text))
            all_codes.update(codes)
    finally:
        doc.close()

    # Remove known non-fixture abbreviations
    all_codes -= _NOT_FIXTURE_CODES

    if not all_codes:
        logger.warning("No fixture codes detected on lighting plan pages")
        return None

    sorted_codes = sorted(all_codes)
    logger.info("Fixture codes extracted from plans (fallback): %s", sorted_codes)

    # Write a minimal CSV that the TakeoffGenerator can parse
    csv_path = os.path.join(output_dir, "lighting_schedule.csv")
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("TYPE\n")
        for code in sorted_codes:
            f.write(f"{code}\n")

    return csv_path
