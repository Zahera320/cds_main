"""
Document Processing Package
============================
Public API:
    process_document_pages(document_id, user_id)  → called by background task
    reclassify_document_pages(document_id, user_id) → reclassify only
    process_page_from_pdf(pdf_path, page_number, document_id, pages_dir) → dict
    classify_all_pages(pdf_path, page_texts, total_pages) → classification dict
"""
from .document_processor import process_document_pages, reclassify_document_pages, run_batch_autocount_pipeline
from .page_processor import process_page_from_pdf
from .page_classifier import classify_all_pages
from .vlm_classifier import vlm_verify_all_pages, is_vlm_available
from .full_extractor import DocumentExtractor
from .docling_extractor import DoclingExtractor, DoclingResult
from .schedule_parser import ScheduleIsolator
from .plan_splitter import PlanSplitter
from .takeoff_generator import TakeoffGenerator

__all__ = [
    "process_document_pages",
    "reclassify_document_pages",
    "run_batch_autocount_pipeline",
    "process_page_from_pdf",
    "classify_all_pages",
    "vlm_verify_all_pages",
    "is_vlm_available",
    "DocumentExtractor",
    "DoclingExtractor",
    "DoclingResult",
    "ScheduleIsolator",
    "PlanSplitter",
    "TakeoffGenerator",
]
