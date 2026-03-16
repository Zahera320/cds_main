"""
Single-Page Processor
======================
Orchestrates text extraction + image conversion for one PDF page
and returns a plain dict with all data needed for DB storage.

Public API:
    process_single_page(page, page_number, document_id, pages_dir) → dict
    process_page_from_pdf(pdf_path, page_number, document_id, pages_dir) → dict
"""

import logging
from typing import Any, Dict, Optional

import fitz  # PyMuPDF

from .text_extractor import extract_text
from .image_converter import convert_page_to_image

logger = logging.getLogger(__name__)


def process_single_page(
    page: fitz.Page,
    page_number: int,
    document_id: str,
    pages_dir: str,
) -> Dict[str, Any]:
    """
    Process one PDF page end-to-end from an already-loaded page object.

    Args:
        page        : PyMuPDF Page object for this page.
        page_number : 1-based page index.
        document_id : UUID of the parent document (used in log messages).
        pages_dir   : Directory where page images are saved.

    Returns:
        {
            "page_number"    : int,
            "extracted_text" : str,
            "text_length"    : int,
            "ocr_used"       : bool,
            "image_path"     : str | None,
        }
    """
    logger.debug("Processing page %d of document %s", page_number, document_id)

    # ── Text extraction ───────────────────────────────────────────────────────
    extracted_text, ocr_used = extract_text(page)
    text_length = len(extracted_text)

    # ── Image conversion ──────────────────────────────────────────────────────
    image_path: Optional[str] = None
    try:
        image_path = convert_page_to_image(page, pages_dir, page_number)
    except Exception as exc:
        # Image failure is non-fatal: log and continue; image_path stays None.
        logger.warning(
            "Image conversion failed for page %d (doc %s): %s",
            page_number, document_id, exc,
        )

    return {
        "page_number": page_number,
        "extracted_text": extracted_text,
        "text_length": text_length,
        "ocr_used": ocr_used,
        "image_path": image_path,
    }


def process_page_from_pdf(
    pdf_path: str,
    page_number: int,
    document_id: str,
    pages_dir: str,
) -> Dict[str, Any]:
    """
    Self-contained page processor that opens its own PDF handle.

    Designed for parallel execution — each call is fully independent
    and does not share any mutable state with other calls.

    Args:
        pdf_path    : Path to the PDF file on disk.
        page_number : 1-based page index.
        document_id : UUID of the parent document.
        pages_dir   : Directory for rendered page images.

    Returns:
        Same dict as process_single_page().
    """
    pdf_doc = fitz.open(pdf_path)
    try:
        page = pdf_doc.load_page(page_number - 1)
        return process_single_page(
            page=page,
            page_number=page_number,
            document_id=document_id,
            pages_dir=pages_dir,
        )
    finally:
        pdf_doc.close()
