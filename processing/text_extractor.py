"""
Text Extraction Module
======================
Handles two extraction strategies for a single PDF page with intelligent detection:

  1. Native  — reads embedded text directly from the PDF.
  2. OCR     — renders the page as an image and runs pytesseract.

Intelligent OCR Detection:
  - Adaptive thresholds based on page dimensions
  - Text quality and density analysis
  - Better scanned document detection
  - Content pattern validation

Public API:
    extract_text(page)  →  (extracted_text: str, ocr_used: bool)
"""

import logging
import re
from typing import Tuple

import fitz  # PyMuPDF

from config import BASE_OCR_THRESHOLD, MIN_TEXT_DENSITY, MAX_GARBLED_RATIO, OCR_ENHANCED_MODE

logger = logging.getLogger(__name__)


# ── Helper functions for intelligent detection ──────────────────────────────────

def _calculate_adaptive_threshold(page: fitz.Page) -> int:
    """
    Calculate OCR threshold based on page dimensions.
    Larger pages should have more text if they're digital.
    """
    try:
        rect = page.rect
        # Convert points to square inches (72 points = 1 inch)
        page_area_sq_in = (rect.width * rect.height) / (72 * 72)
        
        # Expected minimum characters per square inch for digital documents
        expected_chars = int(page_area_sq_in * MIN_TEXT_DENSITY)
        
        # Use base threshold as minimum, scale up for larger pages
        return max(BASE_OCR_THRESHOLD, expected_chars)
    except Exception:
        return BASE_OCR_THRESHOLD


def _analyze_text_quality(text: str) -> float:
    """
    Analyze text quality to detect garbled/corrupted extraction.
    Returns ratio of potentially garbled characters (0.0 = good, 1.0 = all garbled).
    """
    if not text:
        return 1.0
    
    # Count potentially problematic patterns
    garbled_patterns = [
        r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\xFF]+',  # Control chars and high ASCII
        r'[^\w\s\.,!?;:()\[\]{}"\'-]+',             # Non-standard chars
        r'\b[a-zA-Z]{1}\s[a-zA-Z]{1}\s',            # Single chars with spaces (OCR artifacts)
        r'[0-9]{4,}',                              # Long number sequences (potential OCR errors)
    ]
    
    garbled_count = 0
    for pattern in garbled_patterns:
        matches = re.findall(pattern, text)
        garbled_count += sum(len(match) for match in matches)
    
    return min(1.0, garbled_count / len(text))


def _detect_scanned_document(page: fitz.Page, native_text: str) -> bool:
    """
    Enhanced detection for scanned documents based on multiple factors.
    """
    try:
        # Check text density
        rect = page.rect
        page_area_sq_in = (rect.width * rect.height) / (72 * 72)
        text_density = len(native_text) / page_area_sq_in if page_area_sq_in > 0 else 0
        
        # Check for images on the page (scanned docs often have large images)
        images = page.get_images(full=True)
        has_large_images = len(images) > 0
        
        # Check text quality
        garbled_ratio = _analyze_text_quality(native_text)
        
        # Decision logic
        is_scanned = (
            text_density < MIN_TEXT_DENSITY or  # Very low text density
            garbled_ratio > MAX_GARBLED_RATIO or  # High garbled content
            (has_large_images and len(native_text) < BASE_OCR_THRESHOLD)  # Images + little text
        )
        
        if is_scanned:
            logger.debug(
                "Page %d detected as scanned: density=%.2f, garbled=%.2f, images=%d, text_len=%d",
                page.number, text_density, garbled_ratio, len(images), len(native_text)
            )
        
        return is_scanned
    except Exception as exc:
        logger.warning("Scanned document detection failed (page %d): %s", page.number, exc)
        return len(native_text) < BASE_OCR_THRESHOLD


# ── Strategy 1: Native text ───────────────────────────────────────────────────

def extract_native_text(page: fitz.Page) -> str:
    """
    Read text that is already embedded in the PDF page.
    Returns an empty string on any failure.
    """
    try:
        return page.get_text("text").strip()
    except Exception as exc:
        logger.warning("Native text extraction failed (page %d): %s", page.number, exc)
        return ""


# ── Strategy 2: OCR fallback ──────────────────────────────────────────────────

def apply_ocr(page: fitz.Page, enhanced_processing: bool = False) -> str:
    """
    Render the page to a high-DPI image and run pytesseract OCR on it.
    
    Args:
        page: PyMuPDF page object
        enhanced_processing: Use higher DPI and image preprocessing for difficult pages
    
    Returns:
        Extracted text or empty string on failure
    """
    try:
        import io
        import pytesseract
        from PIL import Image, ImageEnhance, ImageFilter

        # Disable Pillow's decompression bomb check — 300/400 DPI construction
        # drawings can easily exceed the default 178 MP pixel limit.
        Image.MAX_IMAGE_PIXELS = None

        # Use higher DPI for enhanced processing
        dpi = 400 if enhanced_processing else 300
        scale = dpi / 72
        matrix = fitz.Matrix(scale, scale)
        pixmap = page.get_pixmap(matrix=matrix, colorspace=fitz.csRGB)

        # Convert pixmap → raw bytes, then immediately free the pixmap so
        # we don't hold both the uncompressed pixmap AND the PNG bytes in
        # memory at the same time.
        png_bytes = pixmap.tobytes("png")
        pixmap = None  # release ~30-150 MB uncompressed pixmap
        image = Image.open(io.BytesIO(png_bytes))
        png_bytes = None  # release PNG bytes now that PIL has decoded them
        
        # Apply image preprocessing for enhanced processing
        if enhanced_processing:
            try:
                # Convert to grayscale for better OCR
                image = image.convert('L')
                
                # Enhance contrast
                enhancer = ImageEnhance.Contrast(image)
                image = enhancer.enhance(1.5)
                
                # Apply slight sharpening
                image = image.filter(ImageFilter.SHARPEN)
                
                logger.debug("Applied enhanced image preprocessing for page %d", page.number)
            except Exception as prep_exc:
                logger.warning("Image preprocessing failed (page %d): %s", page.number, prep_exc)
        
        # Configure tesseract for better accuracy
        custom_config = r'--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.,!?;:()[]{}"\'-@#$%^&*+=/<>|~` '
        ocr_text = pytesseract.image_to_string(image, config=custom_config).strip()
        
        logger.debug("OCR extracted %d characters from page %d (DPI: %d)", 
                    len(ocr_text), page.number, dpi)
        
        return ocr_text

    except ImportError:
        logger.warning("pytesseract / Pillow not installed — OCR skipped for page %d.", page.number)
        return ""
    except Exception as exc:
        logger.warning("OCR failed (page %d): %s", page.number, exc)
        return ""


# ── Public API ────────────────────────────────────────────────────────────────

def extract_text(page: fitz.Page) -> Tuple[str, bool]:
    """
    Extract the best available text from a PDF page using intelligent detection.

    Enhanced Flow:
        1. Try native text extraction
        2. Calculate adaptive threshold based on page size
        3. Analyze text quality and detect scanned documents
        4. Apply OCR with appropriate settings if needed
        5. Use enhanced OCR processing for difficult pages
        6. Return best result with OCR usage flag

    Returns:
        (text: str, ocr_used: bool)
    """
    logger.debug("Starting text extraction for page %d", page.number)
    
    # Step 1: Try native text extraction
    native_text = extract_native_text(page)
    
    # Step 2: Calculate adaptive threshold
    adaptive_threshold = _calculate_adaptive_threshold(page)
    
    # Step 3: Enhanced scanned document detection
    is_likely_scanned = _detect_scanned_document(page, native_text)
    
    # Step 4: Decision logic
    should_use_ocr = (
        len(native_text) < adaptive_threshold or  # Below adaptive threshold
        is_likely_scanned  # Detected as scanned document
    )
    
    if not should_use_ocr:
        logger.debug(
            "Page %d: using native text (length=%d, threshold=%d, quality=good)",
            page.number, len(native_text), adaptive_threshold
        )
        return native_text, False

    # Step 5: Apply OCR
    logger.debug(
        "Page %d: native text insufficient (length=%d, threshold=%d) → trying OCR",
        page.number, len(native_text), adaptive_threshold
    )
    
    # Try standard OCR first
    ocr_text = apply_ocr(page, enhanced_processing=False)
    
    # If standard OCR fails or produces poor results, try enhanced processing
    # (only when OCR_ENHANCED_MODE is enabled in config)
    if OCR_ENHANCED_MODE and len(ocr_text) < len(native_text) * 0.5:
        logger.debug("Page %d: trying enhanced OCR processing", page.number)
        enhanced_ocr_text = apply_ocr(page, enhanced_processing=True)
        
        # Use enhanced OCR if it's better
        if len(enhanced_ocr_text) > len(ocr_text):
            ocr_text = enhanced_ocr_text
    
    # Step 6: Choose best result
    if len(ocr_text) > len(native_text):
        logger.debug(
            "Page %d: OCR successful (native=%d chars, OCR=%d chars)",
            page.number, len(native_text), len(ocr_text)
        )
        return ocr_text, True
    else:
        # OCR didn't improve results, return native text
        logger.debug(
            "Page %d: OCR did not improve results, using native text (native=%d chars, OCR=%d chars)",
            page.number, len(native_text), len(ocr_text)
        )
        return native_text, False  # Native text was used, so ocr_used is False
