"""
Page Image Conversion Module
=============================
Converts a single PDF page to a high-resolution PNG image.

Default DPI: 300  (recommended for readable page captures)

Public API:
    convert_page_to_image(page, output_dir, page_number, dpi) → image_path: str
"""

import os
import logging

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)

DEFAULT_DPI: int = 150


def _build_image_path(output_dir: str, page_number: int) -> str:
    """Return a zero-padded PNG filename inside output_dir."""
    return os.path.join(output_dir, f"page_{page_number:04d}.png")


def _render_page(page: fitz.Page, dpi: int) -> fitz.Pixmap:
    """
    Render a PDF page to a Pixmap at the given DPI.
    PyMuPDF native DPI is 72, so scale = dpi / 72.
    """
    scale = dpi / 72
    matrix = fitz.Matrix(scale, scale)
    return page.get_pixmap(matrix=matrix, colorspace=fitz.csRGB)


# ── Public API ────────────────────────────────────────────────────────────────

def convert_page_to_image(
    page: fitz.Page,
    output_dir: str,
    page_number: int,
    dpi: int = DEFAULT_DPI,
) -> str:
    """
    Render a PDF page as a PNG and save it to:
        output_dir/page_XXXX.png

    Args:
        page        : PyMuPDF Page object (0-indexed internally, handled by caller).
        output_dir  : Directory where the PNG is saved.
                      Created automatically if it does not exist.
        page_number : 1-based page number used in the output filename.
        dpi         : Rendering resolution (default 300 DPI).

    Returns:
        Absolute path to the saved PNG file.

    Raises:
        OSError / Exception on disk-write failure.
    """
    os.makedirs(output_dir, exist_ok=True)

    image_path = _build_image_path(output_dir, page_number)

    try:
        pixmap = _render_page(page, dpi)
        pixmap.save(image_path)
        pixmap = None  # release pixmap memory immediately
        logger.debug("Page %d image saved → %s", page_number, image_path)
    except Exception as exc:
        logger.error("Failed to save image for page %d: %s", page_number, exc)
        raise

    return image_path
