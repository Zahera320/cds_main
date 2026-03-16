"""
AWS Textract Table Extraction
==============================
Provides table extraction from PDF pages using AWS Textract's AnalyzeDocument
and StartDocumentAnalysis APIs.

Textract returns structured table data (cells with row/column indices) which
this module converts into the same list-of-lists format that the rest of the
pipeline expects (matching Docling's output format).

PDF-to-image rendering is done on CPU via PyMuPDF before sending pages
to Textract.

Public API:
    textract_extract_tables(pdf_path, page_numbers=None)
        → Dict[int, List[List[List[str]]]]
        Maps page_number → list of tables, each table = list of rows (list of cell strings).

    textract_extract_tables_all(pdf_path)
        → (all_tables_data, text_by_page, num_pages)
        Full extraction: returns structured table data, text by page, and page count.
"""

import io
import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import boto3
import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


def _get_textract_client():
    """Create a Textract client using credentials from config."""
    from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME

    kwargs = {"region_name": AWS_REGION_NAME}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY

    return boto3.client("textract", **kwargs)


def _page_to_png_bytes(pdf_path: str, page_number: int, dpi: int = 300) -> bytes:
    """Render a single PDF page to PNG bytes using PyMuPDF (CPU).

    Args:
        pdf_path: Path to the PDF file.
        page_number: 1-based page number.
        dpi: Resolution for rendering (default 300 for Textract quality).

    Returns:
        PNG image as bytes.
    """
    doc = fitz.open(pdf_path)
    try:
        page = doc[page_number - 1]
        # Use a zoom matrix for the desired DPI (72 DPI is default)
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)
        png_bytes = pix.tobytes("png")
        return png_bytes
    finally:
        doc.close()


def _analyze_page_with_textract(
    client, png_bytes: bytes
) -> dict:
    """Call Textract AnalyzeDocument (synchronous) on a single page image.

    Uses the TABLES and FORMS feature types for structured table extraction.
    Synchronous API supports single-page images up to 10MB.

    Returns the raw Textract response.
    """
    response = client.analyze_document(
        Document={"Bytes": png_bytes},
        FeatureTypes=["TABLES"],
    )
    return response


def _parse_textract_tables(response: dict) -> Tuple[List[List[List[str]]], str]:
    """Parse Textract response into structured table data and plain text.

    Returns:
        (tables, text) where:
        - tables: list of tables, each table is a list of rows (list of cell strings)
        - text: full page text extracted by Textract
    """
    blocks = response.get("Blocks", [])

    # Build block ID lookup
    block_map = {b["Id"]: b for b in blocks}

    # Extract full page text from LINE blocks
    lines = []
    for block in blocks:
        if block["BlockType"] == "LINE":
            lines.append(block.get("Text", ""))
    page_text = "\n".join(lines)

    # Find TABLE blocks and extract cell data
    tables = []
    for block in blocks:
        if block["BlockType"] != "TABLE":
            continue

        # Collect all cells for this table
        cells = []
        for rel in block.get("Relationships", []):
            if rel["Type"] == "CHILD":
                for child_id in rel["Ids"]:
                    child = block_map.get(child_id)
                    if child and child["BlockType"] == "CELL":
                        cells.append(child)

        if not cells:
            continue

        # Determine table dimensions
        max_row = max(c.get("RowIndex", 0) for c in cells)
        max_col = max(c.get("ColumnIndex", 0) for c in cells)

        # Build 2D grid
        grid = [["" for _ in range(max_col)] for _ in range(max_row)]

        for cell in cells:
            row_idx = cell.get("RowIndex", 1) - 1
            col_idx = cell.get("ColumnIndex", 1) - 1

            # Get cell text from WORD children
            cell_text_parts = []
            for rel in cell.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    for word_id in rel["Ids"]:
                        word_block = block_map.get(word_id)
                        if word_block and word_block["BlockType"] == "WORD":
                            cell_text_parts.append(word_block.get("Text", ""))
                        elif word_block and word_block["BlockType"] == "SELECTION_ELEMENT":
                            cell_text_parts.append(
                                "X" if word_block.get("SelectionStatus") == "SELECTED" else ""
                            )

            cell_text = " ".join(cell_text_parts).strip()

            if 0 <= row_idx < max_row and 0 <= col_idx < max_col:
                grid[row_idx][col_idx] = cell_text

        # Only keep non-empty tables (at least 2 rows with some content)
        non_empty_rows = sum(1 for row in grid if any(c.strip() for c in row))
        if non_empty_rows >= 2:
            tables.append(grid)

    return tables, page_text


def textract_extract_tables(
    pdf_path: str,
    page_numbers: Optional[List[int]] = None,
) -> Dict[int, List[List[List[str]]]]:
    """Extract tables from specific pages of a PDF using AWS Textract.

    Args:
        pdf_path: Path to the PDF file.
        page_numbers: 1-based page numbers to process. If None, processes all pages.

    Returns:
        Dict mapping page_number → list of tables.
        Each table is a list of rows, each row is a list of cell strings.
    """
    client = _get_textract_client()

    # Determine pages to process
    if page_numbers is None:
        doc = fitz.open(pdf_path)
        page_numbers = list(range(1, doc.page_count + 1))
        doc.close()

    tables_by_page: Dict[int, List[List[List[str]]]] = {}

    for pn in page_numbers:
        try:
            t0 = time.time()
            png_bytes = _page_to_png_bytes(pdf_path, pn)
            response = _analyze_page_with_textract(client, png_bytes)
            tables, _ = _parse_textract_tables(response)

            if tables:
                tables_by_page[pn] = tables
                logger.info(
                    "Textract: page %d — %d tables extracted (%.1fs)",
                    pn, len(tables), time.time() - t0,
                )
            else:
                logger.debug("Textract: page %d — no tables found (%.1fs)", pn, time.time() - t0)

        except Exception as exc:
            logger.warning("Textract: failed to process page %d: %s", pn, exc)

    return tables_by_page


def textract_extract_tables_all(
    pdf_path: str,
) -> Tuple[List[dict], Dict[int, str], int]:
    """Extract all tables and text from every page of a PDF using Textract.

    This is the full-extraction equivalent of DoclingExtractor — it processes
    every page and returns structured data suitable for saving as JSON/CSV.

    Returns:
        (all_tables_data, text_by_page, num_pages) where:
        - all_tables_data: list of table dicts with keys:
            table_index, page_number, csv_file (placeholder), row_count, rows, caption
        - text_by_page: {page_number: extracted_text}
        - num_pages: total number of pages in the PDF
    """
    client = _get_textract_client()

    doc = fitz.open(pdf_path)
    num_pages = doc.page_count
    doc.close()

    all_tables_data = []
    text_by_page: Dict[int, str] = {}
    table_counter = 0

    for pn in range(1, num_pages + 1):
        try:
            t0 = time.time()
            png_bytes = _page_to_png_bytes(pdf_path, pn)
            response = _analyze_page_with_textract(client, png_bytes)
            tables, page_text = _parse_textract_tables(response)

            text_by_page[pn] = page_text

            for table_grid in tables:
                table_counter += 1
                # Convert grid to list-of-lists with string values
                rows = [
                    [str(cell) if cell is not None else "" for cell in row]
                    for row in table_grid
                ]

                all_tables_data.append({
                    "table_index": table_counter,
                    "page_number": pn,
                    "csv_file": f"table_{table_counter:03d}.csv",
                    "row_count": len(rows),
                    "rows": rows,
                    "caption": "",
                })

            elapsed = time.time() - t0
            logger.info(
                "Textract: page %d/%d — %d tables, %d chars text (%.1fs)",
                pn, num_pages, len(tables), len(page_text), elapsed,
            )

        except Exception as exc:
            logger.warning("Textract: failed to process page %d: %s", pn, exc)
            text_by_page[pn] = ""

    logger.info(
        "Textract extraction complete: %d pages, %d tables total",
        num_pages, table_counter,
    )

    return all_tables_data, text_by_page, num_pages
