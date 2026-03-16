"""
AWS Textract-based PDF Table Extraction Pipeline

Extracts tables from PDFs using AWS Textract paid model, providing
superior table structure recognition compared to Docling.

For each uploaded PDF:
    1. Opens PDF page-by-page
    2. Renders pages to high-quality images (CPU)
    3. Sends pages concurrently to AWS Textract via ThreadPoolExecutor
    4. Extracts table structure (rows, columns, cells)
    5. Saves individual tables as CSV files
    6. Builds JSON index for programmatic access
    7. Creates combined text+table file (ScheduleIsolator compatible)

Public API:
    TextractTableExtractor(pdf_path, output_dir).run()
        → TextractExtractorResult(table_dir, tables_json_path, table_count,
                                   table_files, combined_txt_path)
"""

import csv
import gc
import io
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
import fitz  # PyMuPDF for reading PDF pages
from botocore.exceptions import ClientError
from botocore.config import Config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Render configuration
# ---------------------------------------------------------------------------

# DPI for page rendering sent to Textract (300 → best OCR accuracy)
_RENDER_DPI = 300

# Maximum concurrent Textract API calls (I/O bound → high concurrency safe)
_MAX_TEXTRACT_WORKERS = 8

# Textract retry settings for ThrottlingException / transient errors
_RETRY_ATTEMPTS = 4
_RETRY_BACKOFF_BASE = 1.5  # seconds (exponential back-off)


# Textract synchronous API limits:
#   - File size: 10 MB max (use 9.5 MB as safety margin)
#   - Pixel dimensions: 10000 px max on either side
_TEXTRACT_MAX_BYTES = 9_500_000
_TEXTRACT_MAX_PIXELS = 10_000


def _render_page_to_jpeg(pdf_path: str, page_num: int) -> bytes:
    """
    Render a single PDF page to JPEG bytes suitable for AWS Textract.

    Textract synchronous API limits: 10 MB file, 10000 px per side.
    Engineering drawings at 300 DPI on 36"x24" sheets = ~10800x7200 px,
    which exceeds the pixel limit. This function:
      1. Renders at _RENDER_DPI via PyMuPDF.
      2. Downsizes if either dimension exceeds 10000 px.
      3. Encodes as JPEG (much smaller than PNG for drawings).
      4. If still > 9.5 MB, reduces JPEG quality then DPI until it fits.
    """
    from PIL import Image

    Image.MAX_IMAGE_PIXELS = None  # engineering drawings can be very large

    dpi = _RENDER_DPI
    while dpi >= 100:
        zoom = dpi / 72.0
        mat = fitz.Matrix(zoom, zoom)

        with fitz.open(pdf_path) as doc:
            page = doc[page_num]
            pix = page.get_pixmap(matrix=mat, alpha=False, colorspace=fitz.csRGB)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        # Cap pixel dimensions to Textract's 10000 px limit
        w, h = img.size
        if w > _TEXTRACT_MAX_PIXELS or h > _TEXTRACT_MAX_PIXELS:
            scale = _TEXTRACT_MAX_PIXELS / max(w, h)
            new_w, new_h = int(w * scale), int(h * scale)
            img = img.resize((new_w, new_h), Image.LANCZOS)
            logger.info(
                "Page %d: resized %dx%d → %dx%d to fit Textract pixel limit",
                page_num + 1, w, h, new_w, new_h,
            )

        # Encode as JPEG — try decreasing quality until under size limit
        for quality in (90, 80, 70):
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality)
            jpeg_bytes = buf.getvalue()
            if len(jpeg_bytes) <= _TEXTRACT_MAX_BYTES:
                logger.info(
                    "Page %d: %.1f MB JPEG at %d DPI, quality=%d",
                    page_num + 1, len(jpeg_bytes) / 1_000_000, dpi, quality,
                )
                return jpeg_bytes

        logger.info(
            "Page %d at %d DPI still %.1f MB after quality reduction — lowering DPI",
            page_num + 1, dpi, len(jpeg_bytes) / 1_000_000,
        )
        dpi -= 50

    logger.warning(
        "Page %d: could not reduce image below size limit (%.1f MB)",
        page_num + 1, len(jpeg_bytes) / 1_000_000,
    )
    return jpeg_bytes


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _get_textract_client():
    """Create and return AWS Textract client with credentials from config."""
    from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME

    # Use adaptive retry mode for automatic back-off on throttling
    boto_config = Config(
        retries={"max_attempts": _RETRY_ATTEMPTS, "mode": "adaptive"},
        max_pool_connections=_MAX_TEXTRACT_WORKERS + 2,
    )

    return boto3.client(
        "textract",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION_NAME,
        config=boto_config,
    )


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class TextractExtractorResult:
    """Container for all outputs of a Textract extraction run."""
    table_dir: str = ""                    # Directory containing individual table CSVs
    tables_json_path: str = ""             # JSON with all tables for route serving
    table_count: int = 0
    table_files: List[str] = field(default_factory=list)  # Paths to individual CSV files
    combined_txt_path: str = ""            # Combined text+table file (ScheduleIsolator compat)


# ---------------------------------------------------------------------------
# Textract Table Extractor
# ---------------------------------------------------------------------------

class TextractTableExtractor:
    """
    Extracts tables from PDF using AWS Textract API.

    Page images are rendered at _RENDER_DPI on CPU, then dispatched to
    Textract concurrently (_MAX_TEXTRACT_WORKERS parallel threads).

    Outputs are written to organised sub-directories under *output_dir*:
        output_dir/
            tables/          — table_001.csv … table_NNN.csv
            tables_all.json           (JSON tables for route serving)
            combined_text_table.txt   (ScheduleIsolator compatible)
    """

    def __init__(
        self,
        pdf_path: str,
        output_dir: str,
        page_numbers: Optional[List[int]] = None,
    ):
        """
        Args:
            page_numbers: 1-based page numbers to process. When provided, only
                          those pages are rendered and sent to Textract.  If
                          None (default) all pages are processed.
        """
        self.pdf_path = Path(pdf_path)
        self.output_dir = Path(output_dir)
        # Convert to a sorted set of 0-based indices for internal use
        self.page_filter: Optional[set] = (
            {pn - 1 for pn in page_numbers} if page_numbers else None
        )
        self.client = None
        self._counter_lock = threading.Lock()
        self.table_counter = 0

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> TextractExtractorResult:
        """Run the full extraction pipeline and return paths to all outputs."""
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

        result = TextractExtractorResult()

        logger.info(
            "Textract extractor starting — render DPI: %d, workers: %d",
            _RENDER_DPI,
            _MAX_TEXTRACT_WORKERS,
        )

        # Create output sub-directories
        tables_dir = self.output_dir / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)
        result.table_dir = str(tables_dir)

        # Initialize Textract client (with adaptive retry built-in)
        try:
            self.client = _get_textract_client()
        except Exception as exc:
            logger.error("Failed to initialize AWS Textract client: %s", exc)
            raise RuntimeError(f"AWS Textract initialization failed: {exc}")

        # ----- Phase 1: render target pages to PNG (parallelised) -----------
        t0 = time.time()
        page_images: Dict[int, bytes] = {}  # page_num (0-based) → PNG bytes
        with fitz.open(str(self.pdf_path)) as doc:
            page_count = len(doc)

        # Restrict to classified pages when a filter is provided
        pages_to_process = (
            sorted(self.page_filter & set(range(page_count)))
            if self.page_filter is not None
            else list(range(page_count))
        )

        if self.page_filter is not None:
            logger.info(
                "Textract: rendering %d/%d pages (schedule pages only) at %d DPI: %s",
                len(pages_to_process), page_count, _RENDER_DPI,
                [pn + 1 for pn in pages_to_process],
            )
        else:
            logger.info("Textract: rendering %d pages at %d DPI…", page_count, _RENDER_DPI)

        def _render(page_num: int) -> Tuple[int, bytes]:
            return page_num, _render_page_to_jpeg(str(self.pdf_path), page_num)

        with ThreadPoolExecutor(max_workers=min(4, len(pages_to_process) or 1)) as rend_pool:
            futures = {rend_pool.submit(_render, pn): pn for pn in pages_to_process}
            for fut in as_completed(futures):
                pn, img_bytes = fut.result()
                page_images[pn] = img_bytes

        logger.info("Textract: rendering done in %.1fs", time.time() - t0)

        # ----- Phase 2: send target pages to Textract concurrently ---------
        t1 = time.time()
        logger.info(
            "Textract: dispatching %d pages to AWS Textract (%d concurrent)…",
            len(pages_to_process), min(_MAX_TEXTRACT_WORKERS, len(pages_to_process) or 1),
        )

        # Collect (page_num, tables_data_list) results preserving order
        page_results: Dict[int, List[dict]] = {}

        def _process_page(page_num: int) -> Tuple[int, List[dict]]:
            return page_num, self._call_textract_for_page(
                page_num, page_images[page_num], tables_dir
            )

        with ThreadPoolExecutor(
            max_workers=min(_MAX_TEXTRACT_WORKERS, len(pages_to_process) or 1)
        ) as pool:
            futures = {pool.submit(_process_page, pn): pn for pn in pages_to_process}
            for fut in as_completed(futures):
                try:
                    pn, tables_on_page = fut.result()
                    page_results[pn] = tables_on_page
                except Exception as exc:
                    pn = futures[fut]
                    logger.warning("Textract: page %d failed: %s", pn + 1, exc)
                    page_results[pn] = []

        logger.info("Textract: all pages processed in %.1fs", time.time() - t1)

        # Free page image memory
        page_images.clear()
        gc.collect()

        # ----- Phase 3: merge results in page order -----------------------
        all_tables_data: List[dict] = []
        for pn in sorted(page_results):
            all_tables_data.extend(page_results[pn])

        result.table_count = len(all_tables_data)
        result.table_files = [
            str(tables_dir / f"table_{i:03d}.csv")
            for i in range(1, result.table_count + 1)
        ]

        json_path = self.output_dir / "tables_all.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_tables_data, f, ensure_ascii=False, indent=2)
        result.tables_json_path = str(json_path)

        elapsed_total = time.time() - t0
        logger.info(
            "Textract: extraction complete — %d tables from %d/%d pages in %.1fs",
            result.table_count, len(pages_to_process), page_count, elapsed_total,
        )

        gc.collect()

        return result

    # ------------------------------------------------------------------
    # Per-page Textract call (runs inside thread pool)
    # ------------------------------------------------------------------

    def _call_textract_for_page(
        self, page_num: int, image_bytes: bytes, tables_dir: Path
    ) -> List[dict]:
        """
        Send one page image to Textract and return structured table dicts.
        Includes exponential back-off retry on ThrottlingException.
        """
        tables_on_page = []
        t0 = time.time()

        for attempt in range(_RETRY_ATTEMPTS):
            try:
                response = self.client.analyze_document(
                    Document={"Bytes": image_bytes},
                    FeatureTypes=["TABLES"],
                )
                break  # success
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if code == "AccessDeniedException":
                    logger.error("AWS Textract: Access denied — check credentials/permissions.")
                    raise RuntimeError("AWS Textract access denied")
                elif code in ("ThrottlingException", "ProvisionedThroughputExceededException"):
                    wait = _RETRY_BACKOFF_BASE ** attempt
                    logger.warning(
                        "Textract throttled on page %d (attempt %d/%d) — retrying in %.1fs",
                        page_num + 1, attempt + 1, _RETRY_ATTEMPTS, wait,
                    )
                    if attempt < _RETRY_ATTEMPTS - 1:
                        time.sleep(wait)
                        continue
                    raise
                else:
                    logger.warning("Textract error on page %d: %s", page_num + 1, exc)
                    raise
        else:
            return tables_on_page  # all retries exhausted

        tables = self._extract_tables_from_response(response, page_num + 1)

        for table_data in tables:
            with self._counter_lock:
                self.table_counter += 1
                idx = self.table_counter

            csv_name = f"table_{idx:03d}.csv"
            csv_path = tables_dir / csv_name
            rows = table_data["rows"]

            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(rows)

            tables_on_page.append({
                "table_index": idx,
                "page_number": page_num + 1,
                "csv_file": csv_name,
                "row_count": len(rows),
                "rows": [
                    [str(cell) if cell is not None else "" for cell in row]
                    for row in rows
                ],
                "caption": table_data.get("caption", ""),
            })

        logger.info(
            "Textract: page %d — %d tables (%.2fs)",
            page_num + 1, len(tables_on_page), time.time() - t0,
        )
        return tables_on_page

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _extract_tables_from_response(self, response: dict, page_num: int) -> List[dict]:
        """Parse AWS Textract response and extract table structure."""
        tables = []
        block_map: Dict[str, dict] = {}
        relationships: Dict[str, List[str]] = {}

        for block in response.get("Blocks", []):
            block_id = block.get("Id")
            block_map[block_id] = block
            for rel in block.get("Relationships", []):
                if rel.get("Type") == "CHILD":
                    relationships[block_id] = rel.get("Ids", [])

        for block in response.get("Blocks", []):
            if block.get("BlockType") == "TABLE":
                try:
                    table_rows = self._extract_table_rows(block, block_map, relationships)
                    if table_rows and len(table_rows) >= 2:
                        tables.append({"rows": table_rows, "caption": ""})
                except Exception as exc:
                    logger.warning("Failed to extract table structure on page %d: %s", page_num, exc)

        return tables

    def _extract_table_rows(
        self, table_block: dict, block_map: dict, relationships: dict
    ) -> Optional[List[List[str]]]:
        """Extract 2D row/column grid from a Textract TABLE block."""
        table_id = table_block.get("Id")
        cell_block_ids = relationships.get(table_id, [])
        if not cell_block_ids:
            return None

        cells = []
        max_row = max_col = 0

        for cell_id in cell_block_ids:
            cell_block = block_map.get(cell_id)
            if not cell_block or cell_block.get("BlockType") != "CELL":
                continue
            row_idx = cell_block.get("RowIndex", 0)
            col_idx = cell_block.get("ColumnIndex", 0)
            row_span = cell_block.get("RowSpan", 1)
            col_span = cell_block.get("ColumnSpan", 1)
            max_row = max(max_row, row_idx + row_span - 1)
            max_col = max(max_col, col_idx + col_span - 1)
            cell_text = self._extract_cell_text(cell_block, block_map, relationships)
            for r in range(row_idx, row_idx + row_span):
                for c in range(col_idx, col_idx + col_span):
                    cells.append((r, c, cell_text))

        if not cells or max_row < 0 or max_col < 0:
            return None

        rows = [["" for _ in range(max_col + 1)] for _ in range(max_row + 1)]
        for row_idx, col_idx, text in cells:
            if row_idx <= max_row and col_idx <= max_col:
                rows[row_idx][col_idx] = text

        return rows

    def _extract_cell_text(
        self, cell_block: dict, block_map: dict, relationships: dict
    ) -> str:
        """Extract text content from a CELL block via its WORD children."""
        cell_id = cell_block.get("Id")
        child_ids = relationships.get(cell_id, [])
        text_parts = []
        for child_id in child_ids:
            child_block = block_map.get(child_id)
            if not child_block:
                continue
            if child_block.get("BlockType") in ("WORD", "LINE"):
                text_parts.append(child_block.get("Text", ""))
            elif child_block.get("BlockType") == "SELECTION_ELEMENT":
                if child_block.get("SelectionStatus") == "SELECTED":
                    text_parts.append("X")
        return " ".join(text_parts).strip()


# ---------------------------------------------------------------------------
# Backward compatibility wrapper
# ---------------------------------------------------------------------------

def extract_tables_with_textract(pdf_path: str, output_dir: str) -> Dict:
    """
    Extract tables using AWS Textract.

    Returns dict with 'table_count', 'table_files', 'tables_json_path'.
    """
    extractor = TextractTableExtractor(pdf_path, output_dir)
    result = extractor.run()
    return {
        "table_dir": result.table_dir,
        "table_count": result.table_count,
        "table_files": result.table_files,
        "tables_json_path": result.tables_json_path,
        "combined_txt_path": result.combined_txt_path,
    }
