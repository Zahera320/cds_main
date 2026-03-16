"""
Docling-based PDF Extraction Pipeline

For each uploaded PDF this module:
    1. Converts the PDF via Docling (text + table structure recognition)
    2. Saves all page images to a dedicated images folder
    3. Saves full extracted text to a .txt file
    4. Saves every detected table as an individual CSV file
    5. Identifies the Light Fixture Schedule table and saves it separately
    6. Saves a combined text+table file compatible with ScheduleIsolator

Public API:
    DoclingExtractor(pdf_path, output_dir).run()
        → DoclingResult(text_path, table_dir, images_dir, schedule_csv_path,
                        combined_txt_path, table_count, page_count)
"""

import csv
import gc
import io
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.base_models import InputFormat

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------


def _clean_docling_columns(df):
    """Strip hierarchical Docling prefix from DataFrame column names.

    Docling's ``export_to_dataframe`` produces column names like:
        "TABLE CAPTION.NOTES TEXT:.ACTUAL HEADER"
    This function detects the shared prefix and strips it so only the
    real header text remains (e.g. "ACTUAL HEADER").
    """
    cols = [str(c) for c in df.columns]
    if len(cols) < 2:
        df.columns = cols
        return df

    # Only proceed if columns look like hierarchical Docling names
    # (all start with same long text prefix)
    try:
        prefix = os.path.commonprefix(cols)
    except TypeError:
        return df

    if not prefix or len(prefix) < 10:
        # Short or no shared prefix — nothing to strip
        df.columns = cols
        return df

    # Trim prefix to the last '.' boundary to avoid cutting mid-word
    dot_pos = prefix.rfind('.')
    if dot_pos > 0:
        prefix = prefix[:dot_pos + 1]
    else:
        df.columns = cols
        return df  # no '.' separator found — leave as-is

    cleaned = [c[len(prefix):] if c.startswith(prefix) else c for c in cols]
    # Avoid entirely empty headers — fall back to Column_N
    cleaned = [h if h.strip() else f"Column_{i}" for i, h in enumerate(cleaned)]
    df.columns = cleaned
    return df


@dataclass
class DoclingResult:
    """Container for all outputs of a Docling extraction run."""
    text_path: str = ""                    # Full text .txt file
    table_dir: str = ""                    # Directory containing individual table CSVs
    images_dir: str = ""                   # Directory containing page images
    schedule_csv_path: Optional[str] = None  # Light Fixture Schedule CSV (if found)
    combined_txt_path: str = ""            # Combined text+table file (ScheduleIsolator compat)
    table_count: int = 0
    page_count: int = 0
    tables_json_path: str = ""             # JSON with all tables for route serving
    table_files: List[str] = field(default_factory=list)  # Paths to individual CSV files


class DoclingExtractor:
    """
    Wraps Docling to extract text, tables, and images from a PDF.

    Outputs are written to organised sub-directories under *output_dir*:
        output_dir/
            images/          — page_0001.png … page_NNNN.png
            tables/          — table_001.csv … table_NNN.csv
            text/            — full_text.txt
            combined_text_table.txt   (ScheduleIsolator compatible)
            tables_all.json           (JSON tables for route serving)
            lighting_schedule.csv     (if Light Fixture Schedule found)
    """

    def __init__(self, pdf_path: str, output_dir: str):
        self.pdf_path = Path(pdf_path)
        self.output_dir = Path(output_dir)

    # ------------------------------------------------------------------
    #  Main entry point
    # ------------------------------------------------------------------
    def run(self) -> DoclingResult:
        """Run the full extraction pipeline and return paths to all outputs."""
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

        result = DoclingResult()

        # Create output sub-directories
        images_dir = self.output_dir / "images"
        tables_dir = self.output_dir / "tables"
        text_dir = self.output_dir / "text"
        for d in (images_dir, tables_dir, text_dir):
            d.mkdir(parents=True, exist_ok=True)

        result.images_dir = str(images_dir)
        result.table_dir = str(tables_dir)

        # ----- Configure Docling pipeline ---------------------------------
        pipeline_opts = PdfPipelineOptions(
            do_table_structure=True,
            do_ocr=True,
            generate_page_images=True,
            generate_picture_images=False,  # not used downstream, skip to save RAM
            images_scale=1.5,              # reduced from 2.0; still high quality
        )

        converter = DocumentConverter(
            allowed_formats=[InputFormat.PDF],
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts),
            },
        )

        logger.info("Docling: converting %s …", self.pdf_path.name)
        conv_result = converter.convert(str(self.pdf_path))
        doc = conv_result.document
        result.page_count = doc.num_pages()

        # ----- 1. Save page images ----------------------------------------
        self._save_page_images(conv_result, images_dir, result)

        # ----- 2. Save full text ------------------------------------------
        full_text = doc.export_to_text()
        text_path = text_dir / "full_text.txt"
        text_path.write_text(full_text, encoding="utf-8")
        result.text_path = str(text_path)
        logger.info("Docling: full text saved → %s (%d chars)", text_path.name, len(full_text))

        # ----- 3. Save individual tables as CSV + build JSON index --------
        self._save_tables(doc, tables_dir, result)

        # ----- 4. Build combined text+table file (ScheduleIsolator compat) -
        self._build_combined_file(conv_result, doc, result)

        # ----- 5. Identify & save Light Fixture Schedule ------------------
        self._identify_schedule(doc, result)

        logger.info(
            "Docling extraction complete: %d pages, %d tables, schedule=%s",
            result.page_count, result.table_count,
            "found" if result.schedule_csv_path else "not found",
        )

        # Explicitly free the large Docling conversion objects.
        # Everything useful has been saved to disk at this point.
        del conv_result, doc, converter
        gc.collect()

        return result

    # ------------------------------------------------------------------
    #  Page images
    # ------------------------------------------------------------------
    def _save_page_images(self, conv_result, images_dir: Path, result: DoclingResult):
        """Save rendered page images as PNGs, releasing each from RAM immediately."""
        doc = conv_result.document
        saved = 0
        for page_no, page_item in sorted(doc.pages.items()):
            if page_item.image is None:
                continue
            try:
                # page_item.image is an ImageRef — get the actual PIL Image
                image_ref = page_item.image
                pil_img = getattr(image_ref, 'pil_image', None)
                if pil_img is None:
                    # Fallback: maybe it IS a PIL image directly
                    pil_img = image_ref
                img_path = images_dir / f"page_{page_no:04d}.png"
                pil_img.save(str(img_path), "PNG")
                saved += 1
            except Exception as exc:
                logger.warning("Failed to save page %d image: %s", page_no, exc)
            finally:
                # Free the cached PIL image(s) from Docling's internal
                # _image_cache dict to release RAM immediately after saving.
                try:
                    page_item._image_cache.clear()
                except AttributeError:
                    pass
        gc.collect()
        logger.info("Docling: saved %d / %d page images", saved, result.page_count)

    # ------------------------------------------------------------------
    #  Tables → individual CSV + JSON index
    # ------------------------------------------------------------------
    def _save_tables(self, doc, tables_dir: Path, result: DoclingResult):
        """Save every extracted table as a CSV and build a JSON index."""
        all_tables_data = []

        for idx, table_item in enumerate(doc.tables, start=1):
            try:
                df = table_item.export_to_dataframe(doc)
                df = _clean_docling_columns(df)
                csv_name = f"table_{idx:03d}.csv"
                csv_path = tables_dir / csv_name
                df.to_csv(str(csv_path), index=False, encoding="utf-8")
                result.table_files.append(str(csv_path))

                # Determine which page this table is on
                page_no = table_item.prov[0].page_no if table_item.prov else 0

                # Build JSON-serialisable structure (list-of-lists)
                rows = [df.columns.tolist()] + df.values.tolist()
                # Convert numpy types to native Python
                rows = [
                    [str(cell) if cell is not None else "" for cell in row]
                    for row in rows
                ]

                all_tables_data.append({
                    "table_index": idx,
                    "page_number": page_no,
                    "csv_file": csv_name,
                    "row_count": len(rows),
                    "rows": rows,
                    "caption": table_item.caption_text(doc) or "",
                })
            except Exception as exc:
                logger.warning("Failed to save table %d: %s", idx, exc)

        result.table_count = len(all_tables_data)

        # Write JSON index
        json_path = self.output_dir / "tables_all.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_tables_data, f, ensure_ascii=False, indent=2)
        result.tables_json_path = str(json_path)

        logger.info("Docling: saved %d tables", result.table_count)

    # ------------------------------------------------------------------
    #  Combined text+table file (backward compatible with ScheduleIsolator)
    # ------------------------------------------------------------------
    def _build_combined_file(self, conv_result, doc, result: DoclingResult):
        """
        Build a combined text file in the same format as the old
        full_extractor.py so that ScheduleIsolator can parse it unchanged.

        Format:
            === PAGE N ===
            <text>
            TABLE K (M rows)
            ======
            CSV rows
            ======
        """
        combined_path = self.output_dir / f"{self.pdf_path.stem}_text_table.txt"

        # Group tables by page number
        tables_by_page: dict = {}
        for idx, table_item in enumerate(doc.tables, start=1):
            page_no = table_item.prov[0].page_no if table_item.prov else 0
            tables_by_page.setdefault(page_no, []).append((idx, table_item))

        # Group text items by page number
        text_by_page: dict = {}
        for text_item in doc.texts:
            if text_item.prov:
                page_no = text_item.prov[0].page_no
            else:
                page_no = 0
            text_by_page.setdefault(page_no, []).append(text_item.text)

        table_counter = 0
        num_pages = doc.num_pages()

        with open(combined_path, "w", encoding="utf-8") as f:
            for page_no in range(1, num_pages + 1):
                f.write(f"\n{'=' * 60}\n")
                f.write(f"=== PAGE {page_no} ===\n")
                f.write(f"{'=' * 60}\n\n")

                # Page text
                texts = text_by_page.get(page_no, [])
                if texts:
                    f.write("\n".join(texts))
                    f.write("\n\n")

                # Page tables in combined format
                for _, table_item in tables_by_page.get(page_no, []):
                    try:
                        df = table_item.export_to_dataframe(doc)
                        rows = [df.columns.tolist()] + df.values.tolist()
                        rows = [
                            [(str(c) if c is not None else "") for c in row]
                            for row in rows
                        ]
                        if not rows:
                            continue
                        table_counter += 1
                        f.write(f"\nTABLE {table_counter} ({len(rows)} rows)\n")
                        f.write("=" * 60 + "\n")

                        buf = io.StringIO()
                        writer = csv.writer(buf)
                        for row in rows:
                            writer.writerow(row)
                        f.write(buf.getvalue())
                        f.write("=" * 60 + "\n\n")
                    except Exception as exc:
                        logger.warning(
                            "Table serialization failed on page %d: %s",
                            page_no, exc,
                        )

                f.write(f"\n{'─' * 40}\n")

        result.combined_txt_path = str(combined_path)
        logger.info(
            "Docling: combined text+table file → %s (%d tables)",
            combined_path.name, table_counter,
        )

    # ------------------------------------------------------------------
    #  Light Fixture Schedule identification
    # ------------------------------------------------------------------
    def _identify_schedule(self, doc, result: DoclingResult):
        """Find the Light Fixture Schedule table using classify_table.
        
        Falls back to VLM-based extraction when Docling tables don't pass
        keyword classification.
        """
        from processing.table_extractor import classify_table, strip_rows_above_header

        best = None
        best_rows = 0

        for idx, table_item in enumerate(doc.tables, start=1):
            try:
                df = table_item.export_to_dataframe(doc)
                # Build rows as list-of-lists (header + data)
                rows = [df.columns.tolist()] + df.values.tolist()
                rows = [
                    [str(cell) if cell is not None else "" for cell in row]
                    for row in rows
                ]
                cls = classify_table(rows)
                if cls["is_fixture_schedule"]:
                    if len(rows) > best_rows:
                        best = {"table_index": idx, "df": df, "rows": rows}
                        best_rows = len(rows)
            except Exception as exc:
                logger.warning("Schedule check failed for table %d: %s", idx, exc)

        if not best:
            logger.info("Docling: no Light Fixture Schedule found via keyword classification")
            # ── VLM fallback: try Gemini extraction on pages that have tables ─
            try:
                from processing.vlm_classifier import vlm_extract_table, is_vlm_available
                if is_vlm_available():
                    # Collect page numbers that have tables
                    table_pages = set()
                    for table_item in doc.tables:
                        try:
                            prov = table_item.prov
                            if prov:
                                for p in prov:
                                    table_pages.add(p.page_no)
                        except Exception:
                            pass
                    if table_pages:
                        logger.info(
                            "Docling: attempting VLM table extraction on pages %s",
                            sorted(table_pages),
                        )
                        for pn in sorted(table_pages):
                            vlm_data = vlm_extract_table(str(self.pdf_path), pn)
                            if vlm_data and vlm_data.get("fixtures"):
                                import csv as csv_mod
                                schedule_path = self.output_dir / "lighting_schedule.csv"
                                fixtures = vlm_data["fixtures"]
                                # Write structured fixtures as CSV
                                fieldnames = ["code", "description", "fixture_style",
                                              "voltage", "mounting", "lumens",
                                              "cct", "dimming", "max_va"]
                                with open(str(schedule_path), "w", encoding="utf-8", newline="") as f:
                                    w = csv_mod.DictWriter(f, fieldnames=fieldnames)
                                    w.writeheader()
                                    for fix in fixtures:
                                        w.writerow({k: fix.get(k, "") for k in fieldnames})
                                result.schedule_csv_path = str(schedule_path)
                                logger.info(
                                    "Docling: VLM extracted fixture schedule from page %d → %s",
                                    pn, schedule_path.name,
                                )
                                return
            except Exception as exc:
                logger.warning("VLM table extraction fallback failed: %s", exc)
            return

        # Save the schedule CSV (clean bloated Docling column headers first)
        schedule_path = self.output_dir / "lighting_schedule.csv"
        best["df"] = _clean_docling_columns(best["df"])
        best["df"].to_csv(str(schedule_path), index=False, encoding="utf-8")
        result.schedule_csv_path = str(schedule_path)

        logger.info(
            "Docling: Light Fixture Schedule saved → %s (table #%d, %d rows)",
            schedule_path.name, best["table_index"], best_rows,
        )

    # ------------------------------------------------------------------
    #  Helpers
    # ------------------------------------------------------------------
