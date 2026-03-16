# """
# Takeoff Generator — Fixture Search, Count & Bounding Box Overlay
# =================================================================
# Reads the extracted lighting schedule CSV to find fixture types, then
# searches the split PDF (lighting plan pages) to count them, draw
# bounding boxes, and output overlay images and a JSON fixture tally.

# Public API:
#     TakeoffGenerator(csv_path, pdf_path, output_dir).generate() → bool
# """

# import json
# import logging
# import re
# from pathlib import Path

# import fitz  # PyMuPDF

# logger = logging.getLogger(__name__)


# def _rects_overlap(a: fitz.Rect, b: fitz.Rect, tolerance: float = 2.0) -> bool:
#     """Return True if two rectangles overlap (with a small tolerance)."""
#     return not (
#         a.x1 + tolerance < b.x0
#         or b.x1 + tolerance < a.x0
#         or a.y1 + tolerance < b.y0
#         or b.y1 + tolerance < a.y0
#     )


# class TakeoffGenerator:
#     """
#     Reads the extracted CSV to find fixture types, then searches the split PDF
#     to count them, draw bounding boxes, and output overlay images and a JSON tally.
#     """

#     def __init__(self, csv_path, pdf_path, output_dir):
#         self.csv_path = Path(csv_path) if csv_path else None
#         self.pdf_path = Path(pdf_path) if pdf_path else None
#         self.output_dir = Path(output_dir)
#         self.output_dir.mkdir(parents=True, exist_ok=True)

#     def _read_fixture_types(self):
#         """Read the CSV and extract fixture type codes using table_extractor."""
#         import pandas as pd
#         from processing.table_extractor import classify_table, extract_fixtures_from_rows, strip_rows_above_header

#         df = pd.read_csv(self.csv_path, header=None)
#         # Convert entire DataFrame to list-of-lists (strings)
#         rows = []
#         for _, row in df.iterrows():
#             rows.append([str(v) if v is not None and str(v) != "nan" else "" for v in row.values])

#         cleaned = strip_rows_above_header(rows)
#         # skip_classification=True because the CSV was already validated
#         # as a Light Fixture Schedule by _identify_schedule / classify_table
#         fixtures = extract_fixtures_from_rows(cleaned, skip_classification=True)
#         fixture_types = [f["code"] for f in fixtures if f["code"]]
#         return fixture_types

#     @staticmethod
#     def _find_exact_matches(page, fixture_code, already_matched):
#         """
#         Find exact word-boundary matches for a fixture code on a page.

#         Uses page.get_text("words") to get individually positioned words,
#         then matches the fixture code as an exact word (not a substring).
#         Regions already matched by a longer code are excluded.

#         Returns list of fitz.Rect for each valid match.
#         """
#         matches = []

#         # Strategy: use search_for to find candidate locations, then
#         # verify each hit is not a substring of a longer word by checking
#         # the surrounding text via get_text("words").
#         candidates = page.search_for(fixture_code)
#         if not candidates:
#             return matches

#         # Get all words on the page with their bounding boxes
#         words = page.get_text("words")  # list of (x0, y0, x1, y1, word, block, line, word_idx)

#         for inst in candidates:
#             # Skip if this region was already matched by a longer fixture code
#             if any(_rects_overlap(inst, prev) for prev in already_matched):
#                 continue

#             # Verify it's an exact word match, not a substring.
#             # Find the word(s) that overlap with this search result.
#             is_exact = False
#             for w in words:
#                 wrect = fitz.Rect(w[:4])
#                 wtext = w[4]
#                 if _rects_overlap(inst, wrect, tolerance=1.0):
#                     # The word at this location should BE the fixture code
#                     # (exact match) or the fixture code followed by a
#                     # non-alphanumeric character (e.g. "A1/5" for "A1").
#                     wtext_clean = wtext.strip()
#                     if wtext_clean == fixture_code:
#                         is_exact = True
#                         break
#                     # Allow codes like "A1/5" where A1 is the fixture on switch 5
#                     if re.match(
#                         r'^' + re.escape(fixture_code) + r'(?:[/\-]\d+)?$',
#                         wtext_clean,
#                     ):
#                         is_exact = True
#                         break

#             if is_exact:
#                 matches.append(inst)

#         return matches

#     def generate(self):
#         if not self.csv_path or not self.csv_path.exists():
#             logger.error("CSV file not found at %s", self.csv_path)
#             return False

#         if not self.pdf_path or not self.pdf_path.exists():
#             logger.error("PDF file not found at %s", self.pdf_path)
#             return False

#         logger.info(
#             "Starting Takeoff Generation using %s and %s...",
#             self.csv_path.name,
#             self.pdf_path.name,
#         )

#         # 1. Read the CSV and extract fixture types
#         try:
#             fixture_types = self._read_fixture_types()
#             logger.info("Target Fixtures found: %s", fixture_types)
#         except ImportError:
#             logger.error("pandas is required for TakeoffGenerator")
#             return False
#         except Exception as e:
#             logger.error("Error reading CSV: %s", e)
#             return False

#         if not fixture_types:
#             logger.warning("No fixture types found in CSV — skipping takeoff")
#             return False

#         # 2. Open the PDF
#         try:
#             doc = fitz.open(str(self.pdf_path))
#         except Exception as e:
#             logger.error("Error opening PDF: %s", e)
#             return False

#         # Sort fixture codes longest-first so that "A1X" is matched
#         # before "A1", preventing "A1" from claiming "A1X" locations.
#         fixture_types_sorted = sorted(fixture_types, key=len, reverse=True)

#         # Dictionary to keep track of counts across all pages
#         fixture_counts = {t: 0 for t in fixture_types}
#         generated_images = []

#         # 3. Search for symbols and draw bounding boxes on EVERY page
#         for page_num in range(len(doc)):
#             page = doc[page_num]

#             # Track matched regions on this page to avoid double-counting
#             page_matched_rects = []

#             for fixture in fixture_types_sorted:
#                 exact_matches = self._find_exact_matches(
#                     page, fixture, page_matched_rects
#                 )

#                 for inst in exact_matches:
#                     fixture_counts[fixture] += 1
#                     current_count = fixture_counts[fixture]
#                     page_matched_rects.append(inst)

#                     # Draw a Red rectangle around the found text
#                     page.draw_rect(inst, color=(1, 0, 0), width=1.5)

#                     # Add the label next to it (e.g., "E1 - 1")
#                     label = f"{fixture} - {current_count}"
#                     page.insert_text(
#                         (inst.x1 + 2, inst.y0 + 5),
#                         label,
#                         color=(0, 0, 1),
#                         fontsize=8,
#                     )

#             # 4. Export the overlayed image for this page
#             output_image_path = (
#                 self.output_dir / f"output_overlay_page_{page_num + 1}.png"
#             )
#             pix = page.get_pixmap(dpi=200)
#             pix.save(str(output_image_path))
#             pix = None  # free pixmap memory
#             generated_images.append(str(output_image_path))
#             logger.info("Saved annotated image: %s", output_image_path.name)

#         doc.close()

#         # 5. Export the total JSON counts
#         output_json_path = self.output_dir / "fixture_counts.json"
#         with open(output_json_path, "w") as json_file:
#             json.dump(fixture_counts, json_file, indent=4)

#         logger.info("Saved fixture counts to: %s", output_json_path.name)
#         logger.info("Takeoff complete — %s", fixture_counts)
#         return True





# import json
# import logging
# import os
# import re
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from pathlib import Path
# import fitz  # PyMuPDF

# logger = logging.getLogger(__name__)

# def _rects_overlap(a: fitz.Rect, b: fitz.Rect, tolerance: float = 2.0) -> bool:
#     return not (a.x1 + tolerance < b.x0 or b.x1 + tolerance < a.x0 or a.y1 + tolerance < b.y0 or b.y1 + tolerance < a.y0)


# def _process_page_task(args):
#     """
#     Worker function — processes a single PDF page in a thread.
#     Each thread opens its own fitz.Document handle (PyMuPDF is thread-safe
#     with separate Document instances), performs fixture search, draws overlays,
#     renders the page to a PNG at 200 DPI, and returns per-page counts.

#     Using ThreadPoolExecutor (not ProcessPoolExecutor) is intentional:
#     PyMuPDF releases the GIL for its C-level rendering, so threads
#     still execute the DPI render concurrently.

#     Returns: (page_num, sheet_id, {fixture_code: count})
#     """
#     pdf_path, page_num, fixture_types_sorted, overlay_dir, provided_sheet_label = args

#     doc = fitz.open(str(pdf_path))
#     page = doc[page_num]

#     # Determine sheet ID — use provided label (from PlanSplitter) when available,
#     # otherwise attempt to detect it from the bottom-right corner text.
#     if provided_sheet_label:
#         sheet_id = provided_sheet_label
#     else:
#         # Determine sheet ID from bottom-right corner text
#         rect = page.rect
#         search_rect = fitz.Rect(rect.width * 0.75, rect.height * 0.75, rect.width, rect.height)
#         text = page.get_textbox(search_rect).strip()
        
#         # 1. Find ALL potential sheet IDs in that corner using word boundaries (\b)
#         potential_ids = re.findall(r'\b[A-Z]{1,3}[-]?\d{1,4}(?:\.\d{1,3})?\b', text)
        
#         sheet_id = f"Page_{page_num + 1}"
#         if potential_ids:
#             # 2. Filter out known fixture codes (prevents mistaking "A1" for the sheet name)
#             valid_ids = [pid for pid in potential_ids if pid not in fixture_types_sorted]
            
#             if valid_ids:
#                 # 3. Prioritize standard Electrical/Lighting prefixes (E, ED, EL, L, A)
#                 electrical_ids = [pid for pid in valid_ids if re.match(r'^(E|ED|EL|EP|L|LT|A)\d', pid, re.IGNORECASE)]
                
#                 if electrical_ids:
#                     # Take the LAST match (the text closest to the absolute bottom right edge)
#                     sheet_id = electrical_ids[-1]
#                 else:
#                     # Fallback: take the very last valid string found
#                     sheet_id = valid_ids[-1]

#     page_matched_rects = []
#     page_fixture_counts = {t: 0 for t in fixture_types_sorted}
#     words = page.get_text("words")

#     for fixture in fixture_types_sorted:
#         candidates = page.search_for(fixture)
#         if not candidates:
#             continue
#         for inst in candidates:
#             # Skip regions already claimed by a longer fixture code
#             if any(
#                 not (inst.x1 + 2 < prev.x0 or prev.x1 + 2 < inst.x0 or
#                      inst.y1 + 2 < prev.y0 or prev.y1 + 2 < inst.y0)
#                 for prev in page_matched_rects
#             ):
#                 continue
#             is_exact = False
#             for w in words:
#                 wx0, wy0, wx1, wy1 = w[0], w[1], w[2], w[3]
#                 if not (inst.x1 + 1 < wx0 or wx1 + 1 < inst.x0 or
#                         inst.y1 + 1 < wy0 or wy1 + 1 < inst.y0):
#                     wclean = w[4].strip()
#                     if wclean == fixture or re.match(
#                         r'^' + re.escape(fixture) + r'(?:[/\-]\d+)?$', wclean
#                     ):
#                         is_exact = True
#                         break
#             if is_exact:
#                 page_fixture_counts[fixture] += 1
#                 page_matched_rects.append(inst)
#                 page.draw_rect(inst, color=(1, 0, 0), width=1.5)
#                 page.insert_text(
#                     (inst.x1 + 2, inst.y0 + 5),
#                     f"{fixture}",
#                     color=(0, 0, 1),
#                     fontsize=8,
#                 )

#     # Render annotated page at 200 DPI and save overlay PNG
#     overlay_path = Path(overlay_dir) / f"overlay_{sheet_id}.png"
#     page.get_pixmap(dpi=200).save(str(overlay_path))
#     doc.close()

#     return page_num, sheet_id, page_fixture_counts


# class TakeoffGenerator:
#     def __init__(self, csv_path, pdf_path, output_dir, page_sheet_labels: dict = None):
#         self.csv_path = Path(csv_path) if csv_path else None
#         self.pdf_path = Path(pdf_path) if pdf_path else None
#         # New Directory Structure logic
#         self.base_output = Path(output_dir) / "fixture_results"
#         self.overlay_dir = self.base_output / "overlays"
#         self.overlay_dir.mkdir(parents=True, exist_ok=True)

#         self.fixture_metadata = {}  # Stores {Code: Description}
#         # Optional {split_page_idx: sheet_label} from PlanSplitter.
#         # When provided, bypasses the bottom-right corner text detection.
#         self.page_sheet_labels: dict = page_sheet_labels or {}

#     def _read_fixture_data(self):
#         """Read CSV and map Codes to Descriptions.
        
#         Handles two CSV formats:
#           1. Structured (VLM-generated): perfectly clean headers.
#           2. Raw (Docling-extracted): messy, requires heuristic parsing.
#         """
#         import pandas as pd
        
#         # 1. Read the CSV to check its headers
#         df = pd.read_csv(self.csv_path)
#         cols = [str(c).strip().lower() for c in df.columns]
        
#         # 2. Check if it's a perfectly clean CSV (like from the VLM Fallback)
#         if "code" in cols and "description" in cols:
#             code_idx = cols.index("code")
#             desc_idx = cols.index("description")
            
#             for _, row in df.iterrows():
#                 code_val = str(row.iloc[code_idx]).strip()
#                 desc_val = str(row.iloc[desc_idx]).strip()
                
#                 # Skip empty or header repetition rows
#                 if code_val and code_val.lower() not in ['nan', '', 'code', 'type', 'mark']:
#                     self.fixture_metadata[code_val] = desc_val if desc_val.lower() != 'nan' else "No Description"
            
#             return list(self.fixture_metadata.keys())

#         # 3. If it's a messy CSV from Docling, pass it to the heuristic table extractor
#         from processing.table_extractor import extract_fixtures_from_rows, strip_rows_above_header
        
#         df_raw = pd.read_csv(self.csv_path, header=None)
#         # Convert to list of lists of strings, removing Pandas 'nan'
#         rows = [[str(v) if pd.notna(v) and str(v) != "nan" else "" for v in row.values] for _, row in df_raw.iterrows()]
        
#         cleaned = strip_rows_above_header(rows)
#         fixtures = extract_fixtures_from_rows(cleaned, skip_classification=True)
        
#         for f in fixtures:
#             c = f["code"].strip()
#             if c and c.lower() not in ['nan', '', 'code', 'type', 'mark']:
#                 self.fixture_metadata[c] = f.get("description", "No Description")
        
#         return list(self.fixture_metadata.keys())

#     def generate(self):
#         if not self.csv_path.exists() or not self.pdf_path.exists():
#             return False

#         fixture_types = self._read_fixture_data()
#         if not fixture_types:
#             return False

#         # Open once just to get page count, then close — workers open their own handles
#         tmp_doc = fitz.open(str(self.pdf_path))
#         num_pages = len(tmp_doc)
#         tmp_doc.close()

#         fixture_types_sorted = sorted(fixture_types, key=len, reverse=True)

#         tasks = [
#             (str(self.pdf_path), page_num, fixture_types_sorted, str(self.overlay_dir),
#              self.page_sheet_labels.get(page_num))
#             for page_num in range(num_pages)
#         ]

#         # Use one thread per CPU core (capped at the number of pages)
#         max_workers = min(os.cpu_count() or 4, num_pages)
#         logger.info(
#             "Processing %d pages with %d parallel threads...",
#             num_pages, max_workers,
#         )

#         results = []

#         with ThreadPoolExecutor(max_workers=max_workers) as executor:
#             future_to_page = {executor.submit(_process_page_task, task): task[1] for task in tasks}
#             for future in as_completed(future_to_page):
#                 page_num = future_to_page[future]
#                 try:
#                     results.append(future.result())
#                 except Exception as exc:
#                     logger.error("Page %d failed in takeoff worker: %s", page_num, exc)

#         # Sort by original page order so sheet list is in document order
#         results.sort(key=lambda r: r[0])

#         # Aggregate per-page counts into the matrix
#         matrix_counts = {t: {} for t in fixture_types}
#         all_sheets = []
#         for _page_num, sheet_id, page_counts in results:
#             if sheet_id not in all_sheets:
#                 all_sheets.append(sheet_id)
#             for fixture, count in page_counts.items():
#                 matrix_counts[fixture][sheet_id] = (
#                     matrix_counts[fixture].get(sheet_id, 0) + count
#                 )

#         logger.info("All pages processed. Building matrix CSV...")

#         # --- EXPORT MATRIX CSV ---
#         import pandas as pd
#         report_data = []
#         for f_code in fixture_types:
#             row = {"TYPE": f_code, "DESCRIPTION": self.fixture_metadata.get(f_code, "")}
#             total = 0
#             for s_id in all_sheets:
#                 count = matrix_counts[f_code].get(s_id, 0)
#                 row[s_id] = count
#                 total += count
#             row["TOTAL"] = total
#             report_data.append(row)

#         pd.DataFrame(report_data).to_csv(
#             self.base_output / "fixture_takeoff_matrix.csv", index=False
#         )

#         # --- EXPORT JSON ---
#         with open(self.base_output / "fixture_counts.json", "w") as f:
#             json.dump(matrix_counts, f, indent=4)

#         logger.info("Takeoff complete — %s", {k: sum(v.values()) for k, v in matrix_counts.items()})
#         return True





import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import fitz  # PyMuPDF
import pandas as pd

logger = logging.getLogger(__name__)

def _rects_overlap(a: fitz.Rect, b: fitz.Rect, tolerance: float = 2.0) -> bool:
    return not (a.x1 + tolerance < b.x0 or b.x1 + tolerance < a.x0 or a.y1 + tolerance < b.y0 or b.y1 + tolerance < a.y0)


def _process_page_task(args):
    """
    Worker function — processes a single PDF page in a thread.
    Each thread opens its own fitz.Document handle, performs fixture search, 
    draws overlays, renders the page to a PNG at 200 DPI, and returns counts
    AND exact bounding box coordinates.
    """
    pdf_path, page_num, fixture_types_sorted, overlay_dir = args

    doc = fitz.open(str(pdf_path))
    page = doc[page_num]

    # Get ALL words on the page immediately. 
    # words format: (x0, y0, x1, y1, "word", block_no, line_no, word_no)
    words = page.get_text("words")

    # 1. SMART SHEET ID EXTRACTION (Geometry-Based)
    sheet_id = f"Page_{page_num + 1}"
    potential_sheets = []
    
    # Regex for standard architectural sheet numbers (e.g., E101, ED-2.1, L300)
    sheet_regex = re.compile(r'^[A-Z]{1,3}[-]?\d{1,4}(?:\.\d{1,3})?[A-Z]?$', re.IGNORECASE)
    
    # Bottom-right corner coordinates of the page
    x_max = page.rect.x1
    y_max = page.rect.y1

    for w in words:
        text_val = w[4].strip()
        # Remove trailing punctuation (like E101. -> E101)
        text_val = re.sub(r'^[.,;:!]+|[.,;:!]+$', '', text_val)
        
        # If it matches the sheet pattern AND is not a fixture code
        if sheet_regex.match(text_val) and text_val not in fixture_types_sorted:
            # Prioritize standard electrical/lighting prefixes
            is_priority = bool(re.match(r'^(E|ED|EL|EP|L|LT|A|P|M)\d', text_val, re.IGNORECASE))
            
            # Save: (Text, x0, y0, Is_Priority)
            potential_sheets.append((text_val, w[0], w[1], is_priority))
                
    if potential_sheets:
        # Sort candidates! 
        # 1st criteria: Priority (True comes before False)
        # 2nd criteria: Distance to bottom-right corner (Smallest distance first)
        potential_sheets.sort(key=lambda item: (
            not item[3], 
            (x_max - item[1])**2 + (y_max - item[2])**2 
        ))
        
        sheet_id = potential_sheets[0][0]
        
    print(f"\n--- PAGE {page_num + 1} SHEET EXTRACTION ---")
    print(f"Top 3 Candidates closest to bottom-right: {[p[0] for p in potential_sheets[:3]]}")
    print(f"FINAL SELECTED SHEET ID: {sheet_id}")
    print("------------------------------------------\n")

    page_matched_rects = []
    page_fixture_counts = {t: 0 for t in fixture_types_sorted}
    page_fixture_instances = []  # NEW: List to store bounding box geometry

    for fixture in fixture_types_sorted:
        candidates = page.search_for(fixture)
        if not candidates:
            continue
            
        for inst in candidates:
            # Skip regions already claimed by a longer fixture code
            if any(
                not (inst.x1 + 2 < prev.x0 or prev.x1 + 2 < inst.x0 or
                     inst.y1 + 2 < prev.y0 or prev.y1 + 2 < inst.y0)
                for prev in page_matched_rects
            ):
                continue
                
            is_exact = False
            for w in words:
                wx0, wy0, wx1, wy1 = w[0], w[1], w[2], w[3]
                # Check overlap
                if not (inst.x1 + 1 < wx0 or wx1 + 1 < inst.x0 or
                        inst.y1 + 1 < wy0 or wy1 + 1 < inst.y0):
                    
                    wclean = w[4].strip()
                    # Exact match OR fixture code followed by a slash/dash (e.g. A1/5)
                    if wclean == fixture or re.match(r'^' + re.escape(fixture) + r'(?:[/\-]\d+)?$', wclean):
                        is_exact = True
                        break
            
            if is_exact:
                page_fixture_counts[fixture] += 1
                page_matched_rects.append(inst)

                # NEW: Save the exact coordinates for the frontend (rounded to 2 decimals)
                page_fixture_instances.append({
                    "id": f"{sheet_id}_{fixture}_{len(page_fixture_instances)}", # Unique ID for React
                    "type": fixture,
                    "x0": round(inst.x0, 2),
                    "y0": round(inst.y0, 2),
                    "x1": round(inst.x1, 2),
                    "y1": round(inst.y1, 2)
                })
                
                # Draw a Red rectangle around the found text
                page.draw_rect(inst, color=(1, 0, 0), width=1.5)
                
                # Add the label next to it
                page.insert_text(
                    (inst.x1 + 2, inst.y0 + 5),
                    f"{fixture}",
                    color=(0, 0, 1),
                    fontsize=8,
                )

    # Render annotated page at 200 DPI and save overlay PNG
    overlay_path = Path(overlay_dir) / f"overlay_{sheet_id}.png"
    page.get_pixmap(dpi=200).save(str(overlay_path))
    doc.close()

    # RETURN the newly collected instances along with everything else
    return page_num, sheet_id, page_fixture_counts, page_fixture_instances


class TakeoffGenerator:
    """
    Generates lighting takeoffs. 
    Outputs:
    - fixture_results/fixture_takeoff_matrix.csv
    - fixture_results/fixture_counts.json
    - fixture_results/fixture_instances.json  <-- NEW (for frontend interaction)
    - fixture_results/overlays/overlay_SHEET.png
    """
    def __init__(self, csv_path, pdf_path, output_dir, page_sheet_labels: dict = None):
        self.csv_path = Path(csv_path) if csv_path else None
        self.pdf_path = Path(pdf_path) if pdf_path else None
        
        # Define the new directory structure
        self.base_output = Path(output_dir) / "fixture_results"
        self.overlay_dir = self.base_output / "overlays"
        
        # Ensure directories exist
        self.overlay_dir.mkdir(parents=True, exist_ok=True)
        self.fixture_metadata = {}  # Stores {Code: Description}
        # Optional {split_page_idx: sheet_label} from PlanSplitter.
        # When provided, bypasses the bottom-right corner text detection.
        self.page_sheet_labels: dict = page_sheet_labels or {}

    def _read_fixture_data(self):
        """Read CSV: Clean empty columns, then map Code and Description."""
        import pandas as pd
        import re

        try:
            # 1. READ CSV: use keep_default_na=False so empty cells are "" instead of NaN floats
            df = pd.read_csv(self.csv_path, header=None, keep_default_na=False).astype(str)
            
            # 2. DATA CLEANING: Remove completely empty columns caused by leading commas
            df = df.apply(lambda x: x.str.strip())
            df = df.loc[:, (df != '').any(axis=0)] # Drop columns where every value is ''
            
            if df.empty or len(df.columns) == 0:
                return []

            # Reset column indices after dropping the empty ones
            df.columns = range(df.columns.size)

            # 3. FORCE ASSUMPTION: The first non-empty column is ALWAYS the Fixture Code
            code_col = 0
            
            # Find which column says "desc" in the first 3 rows
            desc_col = 1
            if len(df.columns) > 1:
                for r_idx in range(min(3, len(df))):
                    row_vals = [str(c).lower() for c in df.iloc[r_idx].values]
                    try:
                        desc_col = next(i for i, cell in enumerate(row_vals) if "desc" in cell)
                        break  # Found it! Stop searching.
                    except StopIteration:
                        continue

            # 4. Extract the data
            for _, row in df.iterrows():
                c = str(row.iloc[code_col]).strip()
                d = str(row.iloc[desc_col]).strip() if len(row.values) > desc_col else "No Description"

                # Docling edge-case: sometimes the last column repeats the pure code. 
                last_val = str(row.iloc[-1]).strip()
                sec_last_val = str(row.iloc[-2]).strip() if len(row.values) >= 2 else ""
                
                alt_code = last_val if last_val and last_val.lower() not in ['nan', ''] else sec_last_val
                
                if alt_code and len(alt_code) <= 6 and " " not in alt_code and (len(c) > 6 or " " in c):
                    c = alt_code

                # Remove stray quotes
                c = re.sub(r'^[\'"]+|[\'"]+$', '', c)
                d = re.sub(r'^[\'"]+|[\'"]+$', '', d)

                # 5. PDF Garbage Filter
                bad_words = ['nan', '', 'type', 'mark', 'label', 'code', 'symbol', 'fixture', 'description', 'letter']
                if c and c.lower() not in bad_words:
                    # A fixture code should never be a massive paragraph
                    if len(c) <= 15: 
                        self.fixture_metadata[c] = d if d else "No Description"

            # 6. HANDLE MERGED CELLS (e.g., "E3 E4" -> Splits into "E3" and "E4")
            final_fixtures = {}
            for code, desc in self.fixture_metadata.items():
                if " " in code and len(code) <= 12:
                    for split_code in code.split():
                        final_fixtures[split_code.strip()] = desc
                else:
                    final_fixtures[code] = desc
            
            self.fixture_metadata = final_fixtures

            return list(self.fixture_metadata.keys())

        except Exception as e:
            print(f"Error reading CSV in TakeoffGenerator: {e}")
            logger.error("Error reading CSV in TakeoffGenerator: %s", e)
            return []

    def generate(self):
        print("\n--- Starting Takeoff Generator ---")
        if not self.csv_path or not self.csv_path.exists():
            print(f"❌ ERROR: Missing CSV file at {self.csv_path}")
            return False
            
        if not self.pdf_path or not self.pdf_path.exists():
            print(f"❌ ERROR: Missing PDF file at {self.pdf_path}")
            return False

        fixture_types = self._read_fixture_data()
        print(f"✅ Found {len(fixture_types)} fixture types: {fixture_types}")
        
        if not fixture_types:
            print("⚠️ WARNING: No fixture types found in CSV — skipping takeoff")
            return False

        # Open once just to get page count, then close
        tmp_doc = fitz.open(str(self.pdf_path))
        num_pages = len(tmp_doc)
        tmp_doc.close()

        # Sort longest-first to prevent 'A1' from matching inside 'AA1'
        fixture_types_sorted = sorted(fixture_types, key=len, reverse=True)

        tasks = [
            (str(self.pdf_path), page_num, fixture_types_sorted, str(self.overlay_dir))
            for page_num in range(num_pages)
        ]

        # Use one thread per CPU core (capped at the number of pages)
        max_workers = min(os.cpu_count() or 4, num_pages)
        print(f"🚀 Processing {num_pages} pages with {max_workers} threads...")

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {executor.submit(_process_page_task, task): task[1] for task in tasks}
            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    print(f"❌ ERROR: Page {page_num} failed in takeoff worker: {exc}")

        # Sort by original page order
        results.sort(key=lambda r: r[0])

        # Aggregate per-page counts into the matrix
        matrix_counts = {t: {} for t in fixture_types}
        all_sheets = []
        all_instances = {}  # NEW: Store all bounding box coordinates
        
        for _page_num, sheet_id, page_counts, page_instances in results:
            if sheet_id not in all_sheets:
                all_sheets.append(sheet_id)
                
            # Initialize or extend the list of instances for this sheet
            if sheet_id not in all_instances:
                all_instances[sheet_id] = []
            all_instances[sheet_id].extend(page_instances)
                
            for fixture, count in page_counts.items():
                matrix_counts[fixture][sheet_id] = matrix_counts[fixture].get(sheet_id, 0) + count

        print("💾 Building matrix CSV and JSONs...")

        # --- EXPORT MATRIX CSV ---
        report_data = []
        for f_code in fixture_types:
            row = {"TYPE": f_code, "DESCRIPTION": self.fixture_metadata.get(f_code, "")}
            total = 0
            for s_id in all_sheets:
                count = matrix_counts[f_code].get(s_id, 0)
                row[s_id] = count
                total += count
            row["TOTAL"] = total
            report_data.append(row)

        pd.DataFrame(report_data).to_csv(self.base_output / "fixture_takeoff_matrix.csv", index=False)

        # --- EXPORT COUNTS JSON ---
        with open(self.base_output / "fixture_counts.json", "w") as f:
            json.dump(matrix_counts, f, indent=4)
            
        # --- EXPORT INSTANCES JSON (FRONTEND BOUNDING BOXES) ---
        with open(self.base_output / "fixture_instances.json", "w") as f:
            json.dump(all_instances, f, indent=4)

        print(f"✅ Takeoff complete! Total fixtures found: {sum(sum(v.values()) for v in matrix_counts.values())}")
        print(f"📁 Files saved in: {self.base_output}\n")
        return True


if __name__ == "__main__":
    # Standard paths for standalone testing
    BASE_OUT = "/home/ubuntu/project/cds_main/app.py/storage/4/56ce83bb-f5e5-450b-a599-5a8206c5d5a3/pipeline" 
    
    generator = TakeoffGenerator(
        f"/home/ubuntu/project/cds_main/app.py/storage/4/56ce83bb-f5e5-450b-a599-5a8206c5d5a3/pipeline/lighting_schedule.csv", 
        f"/home/ubuntu/project/cds_main/app.py/storage/4/56ce83bb-f5e5-450b-a599-5a8206c5d5a3/pipeline/lighting_panel_plans.pdf", 
        BASE_OUT
    )
    generator.generate()