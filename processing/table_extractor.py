"""
Table Extractor — Luminaire Fixture Extraction Pipeline
========================================================
Extracts structured fixture records (code, description, voltage, mounting,
lumens, CCT, dimming, VA) from luminaire schedule tables in PDF pages.

Eight-layer pipeline:
    Layer 1 — Docling-based table detection
    Layer 2 — Text-based table detection   (fallback)
    Layer 3 — Table filtering  (luminaire schedule vs panel schedule)
    Layer 4 — Header row detection & column mapping
    Layer 5 — Row parsing & embedded data recovery
    Layer 6 — Post-parse panel schedule rejection
    Layer 7 — VLM vision fallback  (Claude API)
    Layer 8 — Combo page handling  (schedule on lighting plan pages)

Public API:
    extract_fixtures(pdf_path, schedule_pages, plan_pages, plan_codes)
        → List[FixtureRecord]
"""

import base64
import json
import logging
import os
import re
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  Data Classes
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class FixtureRecord:
    """A single luminaire fixture extracted from a schedule table.

    Standard fields are mapped from known column aliases for backwards compatibility.
    The `raw_data` dict contains ALL columns from the original table with their
    original header names as keys - this enables fully dynamic extraction.
    """
    code: str = ""
    description: str = ""
    mounting: str = ""
    fixture_style: str = ""
    voltage: str = ""
    lumens: str = ""
    cct: str = ""
    dimming: str = ""
    max_va: str = ""
    # Dynamic storage: ALL columns from original table {header_name: cell_value}
    raw_data: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        # Ensure raw_data is included (asdict handles it, but be explicit)
        return d


# ══════════════════════════════════════════════════════════════════════════════
#  Constants — Column Aliases  (checked in priority order)
# ══════════════════════════════════════════════════════════════════════════════

# Each entry: (field_name, [(alias, is_short_pattern), ...])
# # Short patterns (<=3 chars) require exact match; longer ones use substring.
# _COLUMN_ALIASES: List[Tuple[str, List[Tuple[str, bool]]]] = [
#     ("code", [
#         ("fixture id", False), ("id", True), ("mark", True),
#         ("fixture type", False), ("fixture letter", False),
#         ("type", False), ("fixture", False),
#         ("symbol", False), ("designation", False),
#     ]),
#     ("description", [
#         ("fixture description", False), ("luminaire type", False),
#         ("description", False), ("desc", False),
#     ]),
#     ("mounting", [
#         ("mounting type", False), ("mounting style", False),
#         ("mounting", False), ("mount", False), ("mtg", True),
#     ]),
#     ("fixture_style", [
#         ("fixture style", False), ("fixture type", False),
#         ("style", False), ("catalog", False),
#     ]),
#     ("voltage", [
#         ("voltage", False), ("volts", False), ("v", True),
#     ]),
#     ("lumens", [
#         ("rated lumen", False), ("lumen output", False),
#         ("light output", False), ("lumens", False),
#         ("lumen/watts", False), ("lum", True), ("lamps", False),
#     ]),
#     ("cct", [
#         ("color temperature", False), ("color temp", False),
#         ("color", False), ("cct", True), ("kelvin", False),
#     ]),
#     ("dimming", [
#         ("ballast/driver", False), ("ballast driver", False),
#         ("dimming", False), ("dim", True), ("driver", False),
#         ("ballast", False),
#     ]),
#     ("max_va", [
#         ("max va", False), ("va", True), ("watts", False),
#         ("wattage", False), ("input watts", False),
#         ("power", False), ("watt", False),
#     ]),
# ]


# ══════════════════════════════════════════════════════════════════════════════
#  Constants — Column Aliases  (checked in priority order)
# ══════════════════════════════════════════════════════════════════════════════

_COLUMN_ALIASES: List[Tuple[str, List[Tuple[str, bool]]]] = [
    ("code", [
        ("fixture id", False), ("id", True), ("mark", True),
        ("fixture type", False), ("fixture letter", False),
        ("type", True), ("fixture", True),  # <-- Changed to True (Exact Match)
        ("symbol", False), ("designation", False),
    ]),
    ("description", [
        ("fixture description", False), ("luminaire type", False),
        ("description", False), ("desc", False),
    ]),
    ("mounting", [
        ("mounting type", False), ("mounting style", False),
        ("mounting", False), ("mount", False), ("mtg", True),
    ]),
    ("fixture_style", [
        ("fixture style", False), ("fixture type", False),
        ("style", False), ("catalog", False),
    ]),
    ("voltage", [
        ("voltage", False), ("volts", False), ("volt", False), ("v", True), # <-- Added "volt"
    ]),
    ("lumens", [
        ("rated lumen", False), ("lumen output", False),
        ("light output", False), ("lumens", False),
        ("lumen/watts", False), ("lum", True), ("lamps", False),
    ]),
    ("cct", [
        ("color temperature", False), ("color temp", False),
        ("color", True), ("cct", True), ("kelvin", False), # <-- Changed to True
    ]),
    ("dimming", [
        ("ballast/driver", False), ("ballast driver", False),
        ("dimming", False), ("dim", True), ("driver", False),
        ("ballast", False),
    ]),
    ("max_va", [
        ("max va", False), ("va", True), ("watts", False),
        ("wattage", False), ("input watts", False),
        ("power", True), ("watt", False), # <-- Changed to True
    ]),
]


# Additional negative keywords for broader table classification
_BROAD_NEGATIVE_KEYWORDS = [
    "electrical abbreviation", "abbreviation",
    "sheet index", "drawing index", "drawing list",
    "receptacle type legend", "symbol legend",
    "project no", "issue date",
    "motor schedule", "motor no",
    "load classification", "connected load", "demand factor",
    "estimated demand", "panel totals",
    "existing panel",
    "sub-metering", "virtual meter", "feeder schedule",
    "bus rating",
    "box type", "conduit", "cover color", "telecom bracket"  # <-- Added Floor Box headers
]





# ══════════════════════════════════════════════════════════════════════════════
#  Constants — Table Filtering Keywords (Layer 3)
# ══════════════════════════════════════════════════════════════════════════════

_POSITIVE_KEYWORDS = [
    "luminaire schedule",
    "light fixture schedule",
    "lighting fixture schedule",
    "luminaire fixture schedule",
]

# "fixture schedule" alone is ambiguous — could be floorbox, device, etc.
# It's handled separately with column-header validation.
_AMBIGUOUS_KEYWORDS = [
    "fixture schedule", 
    
]

_NEGATIVE_KEYWORDS = [
    "panel schedule", "panelboard", "branch panel",
    "motor schedule", "equipment schedule", "equipment connection",
    "circuit description", "breaker schedule", "breaker function",
    "connected load", "panel totals", "energy compliance",
    "lighting control", "control device", "override switch",
    "lighting control schedule", "lighting control summary",
    "lighting control panel",
    "floorbox", "floor box", "poke thru", "poke-thru",
    "receptacle",
    "communications description",
    "motor no", "motor number",
    "sub-metering schedule", "sub metering",
    "virtual meter", "feeder schedule",
    "symbol legend", "receptacle type legend",
    "lightning fixture",
    "interior lighting",        # energy compliance / control pages
    "lighting compliance",      # comcheck-style compliance tables
    "compliance statement",     # energy code statements
    "auto partial on",          # lighting control action descriptions
    "comcheck",                 # energy code compliance software
]

_PANEL_LABEL_RE = re.compile(r"panel\s+[a-z0-9]", re.IGNORECASE)

# Header keywords that should NOT be treated as fixture codes
_HEADER_KEYWORDS = set()
for _field_name, _aliases in _COLUMN_ALIASES:
    for _alias, _ in _aliases:
        _HEADER_KEYWORDS.add(_alias.lower())
# Add common non-fixture words
_HEADER_KEYWORDS.update({
    "schedules", "luminaire", "schedule", "lighting",
    "fixture", "description", "type", "style",
    "notes", "remarks", "general", "total",
    "quantity", "location", "room", "area",
})


# ══════════════════════════════════════════════════════════════════════════════
#  Layer 1 & 2 — Table Detection
# ══════════════════════════════════════════════════════════════════════════════

def _clean_cell(val) -> str:
    """Clean a single cell value: None → '', collapse whitespace, strip."""
    if val is None:
        return ""
    s = str(val)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _clean_table(raw_table: list) -> list:
    """Clean all cells in a 2D table grid."""
    return [[_clean_cell(cell) for cell in row] for row in raw_table]


def _is_empty_table(table: list) -> bool:
    """True if every cell in the table is empty."""
    return all(
        all(c == "" for c in row)
        for row in table
    )


def _docling_tables_for_pages(
    pdf_path: str,
    page_numbers: List[int],
) -> Dict[int, List[list]]:
    """
    Layers 1-2: Use Docling to extract tables from the specified pages.

    Returns {page_number: [cleaned_2d_grid, ...]}.
    """
    from collections import defaultdict

    page_set = set(page_numbers)
    tables_by_page: Dict[int, List[list]] = defaultdict(list)

    pipeline_opts = PdfPipelineOptions(
        do_table_structure=True,
        do_ocr=False,
    )
    converter = DocumentConverter(
        allowed_formats=[InputFormat.PDF],
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts),
        },
    )

    conv_result = converter.convert(pdf_path)
    doc = conv_result.document

    for table_item in doc.tables:
        page_no = table_item.prov[0].page_no if table_item.prov else 0
        if page_no not in page_set:
            continue
        try:
            df = table_item.export_to_dataframe(doc)
            rows = [df.columns.tolist()] + df.values.tolist()
            cleaned = [
                [str(c).strip() if c is not None else "" for c in row]
                for row in rows
            ]
            if not _is_empty_table(cleaned) and len(cleaned) >= 2:
                tables_by_page[page_no].append(cleaned)
        except Exception as exc:
            logger.debug("Failed to convert Docling table on page %d: %s", page_no, exc)

    return dict(tables_by_page)


def _aws_textract_tables_for_pages(
    pdf_path: str,
    page_numbers: List[int],
) -> Dict[int, List[list]]:
    """
    Layer 1 Alternative: Use AWS Textract to extract tables from specified pages.

    Returns {page_number: [cleaned_2d_grid, ...]}.

    AWS Textract provides superior table structure recognition for complex
    lighting fixture schedules and panel layouts.
    """
    from collections import defaultdict
    from processing.aws_textract_extractor import TextractTableExtractor
    import tempfile

    page_set = set(page_numbers)
    tables_by_page: Dict[int, List[list]] = defaultdict(list)

    try:
        # Create temporary directory for Textract output
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info("AWS Textract: extracting tables from %d pages", len(page_numbers))
            extractor = TextractTableExtractor(pdf_path, temp_dir)
            result = extractor.run()

            # Load tables from JSON
            if result.tables_json_path and os.path.isfile(result.tables_json_path):
                try:
                    with open(result.tables_json_path, "r", encoding="utf-8") as f:
                        all_tables = json.load(f)

                    for tbl_entry in all_tables:
                        page_no = tbl_entry.get("page_number", 0)
                        if page_no not in page_set:
                            continue

                        rows = tbl_entry.get("rows", [])
                        cleaned = [
                            [str(c).strip() if c is not None else "" for c in row]
                            for row in rows
                        ]
                        if not _is_empty_table(cleaned) and len(cleaned) >= 2:
                            tables_by_page[page_no].append(cleaned)

                    logger.info(
                        "AWS Textract: extracted %d tables from %d pages",
                        sum(len(v) for v in tables_by_page.values()),
                        len(tables_by_page),
                    )
                except Exception as exc:
                    logger.error("Failed to parse Textract JSON output: %s", exc)

    except Exception as exc:
        logger.error("AWS Textract extraction failed: %s", exc)

    return dict(tables_by_page)

# ══════════════════════════════════════════════════════════════════════════════

def _classify_table(table: list) -> str:
    """
    Determine if a table is a luminaire schedule, a rejected schedule, or ambiguous.

    Returns: "positive", "negative", or "ambiguous"
    """
    # Examine first 3 rows
    header_text = " ".join(
        " ".join(row) for row in table[:3]
    ).lower()

    # Check explicit positive keywords first (luminaire/light fixture)
    for kw in _POSITIVE_KEYWORDS:
        if kw in header_text:
            return "positive"

    # Check negative keywords
    for kw in _NEGATIVE_KEYWORDS:
        if kw in header_text:
            return "negative"

    # "fixture schedule" alone is ambiguous — validate with column headers.
    # Must run BEFORE _PANEL_LABEL_RE since data rows may contain "panel"
    # (e.g. "2x4 LED Panel") triggering a false panel match.
    for kw in _AMBIGUOUS_KEYWORDS:
        if kw in header_text:
            best_score = 0
            for row in table[:min(5, len(table))]:
                s, _ = _score_row(row)
                best_score = max(best_score, s)
            if best_score >= 3:
                return "positive"
            return "negative"  # generic fixture schedule (floorbox, device, etc.)

    # Check panel label pattern (e.g. "Panel A", "Panel 1B")
    if _PANEL_LABEL_RE.search(header_text):
        return "negative"

    return "ambiguous"


# Additional negative keywords for broader table classification
_BROAD_NEGATIVE_KEYWORDS = [
    "electrical abbreviation", "abbreviation",
    "sheet index", "drawing index", "drawing list",
    "receptacle type legend", "symbol legend",
    "project no", "issue date",
    "motor schedule", "motor no",
    "load classification", "connected load", "demand factor",
    "estimated demand", "panel totals",
    "existing panel",
    "sub-metering", "virtual meter", "feeder schedule",
    "bus rating",
]


def _is_fixture_title(header_label: str, header_text: str) -> bool:
    """Return True only if the table title/content references a light fixture schedule.

    Used as a final gate before returning is_fixture_schedule=True to prevent
    false positives from tables that score well on column structure but are
    actually lighting control, compliance, or other non-fixture tables.
    """
    combined = (header_label + " " + header_text[:200]).lower()
    return any(t in combined for t in (
        "light fixture", "luminaire", "lighting fixture",
        "luminaire fixture", "fixture schedule",
    ))


def classify_table(table: list) -> dict:
    """
    Public API: Classify a table and extract its header label.

    Returns dict with:
        classification: "LIGHT_FIXTURE_SCHEDULE" | "PANEL_SCHEDULE" | "OTHER"
        header_label:   Detected table title/header (e.g. "LIGHT FIXTURE SCHEDULE")
        is_fixture_schedule: bool
    """
    # Collect text from first 3 rows for classification
    # Normalize embedded newlines so keywords like "override switch" match
    # against cell values like "OVERRIDE\nSWITCH".
    header_text = re.sub(r"\s+", " ", " ".join(
        " ".join(str(c) for c in row) for row in table[:3]
    )).lower()

    # --- Detect header label from the table content ---
    header_label = _extract_header_label(table)

    # --- Check for explicit light fixture schedule (positive) ---
    for kw in _POSITIVE_KEYWORDS:
        if kw in header_text:
            if _is_fixture_title(header_label, header_text):
                return {
                    "classification": "LIGHT_FIXTURE_SCHEDULE",
                    "header_label": header_label or "Light Fixture Schedule",
                    "is_fixture_schedule": True,
                }

    # --- Check for panel / circuit schedule (negative) ---
    # BUT: if the table also has strong fixture-schedule columns (score >= 3),
    # trust the column structure over a stray negative keyword that may appear
    # in fixture descriptions (e.g. "manufacturer", "power description").
    neg_hit = False
    for kw in _NEGATIVE_KEYWORDS:
        if kw in header_text:
            neg_hit = True
            break

    if neg_hit:
        best_score = 0
        best_fields = 0
        for row in table[:min(5, len(table))]:
            s, m = _score_row(row)
            if s > best_score:
                best_score = s
                best_fields = len(m)
        if best_score >= 8 and best_fields >= 4 and _is_fixture_title(header_label, header_text):
            return {
                "classification": "LIGHT_FIXTURE_SCHEDULE",
                "header_label": header_label or "Light Fixture Schedule",
                "is_fixture_schedule": True,
            }
        return {
            "classification": "PANEL_SCHEDULE",
            "header_label": header_label or "Panel Schedule",
            "is_fixture_schedule": False,
        }

    # --- "fixture schedule" without light/luminaire qualifier ---
    # Validate with column headers: must have light-fixture columns.
    # Must run BEFORE _PANEL_LABEL_RE since data rows may contain
    # "panel" (e.g. "2x4 LED Panel") triggering a false panel match.
    for kw in _AMBIGUOUS_KEYWORDS:
        if kw in header_text:
            best_score = 0
            for row in table[:min(5, len(table))]:
                s, _ = _score_row(row)
                best_score = max(best_score, s)
            if best_score >= 3 and _is_fixture_title(header_label, header_text):
                return {
                    "classification": "LIGHT_FIXTURE_SCHEDULE",
                    "header_label": header_label or "Fixture Schedule",
                    "is_fixture_schedule": True,
                }
            return {
                "classification": "OTHER",
                "header_label": header_label or "Fixture Schedule",
                "is_fixture_schedule": False,
            }

    if _PANEL_LABEL_RE.search(header_text):
        return {
            "classification": "PANEL_SCHEDULE",
            "header_label": header_label or "Panel Schedule",
            "is_fixture_schedule": False,
        }

    # --- Check broader negative keywords ---
    for kw in _BROAD_NEGATIVE_KEYWORDS:
        if kw in header_text:
            return {
                "classification": "OTHER",
                "header_label": header_label or "Other",
                "is_fixture_schedule": False,
            }

    # --- Ambiguous: check if it looks like a fixture table by column headers ---
    best_score = 0
    best_fields = 0
    best_mapping = {}
    for row in table[:min(3, len(table))]:
        s, m = _score_row(row)
        if s > best_score:
            best_score = s
            best_fields = len(m)
            best_mapping = m
    # Require score >= 3 AND at least 2 distinct matched fields.
    # Additionally require at least one electrical-specific field
    # (voltage, lumens, cct, dimming, max_va) to avoid false positives
    # on generic tables that happen to have "symbol" + "description" columns.
    _ELECTRICAL_FIELDS = {"voltage", "lumens", "cct", "dimming", "max_va"}
    has_electrical_field = bool(set(best_mapping.keys()) & _ELECTRICAL_FIELDS)
    if best_score >= 3 and best_fields >= 2 and has_electrical_field and _is_fixture_title(header_label, header_text):
        return {
            "classification": "LIGHT_FIXTURE_SCHEDULE",
            "header_label": header_label or "Fixture Schedule",
            "is_fixture_schedule": True,
        }

    return {
        "classification": "OTHER",
        "header_label": header_label or "Other",
        "is_fixture_schedule": False,
    }


def extract_fixtures_from_rows(rows: list, *, skip_classification: bool = False) -> List[dict]:
    """
    Extract structured fixture records from already-extracted table rows.

    Uses Layers 4-6 (header detection, row parsing, panel rejection)
    on the provided 2D row data.  This avoids re-reading the PDF and
    is used by the tables endpoint to attach inventory data to fixture
    schedule tables.

    Args:
        rows: 2D list of cell strings.
        skip_classification: If True, skip the Layer 3 keyword filter.
            Use when the table has already been validated as a fixture
            schedule (e.g. by classify_table or _identify_schedule).

    Returns list of fixture dicts (code, description, mounting, …).
    """
    if not rows or len(rows) < 2:
        return []

    fixtures = _parse_table(rows, skip_classification=skip_classification)
    return [f.to_dict() for f in fixtures]


def strip_rows_above_header(rows: list) -> list:
    """
    Remove rows above the detected header row in a fixture schedule table.

    Steps applied in order:
    1. Strip leading rows that are entirely notes / non-data content.
    2. Strip notes cells embedded within what will become the header row
       (handles merged PDF cells that place GENERAL NOTES alongside column names).
    3. Use _find_header_and_mapping to locate the real header row and drop
       anything above it.
    4. Strip trailing non-data rows (notes / descriptions below the table).
    """
    if not rows or len(rows) < 2:
        return rows

    # Step 1 — remove leading all-notes rows
    rows = _strip_leading_note_rows(rows)
    if not rows:
        return rows

    # Step 2 — clean notes cells out of the first (header) row
    cleaned_header = _strip_notes_cells_from_header(rows[0])
    if cleaned_header != rows[0]:
        rows = [cleaned_header] + rows[1:]

    # Step 3 — find the real header row and strip anything above it
    result = _find_header_and_mapping(rows)
    if result is None:
        return _strip_trailing_non_data_rows(rows)

    header_idx, _, _ = result
    stripped = rows[header_idx:] if header_idx > 0 else rows

    # Step 4 — strip trailing non-data rows (notes, descriptions below the table)
    return _strip_trailing_non_data_rows(stripped)


def _strip_trailing_non_data_rows(rows: list) -> list:
    """
    Remove trailing rows that are not fixture data.

    Trailing rows are often general notes, descriptions, or blank rows
    that appear below the actual schedule table. They should not be
    included in the extracted table.

    A row is considered non-data if:
      - All cells are empty
      - First cell is a header keyword
      - Row contains note-like patterns ("NOTE:", "GENERAL NOTES", etc.)
    """
    if not rows or len(rows) < 3:
        return rows

    # Find the last valid data row by scanning from the bottom
    last_data_idx = len(rows) - 1
    for i in range(len(rows) - 1, 0, -1):  # skip header row (index 0)
        row = rows[i]
        # Check if row is entirely empty
        if all(not cell.strip() for cell in row):
            last_data_idx = i - 1
            continue

        # Check first cell content
        first_cell = row[0].strip() if row else ""

        # If first cell matches note patterns, it's not data
        if first_cell and _NOTE_PATTERNS.match(first_cell):
            last_data_idx = i - 1
            continue

        # If first cell is a known header keyword (not a fixture code), skip
        if first_cell.lower() in _HEADER_KEYWORDS:
            last_data_idx = i - 1
            continue

        # Found a valid data row — stop trimming
        break

    if last_data_idx < len(rows) - 1:
        return rows[:last_data_idx + 1]
    return rows


# Patterns indicating notes / non-data content (used in both leading and trailing strip)
_NOTE_PATTERNS = re.compile(
    r"^\s*(?:KEY\s*NOTE[S]?|KEYNOTE[S]?|NOTE[S]?\s*[:.]|GENERAL\s+NOTE[S]?|REMARK|"
    r"ALL\s+FIXTURE|SEE\s+SPEC|REFER\s+TO|"
    r"\*|†|‡|\d+[.)]\s+[A-Z])",
    re.IGNORECASE,
)


def _is_notes_cell(cell: str) -> bool:
    """Return True if a cell contains notes/paragraph content rather than a column header.

    Notes cells are detected by:
    - Matching the notes keyword regex, OR
    - Being a long paragraph (>80 chars) with multiple sentence-ending punctuation marks.
      Real column headers are always short; notes from merged PDF cells are long sentences.
    """
    s = cell.strip()
    if not s:
        return False
    if _NOTE_PATTERNS.match(s):
        return True
    # Long paragraph text with multiple sentence endings = notes content
    if len(s) > 80 and len(re.findall(r"[.!?]", s)) > 1:
        return True
    return False


def _strip_leading_note_rows(rows: list) -> list:
    """Remove rows at the TOP of the table that are entirely notes / non-data content.

    Handles the case where GENERAL NOTES or KEY NOTES sections appear as full rows
    above the actual fixture schedule header.  A row is considered all-notes if every
    non-empty cell is notes content.
    """
    start_idx = 0
    for i, row in enumerate(rows):
        non_empty = [c.strip() for c in row if c.strip()]
        if not non_empty:
            # Blank row — skip over it
            start_idx = i + 1
            continue
        if all(_is_notes_cell(c) for c in non_empty):
            # Every cell is notes text — this whole row is a notes row
            start_idx = i + 1
        else:
            break
    return rows[start_idx:] if start_idx else rows


def _strip_notes_cells_from_header(row: list) -> list:
    """Remove notes cells from within a header row, keeping only real column-name cells.

    Handles the common case (especially with merged PDF cells) where a row is the
    real header but has long notes paragraphs mixed into its first few cells.
    If ALL cells are notes-like the original row is returned unchanged (safety net).
    """
    cleaned = [c for c in row if not _is_notes_cell(c)]
    return cleaned if cleaned else row


# Patterns that typically appear as a table title in the first cell or row
_LABEL_PATTERNS = re.compile(
    r"(light\s+fixture\s+schedule|luminaire\s+schedule|lighting\s+schedule|"
    r"fixture\s+schedule|panel\s+schedule|motor\s+schedule|"
    r"equipment\s+schedule|electrical\s+abbreviation[s]?|"
    r"electrical\s+sheet\s+index|receptacle\s+type\s+legend|"
    r"load\s+classification|branch\s+panel)",
    re.IGNORECASE,
)


def _extract_header_label(table: list) -> str:
    """
    Try to find a recognisable title/label from the table's first few rows.
    Returns the matched label in title-case, or empty string.
    """
    for row in table[:3]:
        for cell in row:
            cell_str = str(cell)
            m = _LABEL_PATTERNS.search(cell_str)
            if m:
                return m.group(0).strip().title()
    return ""


# ══════════════════════════════════════════════════════════════════════════════
#  Layer 4 — Header Row Detection & Column Mapping
# ══════════════════════════════════════════════════════════════════════════════

def _normalize_header(text: str) -> str:
    """Lowercase, strip, collapse whitespace."""
    return re.sub(r"\s+", " ", text.lower().strip())


def _match_alias(header_text: str, alias: str, is_short: bool) -> bool:
    """
    Check if a header cell matches an alias.
    Short aliases (<=3 chars): exact match only.
    Long aliases: substring match.
    """
    if is_short:
        return header_text == alias
    return alias in header_text


def _score_row(row: list, existing_mapping: dict = None) -> Tuple[int, Dict[str, int]]:
    """
    Score a row as a potential header row.

    Returns (score, column_mapping) where column_mapping is {field_name: col_index}.
    A 2-point bonus is given for "strong" fields: code, description, voltage.
    """
    strong_fields = {"code", "description", "voltage"}
    mapping: Dict[str, int] = {}
    used_cols: set = set()
    score = 0

    for field_name, aliases in _COLUMN_ALIASES:
        if field_name in mapping:
            continue
        for col_idx, cell in enumerate(row):
            if col_idx in used_cols:
                continue
            normalized = _normalize_header(cell)
            if not normalized:
                continue
            for alias, is_short in aliases:
                if _match_alias(normalized, alias, is_short):
                    mapping[field_name] = col_idx
                    used_cols.add(col_idx)
                    score += 1
                    if field_name in strong_fields:
                        score += 2  # bonus for strong fields
                    break
            if field_name in mapping:
                break

    return score, mapping


def _merge_rows(row_a: list, row_b: list) -> list:
    """Merge two consecutive rows by concatenating their cells."""
    max_len = max(len(row_a), len(row_b))
    merged = []
    for i in range(max_len):
        a = row_a[i] if i < len(row_a) else ""
        b = row_b[i] if i < len(row_b) else ""
        parts = [p for p in (a.strip(), b.strip()) if p]
        merged.append(" ".join(parts))
    return merged


def _find_header_and_mapping(table: list) -> Optional[Tuple[int, Dict[str, int], bool]]:
    """
    Layer 4: Find the best header row (or merged pair) and column mapping.

    Returns (header_row_index, column_mapping, is_merged_header) or None.
    For merged headers, header_row_index is the index of the first row in the pair.
    """
    max_rows_to_check = min(10, len(table))
    best_score = 0
    best_result = None

    # Score single rows
    for i in range(max_rows_to_check):
        score, mapping = _score_row(table[i])
        if score > best_score:
            best_score = score
            best_result = (i, mapping, False)

    # Score merged row pairs
    for i in range(max_rows_to_check - 1):
        merged = _merge_rows(table[i], table[i + 1])
        score, mapping = _score_row(merged)
        if score > best_score:
            best_score = score
            best_result = (i, mapping, True)

    # Must have at least a code column mapped
    if best_result is None:
        return None
    _, mapping, _ = best_result
    if "code" not in mapping:
        return None

    return best_result


# ══════════════════════════════════════════════════════════════════════════════
#  Layer 5 — Row Parsing & Embedded Data Recovery
# ══════════════════════════════════════════════════════════════════════════════

def _detect_embedded_data(header_row: list, mapping: Dict[str, int]) -> Optional[list]:
    """
    Detect embedded data: header label and first data value in the same cell.
    e.g., "MARK A" or "MARK\nA" → "MARK" is the header, "A" is data.

    Returns a synthetic data row if embedded data is found, else None.
    """
    synthetic_row = [""] * len(header_row)
    found_any = False

    for field_name, col_idx in mapping.items():
        if col_idx >= len(header_row):
            continue
        cell = header_row[col_idx]
        if not cell:
            continue

        # Find which alias matched this column
        for alias_field, aliases in _COLUMN_ALIASES:
            if alias_field != field_name:
                continue
            for alias, is_short in aliases:
                normalized = _normalize_header(cell)
                if _match_alias(normalized, alias, is_short):
                    # Check if there's text after the alias
                    # Try to strip the alias from the beginning
                    pattern = re.compile(
                        r"^\s*" + re.escape(alias) + r"\s+(.+)$",
                        re.IGNORECASE,
                    )
                    m = pattern.match(cell)
                    if m:
                        remainder = m.group(1).strip()
                        if remainder:
                            synthetic_row[col_idx] = remainder
                            found_any = True
                    break
            break

    return synthetic_row if found_any else None


def _is_data_row(row: list, code_col: int) -> bool:
    """
    Layer 5: Determine if a row is a valid fixture data row.

    Rules:
      - Code column must not be empty.
      - Code must not be a recognized header keyword.
      - For codes > 3 chars, code must contain at least one digit OR
        be a known short-alpha pattern.
      - Pure alpha codes longer than 3 characters are rejected.
    """
    if code_col >= len(row):
        return False

    code = row[code_col].strip()
    if not code:
        return False

    code_lower = code.lower()

    # Reject recognized header keywords
    if code_lower in _HEADER_KEYWORDS:
        return False

    # For codes longer than 3 characters
    if len(code) > 3:
        # Must contain at least one digit
        if not re.search(r"\d", code):
            return False

    return True


def _parse_row(row: list, mapping: Dict[str, int], header_row: list = None) -> FixtureRecord:
    """Convert a data row to a FixtureRecord using the column mapping.

    Args:
        row: The data row cells.
        mapping: Dict mapping field names to column indices.
        header_row: Optional header row to populate raw_data with ALL columns.
                    If provided, every column value is stored in raw_data using
                    the original header as the key.
    """
    record = FixtureRecord()
    code_col = mapping.get("code")

    # Populate standard fields using the known mapping
    for field_name, col_idx in mapping.items():
        if col_idx < len(row):
            value = row[col_idx].strip()
            setattr(record, field_name, value)

    # DYNAMIC EXTRACTION: Store ALL columns in raw_data with original header names
    if header_row:
        for col_idx, cell_value in enumerate(row):
            if col_idx < len(header_row):
                header_name = header_row[col_idx].strip()
                if header_name:  # Only store non-empty headers
                    record.raw_data[header_name] = cell_value.strip()
                else:
                    # Use a generic name for empty headers
                    record.raw_data[f"Column_{col_idx + 1}"] = cell_value.strip()
            else:
                # Extra columns beyond header - store with generic name
                record.raw_data[f"Column_{col_idx + 1}"] = cell_value.strip()

    # --- Fixture code cleaning ---
    # Docling often merges the fixture code cell with adjacent description
    # text, producing values like "A1 2'x2' LED TROFFER..." instead of "A1".
    #
    # Strategy: check the last column for a clean code (many fixture schedule
    # tables repeat the code in the last column). If the last column has a
    # short alphanumeric code, prefer it.
    if record.code and code_col is not None:
        last_val = row[-1].strip() if row else ""
        last_col_idx = len(row) - 1

        # Use last column if: it's a different column, looks like a fixture
        # code, and the current code is longer (polluted).
        if (
            last_col_idx != code_col
            and last_val
            and len(last_val) <= 6
            and re.match(r'^[A-Za-z]\w{0,5}$', last_val)
            and last_val.lower() not in _HEADER_KEYWORDS
            and len(record.code) > len(last_val)
        ):
            record.code = last_val
        elif len(record.code) > 6:
            # Fallback: extract leading code from the merged text
            cleaned = _extract_leading_code(record.code)
            if cleaned:
                record.code = cleaned

    return record


# Pattern to extract a leading fixture code like "A1", "B2X", "C1" from
# a cell that has the code merged with description text.
_LEADING_CODE_RE = re.compile(r'^([A-Za-z]\d?\w{0,3})\s')


def _extract_leading_code(text: str) -> str:
    """Extract a short fixture code from the beginning of a cell value."""
    m = _LEADING_CODE_RE.match(text)
    if m:
        return m.group(1).strip()
    return ""


# ══════════════════════════════════════════════════════════════════════════════
#  Layer 6 — Post-Parse Panel Schedule Rejection
# ══════════════════════════════════════════════════════════════════════════════

def _reject_panel_schedule(fixtures: List[FixtureRecord]) -> bool:
    """
    If > 5 fixtures and > 60% of codes are purely numeric,
    discard (likely a breaker/panel schedule).
    """
    if len(fixtures) <= 5:
        return False

    numeric_count = sum(1 for f in fixtures if f.code.strip().isdigit())
    ratio = numeric_count / len(fixtures)
    return ratio > 0.60


# ══════════════════════════════════════════════════════════════════════════════
#  Core Parser — Layers 3-6 Combined
# ══════════════════════════════════════════════════════════════════════════════

def _parse_table(table: list, *, skip_classification: bool = False) -> List[FixtureRecord]:
    """
    Run Layers 3-6 on a single raw table.

    Returns a list of FixtureRecords (possibly empty).
    """
    # Layer 3 — filtering
    if not skip_classification:
        classification = _classify_table(table)
        if classification == "negative":
            logger.debug("Table rejected by keyword filter (negative)")
            return []

    # Layer 4 — header detection
    result = _find_header_and_mapping(table)
    if result is None:
        logger.debug("No valid header row found in table")
        return []

    header_idx, mapping, is_merged = result
    code_col = mapping.get("code")
    if code_col is None:
        return []

    logger.debug(
        "Header found at row %d (merged=%s), mapping: %s",
        header_idx, is_merged, mapping,
    )

    # Determine index of first data row
    data_start = header_idx + (2 if is_merged else 1)

    # Layer 5 — embedded data recovery
    header_row = table[header_idx] if not is_merged else _merge_rows(
        table[header_idx], table[header_idx + 1]
    )
    fixtures: List[FixtureRecord] = []

    synthetic_row = _detect_embedded_data(header_row, mapping)
    if synthetic_row is not None and _is_data_row(synthetic_row, code_col):
        fixtures.append(_parse_row(synthetic_row, mapping, header_row))

    # Parse data rows - pass header_row for dynamic extraction
    for row in table[data_start:]:
        if _is_data_row(row, code_col):
            fixtures.append(_parse_row(row, mapping, header_row))

    # Layer 6 — post-parse panel rejection
    if fixtures and _reject_panel_schedule(fixtures):
        logger.info(
            "Discarded %d fixtures (panel schedule detected: >60%% numeric codes)",
            len(fixtures),
        )
        return []

    return fixtures


# ══════════════════════════════════════════════════════════════════════════════
#  Layer 7 — VLM Vision Fallback (Claude API)
# ══════════════════════════════════════════════════════════════════════════════

_MAX_VLM_IMAGE_BYTES = 5 * 1024 * 1024  # 5 MB base64 limit


def _render_page_to_png(pdf_path: str, page_number: int, max_dpi: int = 200) -> Optional[bytes]:
    """
    Render a PDF page to PNG bytes, auto-scaling DPI to stay under the
    API base64 size limit.

    Args:
        pdf_path: Path to the PDF file.
        page_number: 1-based page number.
        max_dpi: Starting DPI (will be halved if image is too large).

    Returns PNG bytes or None on failure.
    """
    try:
        doc = fitz.open(pdf_path)
        page = doc[page_number - 1]
        dpi = max_dpi

        while dpi >= 72:
            zoom = dpi / 72.0
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat)
            png_bytes = pix.tobytes("png")
            pix = None  # release pixmap memory

            # Check base64 size (base64 expands ~33%)
            b64_size = len(png_bytes) * 4 // 3
            if b64_size <= _MAX_VLM_IMAGE_BYTES:
                doc.close()
                return png_bytes

            logger.debug(
                "Page %d at %d DPI produces %d bytes base64 (limit %d), halving DPI",
                page_number, dpi, b64_size, _MAX_VLM_IMAGE_BYTES,
            )
            dpi //= 2

        logger.warning(
            "Page %d image too large even at 72 DPI, skipping VLM",
            page_number,
        )
        doc.close()
        return None
    except Exception as exc:
        logger.error("Failed to render page %d: %s", page_number, exc)
        return None


def _vlm_extract_fixtures(
    pdf_path: str,
    page_number: int,
    plan_codes: List[str],
) -> List[FixtureRecord]:
    """
    Layer 7: Use Claude Vision API to extract fixture records from a
    rasterized schedule page.

    Args:
        pdf_path: Path to the PDF.
        page_number: 1-based page number to render.
        plan_codes: Fixture codes found on lighting plan pages (hints).

    Returns list of FixtureRecords extracted by the VLM.
    """
    try:
        from config import ANTHROPIC_API_KEY
        if not ANTHROPIC_API_KEY:
            logger.debug("No ANTHROPIC_API_KEY configured — VLM fixture extraction skipped")
            return []
    except (ImportError, AttributeError):
        logger.debug("ANTHROPIC_API_KEY not configured — VLM fixture extraction skipped")
        return []

    png_bytes = _render_page_to_png(pdf_path, page_number)
    if png_bytes is None:
        return []

    b64_image = base64.b64encode(png_bytes).decode("ascii")

    hint_text = ""
    if plan_codes:
        hint_text = (
            "\n\nHINT: The following fixture codes were found on the lighting plan pages "
            "and should appear in this schedule. Use them to guide your reading of the "
            f"code/mark column: {', '.join(plan_codes)}"
        )

    prompt = (
        "You are analyzing a luminaire/fixture schedule table from an electrical drawing PDF.\n\n"
        "Extract every fixture row from this schedule and return a JSON array. "
        "Each object must have these fields (use empty string if not found):\n"
        '  "code": fixture mark/id/type code\n'
        '  "description": fixture description or luminaire type\n'
        '  "mounting": mounting type\n'
        '  "fixture_style": fixture style or catalog number\n'
        '  "voltage": voltage rating\n'
        '  "lumens": lumen output\n'
        '  "cct": color temperature (CCT/kelvin)\n'
        '  "dimming": dimming/ballast/driver info\n'
        '  "max_va": wattage/VA rating\n\n'
        "Return ONLY the JSON array, no other text. "
        "If you cannot find any fixture data, return an empty array []."
        f"{hint_text}"
    )

    try:
        import anthropic

        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": b64_image,
                            },
                        },
                        {
                            "type": "text",
                            "text": prompt,
                        },
                    ],
                }
            ],
        )

        response_text = message.content[0].text.strip()
        logger.debug("VLM raw response (page %d): %s", page_number, response_text[:500])

        # Extract JSON from response (handle possible markdown wrapping)
        json_match = re.search(r"\[.*\]", response_text, re.DOTALL)
        if not json_match:
            logger.warning("VLM response for page %d contains no JSON array", page_number)
            return []

        data = json.loads(json_match.group())
        if not isinstance(data, list):
            return []

        fixtures = []
        valid_fields = {f.name for f in FixtureRecord.__dataclass_fields__.values()}
        for item in data:
            if not isinstance(item, dict):
                continue
            # Filter to only known fields, convert all to string
            cleaned = {
                k: str(v) if v is not None else ""
                for k, v in item.items()
                if k in valid_fields
            }
            if cleaned.get("code", "").strip():
                fixtures.append(FixtureRecord(**cleaned))

        # Apply panel schedule rejection to VLM results too
        if fixtures and _reject_panel_schedule(fixtures):
            logger.info("VLM results discarded (panel schedule pattern)")
            return []

        logger.info("VLM extracted %d fixtures from page %d", len(fixtures), page_number)
        return fixtures

    except ImportError:
        logger.warning("anthropic package not installed — VLM fixture extraction skipped")
        return []
    except json.JSONDecodeError as exc:
        logger.warning("VLM response JSON parse error for page %d: %s", page_number, exc)
        return []
    except Exception as exc:
        logger.error("VLM fixture extraction failed for page %d: %s", page_number, exc)
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  Public API — Full extraction pipeline
# ══════════════════════════════════════════════════════════════════════════════

def extract_fixtures_from_pages(
    pdf_path: str,
    page_numbers: List[int],
    plan_codes: Optional[List[str]] = None,
    use_vlm_fallback: bool = True,
    use_aws_textract: bool = True,
) -> List[FixtureRecord]:
    """
    Run Layers 1-6 on a set of pages using AWS Textract.

    Args:
        pdf_path: Path to the source PDF.
        page_numbers: 1-based page numbers to extract from.
        plan_codes: Fixture codes found on plan pages (for VLM hints).
        use_vlm_fallback: Whether to try VLM if table extraction finds nothing.
        use_aws_textract: Whether to use AWS Textract for Layer 1 (default: True).

    Returns combined list of FixtureRecords from all pages.
    """
    all_fixtures: List[FixtureRecord] = []

    try:
        logger.info("Using AWS Textract for table extraction Layer 1")
        tables_by_page = _aws_textract_tables_for_pages(pdf_path, page_numbers)
    except Exception as exc:
        logger.error("Layer 1 table extraction failed for %s: %s", pdf_path, exc)
        return all_fixtures

    for pn in page_numbers:
        tables = tables_by_page.get(pn, [])
        for table in tables:
            fixtures = _parse_table(table)
            if fixtures:
                logger.info(
                    "Page %d: extracted %d fixtures from table",
                    pn, len(fixtures),
                )
                all_fixtures.extend(fixtures)

    return all_fixtures


def extract_fixtures(
    pdf_path: str,
    schedule_pages: List[int],
    plan_pages: Optional[List[int]] = None,
    plan_codes: Optional[List[str]] = None,
    use_vlm_fallback: bool = True,
) -> List[FixtureRecord]:
    """
    Full extraction pipeline (Layers 1-8).

    Args:
        pdf_path: Path to the source PDF file.
        schedule_pages: 1-based page numbers classified as SCHEDULE.
        plan_pages: 1-based page numbers classified as LIGHTING_PLAN (for Layer 8).
        plan_codes: Fixture codes found on plan pages (for VLM hint).
        use_vlm_fallback: Whether to use Claude VLM as fallback (Layer 7).

    Returns list of all extracted FixtureRecords.
    """
    plan_pages = plan_pages or []
    plan_codes = plan_codes or []

    # Layers 1-6: Extract from schedule pages
    fixtures = extract_fixtures_from_pages(
        pdf_path, schedule_pages, plan_codes, use_vlm_fallback=False,
    )

    # Layer 7: VLM fallback if Docling extracted nothing
    if not fixtures and use_vlm_fallback and schedule_pages:
        logger.info(
            "No fixtures from Textract on %d schedule pages — trying VLM fallback",
            len(schedule_pages),
        )
        for pn in schedule_pages:
            vlm_fixtures = _vlm_extract_fixtures(pdf_path, pn, plan_codes)
            if vlm_fixtures:
                fixtures.extend(vlm_fixtures)

    # Layer 8: Combo page handling — also try extraction on lighting plan pages
    if plan_pages:
        plan_fixtures = extract_fixtures_from_pages(
            pdf_path, plan_pages, plan_codes, use_vlm_fallback=False,
        )
        if plan_fixtures:
            logger.info(
                "Layer 8: found %d fixtures on lighting plan pages",
                len(plan_fixtures),
            )
            # Deduplicate by code — schedule page fixtures take priority
            existing_codes = {f.code.strip().lower() for f in fixtures if f.code.strip()}
            for f in plan_fixtures:
                if f.code.strip().lower() not in existing_codes:
                    fixtures.append(f)
                    existing_codes.add(f.code.strip().lower())

    # Deduplicate across all sources by code
    seen_codes: set = set()
    deduped: List[FixtureRecord] = []
    for f in fixtures:
        key = f.code.strip().lower()
        if key and key not in seen_codes:
            seen_codes.add(key)
            deduped.append(f)
        elif not key:
            deduped.append(f)  # Keep records without codes (rare but possible)

    if deduped:
        logger.info("Total fixtures extracted: %d (from %s)", len(deduped), pdf_path)

    return deduped
