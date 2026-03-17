"""
Page Classifier Module
=======================
Classifies engineering drawing pages into types and determines relevance
for lighting fixture analysis.

Classification uses four priority levels (highest to lowest):
  1. Sheet Index Lookup   — from cover/legend page table
  2. Title Block Analysis  — bottom-right corner self-description
  3. Sheet Code Prefix     — industry-standard naming conventions
  4. Full-Page Content Scan — keyword search as deepest fallback

After classification, SCHEDULE pages go through additional verification
to distinguish luminaire schedules (relevant) from panel/equipment
schedules (not relevant).

Public API:
    classify_all_pages(pdf_path, page_texts, total_pages)
        → {page_num: {"page_type": str, "is_relevant": bool}}
"""

import enum
import re
import logging
import gc
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from typing import Dict, List, Optional, Tuple, Any

import fitz  # PyMuPDF

from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.datamodel.base_models import InputFormat

logger = logging.getLogger(__name__)

# Timeout in seconds for Docling operations on complex PDFs.
_DOCLING_TIMEOUT = 180

# Cache: avoids re-converting the same PDF within a single classification call.
# Capped at 2 entries (LRU-style) so it never grows unboundedly across requests.
_DOCLING_CACHE_MAXSIZE = 2
_docling_doc_cache: OrderedDict = OrderedDict()
_docling_cache_lock = threading.Lock()  # prevents concurrent conversions


def _clear_docling_cache() -> None:
    """Evict all cached Docling documents and trigger garbage collection."""
    _docling_doc_cache.clear()
    gc.collect()
    logger.debug("Docling doc cache cleared")


# ═══════════════════════════════════════════════════════════════════════════════
# Page Type Enum
# ═══════════════════════════════════════════════════════════════════════════════

class PageType(str, enum.Enum):
    LIGHTING_PLAN = "LIGHTING_PLAN"
    SCHEDULE = "SCHEDULE"
    SYMBOLS_LEGEND = "SYMBOLS_LEGEND"
    COVER = "COVER"
    DEMOLITION_PLAN = "DEMOLITION_PLAN"
    POWER_PLAN = "POWER_PLAN"
    SITE_PLAN = "SITE_PLAN"
    FIRE_ALARM = "FIRE_ALARM"
    RISER = "RISER"
    DETAIL = "DETAIL"
    OTHER = "OTHER"


# Types that are always relevant — SCHEDULE needs additional verification.
_ALWAYS_RELEVANT = {PageType.LIGHTING_PLAN, PageType.SYMBOLS_LEGEND, PageType.COVER}


# ═══════════════════════════════════════════════════════════════════════════════
# Sheet Index — infer type from description text  (Priority 1 helper)
# ═══════════════════════════════════════════════════════════════════════════════

# Ordered — first match wins.
_DESCRIPTION_TYPE_RULES: List[Tuple[List[str], PageType]] = [
    (["demolition", "demo plan", "demo "],                      PageType.DEMOLITION_PLAN),
    (["lighting plan", "lighting layout", "lighting area",
      "electrical lighting", "electrical plan"],                 PageType.LIGHTING_PLAN),
    (["schedule"],                                               PageType.SCHEDULE),
    (["symbol", "abbreviation", "legend"],                       PageType.SYMBOLS_LEGEND),
    (["power plan", "power layout", "power area"],               PageType.POWER_PLAN),
    (["signal plan", "signal layout"],                           PageType.OTHER),
    (["site plan", "site layout"],                               PageType.SITE_PLAN),
    (["fire alarm"],                                             PageType.FIRE_ALARM),
    (["riser"],                                                  PageType.RISER),
    (["detail"],                                                 PageType.DETAIL),
    (["cover sheet", "title sheet", "coversheet"],               PageType.COVER),
]


def _infer_type_from_description(description: str) -> Optional[PageType]:
    """Infer PageType from a sheet index description string."""
    desc_lower = description.lower()
    for keywords, page_type in _DESCRIPTION_TYPE_RULES:
        for kw in keywords:
            if kw in desc_lower:
                return page_type
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Title Block — infer type from self-declared title  (Priority 2)
# ═══════════════════════════════════════════════════════════════════════════════

# Strictly ordered — order matters for disambiguation.
_TITLE_BLOCK_RULES: List[Tuple[List[str], PageType]] = [
    (["demolition plan", "demolition layout", "demo plan"],       PageType.DEMOLITION_PLAN),
    (["site plan", "site layout", "photometric"],                  PageType.SITE_PLAN),
    (["roof electrical plan", "electrical roof plan"],              PageType.OTHER),
    (["cover sheet", "title sheet", "coversheet"],                 PageType.COVER),
    (["electrical symbols", "abbreviation", "legend"],             PageType.SYMBOLS_LEGEND),
    (["panel schedule", "panelboard schedule"],                    PageType.SCHEDULE),
    (["security plan", "technology plan"],                         PageType.FIRE_ALARM),
    (["lighting plan", "lighting layout",
      "electrical lighting plan",
      "plan - lighting", "plan-lighting"],                         PageType.LIGHTING_PLAN),
    (["luminaire schedule", "light fixture schedule",
      "lighting schedule", "fixture schedule",
      "electrical schedules"],                                     PageType.SCHEDULE),
    (["enlarged electrical room", "electrical room plan"],         PageType.OTHER),
    (["power plan", "power layout", "power &",
      "systems plan", "electrical plan"],                          PageType.POWER_PLAN),
    (["fire alarm"],                                               PageType.FIRE_ALARM),
    (["riser diagram", "riser"],                                   PageType.RISER),
    (["detail"],                                                   PageType.DETAIL),
]

# Regex-based title block rules for cases where keywords appear in different order
# e.g. "UPPER LEVEL PLAN - LIGHTING AREA A"
_TITLE_BLOCK_REGEX_RULES: List[Tuple[re.Pattern, PageType]] = [
    (re.compile(r'plan\s*[-–—]\s*lighting', re.IGNORECASE),        PageType.LIGHTING_PLAN),
    (re.compile(r'lighting\s+area', re.IGNORECASE),                PageType.LIGHTING_PLAN),
    (re.compile(r'level\b.*\blighting', re.IGNORECASE),            PageType.LIGHTING_PLAN),
    (re.compile(r'plan\s*[-–—]\s*power', re.IGNORECASE),           PageType.POWER_PLAN),
    (re.compile(r'plan\s*[-–—]\s*demo', re.IGNORECASE),            PageType.DEMOLITION_PLAN),
]


def _infer_type_from_title_block(title_text: str, use_last_match: bool = False) -> Optional[PageType]:
    """Infer PageType from title block text. Order matters.

    When *use_last_match* is True, return the LAST keyword match (by position)
    rather than the first rule-table hit.  This is useful for title blocks
    that contain multiple drawing titles — the primary title is typically
    the last/lowest one (e.g. "ELECTRICAL SITE PLAN" below "DETAIL").
    """
    title_lower = title_text.lower()

    if not use_last_match:
        for keywords, page_type in _TITLE_BLOCK_RULES:
            for kw in keywords:
                if kw in title_lower:
                    return page_type
        for pattern, page_type in _TITLE_BLOCK_REGEX_RULES:
            if pattern.search(title_text):
                return page_type
        return None

    # use_last_match mode: find ALL keyword matches and pick the one with
    # the highest position (latest occurrence in the text).  Title blocks
    # often contain a secondary detail title above the main drawing title.
    best_pos = -1
    best_type: Optional[PageType] = None
    for keywords, page_type in _TITLE_BLOCK_RULES:
        for kw in keywords:
            pos = title_lower.rfind(kw)
            if pos > best_pos:
                best_pos = pos
                best_type = page_type
    for pattern, page_type in _TITLE_BLOCK_REGEX_RULES:
        m = pattern.search(title_text)
        if m and m.start() > best_pos:
            best_pos = m.start()
            best_type = page_type
    return best_type


# ═══════════════════════════════════════════════════════════════════════════════
# Sheet Code Prefix Rules  (Priority 3)
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_by_sheet_code(
    sheet_code: str,
    sheet_title: str = "",
) -> Optional[PageType]:
    """
    Classify by industry-standard sheet code prefixes,
    with disambiguation from the sheet title when ambiguous.
    """
    code_upper = sheet_code.upper().strip()
    title_lower = sheet_title.lower()

    if code_upper == "CS":
        return PageType.COVER

    if code_upper.startswith("E0"):
        return PageType.SYMBOLS_LEGEND

    if code_upper.startswith("E1"):
        if "demolition plan" in title_lower or "demo plan" in title_lower or title_lower.startswith("demo"):
            return PageType.DEMOLITION_PLAN
        return PageType.LIGHTING_PLAN

    if code_upper.startswith("E2"):
        if "power" in title_lower:
            return PageType.POWER_PLAN
        if "signal" in title_lower:
            return PageType.OTHER
        if "demolition plan" in title_lower or "demo plan" in title_lower or title_lower.startswith("demo"):
            return PageType.DEMOLITION_PLAN
        return PageType.LIGHTING_PLAN

    if code_upper.startswith("E3"):
        if "lighting" in title_lower:
            return PageType.LIGHTING_PLAN
        return PageType.POWER_PLAN

    if code_upper.startswith("E4"):
        return PageType.POWER_PLAN

    if code_upper.startswith(("E5", "E6", "E7")):
        if "roof" in title_lower:
            return PageType.OTHER
        if "riser" in title_lower:
            return PageType.RISER
        return PageType.SCHEDULE

    if code_upper.startswith("E8"):
        return PageType.DETAIL

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Full-Page Content Keyword Scan  (Priority 4)
# ═══════════════════════════════════════════════════════════════════════════════

# Regex to strip cross-reference text before scanning — prevents a lighting
# plan that just *mentions* a schedule from being classified as one.
_CROSS_REF_RE = re.compile(
    r'(?:SEE|REFER\s+TO)\s+'
    r'(?:SHEET|PLAN|DRAWING|PAGE)\s+'
    r'\S+\s+'
    r'FOR\s+'
    r'.*?'
    r'(?:SCHEDULE|PLAN|LAYOUT)',
    re.IGNORECASE | re.DOTALL,
)

_CONTENT_SCAN_RULES: List[Tuple[List[str], PageType]] = [
    (["luminaire schedule", "light fixture schedule",
      "lighting schedule", "fixture schedule"],       PageType.SCHEDULE),
    (["lighting plan", "lighting layout",
      "plan - lighting", "plan-lighting",
      "lighting area"],                                PageType.LIGHTING_PLAN),
    (["demolition plan", "demolition layout",
      "demo plan"],                                    PageType.DEMOLITION_PLAN),
    (["power plan", "power layout",
      "receptacle plan"],                              PageType.POWER_PLAN),
    (["electrical symbols", "abbreviations",
      "legend"],                                       PageType.SYMBOLS_LEGEND),
    (["site plan", "site layout"],                     PageType.SITE_PLAN),
    (["fire alarm"],                                   PageType.FIRE_ALARM),
    (["riser diagram", "riser schedule"],               PageType.RISER),
    (["detail"],                                       PageType.DETAIL),
]

# Regex rules for content scan fallback (reversed keyword order etc.)
_CONTENT_SCAN_REGEX_RULES: List[Tuple[re.Pattern, PageType]] = [
    (re.compile(r'plan\s*[-–—]\s*lighting', re.IGNORECASE),  PageType.LIGHTING_PLAN),
    (re.compile(r'level\b.*\blighting', re.IGNORECASE),      PageType.LIGHTING_PLAN),
]


# Additional regex to strip general notes and annotations that mention
# other drawing types in passing (not actual classification indicators).
_GENERAL_NOTES_RE = re.compile(
    r'(?:'
    r'PROVIDE\s+.*?(?:FIRE\s+ALARM|SITE|DETAIL|RISER)'
    r'|REFER\s+TO\s+.*?(?:DIAGRAM|PLAN|SCHEDULE|DETAIL)'
    r'|SEE\s+(?:ELECTRICAL|PLUMBING|MECHANICAL)\s+.*?(?:DETAIL|PLAN)'
    r'|SEE\s+\w+\s+DETAIL'
    r'|COORDINATE\s+WITH\s+.*?(?:FIRE\s+ALARM|PLAN|DIAGRAM)'
    r')',
    re.IGNORECASE | re.DOTALL,
)


def _classify_by_content(page_text: str) -> PageType:
    """Priority 4 — deepest fallback: scan full page text for keywords.

    Uses a weighted approach: if multiple keyword categories match,
    the one with the MOST hits wins.  This avoids a single incidental
    mention of 'fire alarm' in a general note from overriding the
    actual drawing type.
    """
    cleaned = _CROSS_REF_RE.sub("", page_text)
    cleaned = _GENERAL_NOTES_RE.sub("", cleaned).lower()

    # Count hits per page type
    type_scores: Dict[PageType, int] = {}
    for keywords, page_type in _CONTENT_SCAN_RULES:
        count = sum(cleaned.count(kw) for kw in keywords)
        if count > 0:
            type_scores[page_type] = type_scores.get(page_type, 0) + count

    # Regex fallback for reversed/hyphenated patterns
    for pattern, page_type in _CONTENT_SCAN_REGEX_RULES:
        if pattern.search(page_text):
            type_scores[page_type] = type_scores.get(page_type, 0) + 1

    if not type_scores:
        return PageType.OTHER

    # Return the type with the highest score
    best_type = max(type_scores, key=lambda t: type_scores[t])
    logger.debug("Content scan scores: %s → winner: %s",
                 {t.value: s for t, s in type_scores.items()}, best_type.value)
    return best_type


# ═══════════════════════════════════════════════════════════════════════════════
# Schedule Verification  (additional layer after main classification)
# ═══════════════════════════════════════════════════════════════════════════════

_LUMINAIRE_SCHEDULE_KW = [
    "luminaire schedule", "light fixture schedule",
    "lighting schedule", "fixture schedule",
]

_NON_LUMINAIRE_SCHEDULE_KW = [
    "panel schedule", "motor schedule", "equipment schedule",
    "floorbox", "poke thru",
]

_SECONDARY_SCHEDULE_KW = ["fixture", "luminaire", "lighting", "lamp", "led",
                          "watt", "lumen", "cct", "color temp", "dimming"]


def _is_luminaire_schedule(page_text: str) -> bool:
    """
    Determine whether a SCHEDULE page is a *luminaire* schedule (relevant)
    versus a panel / equipment schedule (not relevant).
    """
    text_lower = page_text.lower()

    # Primary check: explicit luminaire schedule keywords
    has_luminaire_kw = any(kw in text_lower for kw in _LUMINAIRE_SCHEDULE_KW)

    if has_luminaire_kw:
        # Disqualify if non-luminaire keywords heavily dominate
        non_luminaire_count = sum(
            text_lower.count(kw) for kw in _NON_LUMINAIRE_SCHEDULE_KW
        )
        luminaire_count = sum(
            text_lower.count(kw) for kw in _LUMINAIRE_SCHEDULE_KW
        )
        if non_luminaire_count > luminaire_count * 3:
            return False
        return True

    # Secondary check: "schedule" AND any lighting-related keyword
    if "schedule" in text_lower:
        if any(kw in text_lower for kw in _SECONDARY_SCHEDULE_KW):
            return True

    return False


# ═══════════════════════════════════════════════════════════════════════════════
# Title Block Extraction
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_title_block_fitz(page: fitz.Page) -> str:
    """
    Fast path: extract title block text using PyMuPDF.
    Crops the bottom-right rectangle — 55 % width → right edge,
    85 % height → bottom edge.
    """
    try:
        rect = page.rect
        crop = fitz.Rect(
            rect.width * 0.55,
            rect.height * 0.85,
            rect.width,
            rect.height,
        )
        text = page.get_textbox(crop)
        return re.sub(r'\s+', ' ', text).strip()
    except Exception as exc:
        logger.debug("fitz title-block extraction failed (page %d): %s",
                     page.number, exc)
        return ""


def _extract_title_block_docling(pdf_path: str, page_idx: int) -> str:
    """
    Fallback: extract title block text using Docling.
    Collects text items from the bottom-right region of the page.
    """
    try:
        doc = _get_docling_doc(pdf_path)
        if doc is None:
            return ""

        page_no = page_idx + 1  # Docling uses 1-based page numbers
        page_item = doc.pages.get(page_no)
        if page_item is None:
            return ""

        # Collect all text items on this page
        page_texts = []
        for text_item in doc.texts:
            if text_item.prov and text_item.prov[0].page_no == page_no:
                page_texts.append(text_item.text)

        if not page_texts:
            return ""

        # Take the last ~20% of text items as a proxy for the bottom of the page
        # (title blocks are typically at the bottom-right)
        n = max(1, len(page_texts) // 5)
        bottom_texts = page_texts[-n:]
        combined = " ".join(bottom_texts)
        return re.sub(r'\s+', ' ', combined).strip()
    except Exception as exc:
        logger.debug("Docling title-block extraction failed (page %d): %s",
                     page_idx, exc)
        return ""


def _get_docling_doc(pdf_path: str):
    """Get or create a cached Docling document for classification.

    Uses a size-capped LRU cache (max 2 entries) so memory is bounded.
    Thread-safe: only one conversion runs at a time per process.
    """
    # Fast path: already cached (check without lock first)
    with _docling_cache_lock:
        if pdf_path in _docling_doc_cache:
            _docling_doc_cache.move_to_end(pdf_path)
            return _docling_doc_cache[pdf_path]

        # Cache miss — convert inside the lock so only one thread converts
        try:
            pipeline_opts = PdfPipelineOptions(
                do_table_structure=True,
                do_ocr=False,  # OCR not needed for classification
            )
            converter = DocumentConverter(
                allowed_formats=[InputFormat.PDF],
                format_options={
                    InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts),
                },
            )
            # Run conversion in a separate thread with a timeout to prevent
            # hanging on complex engineering PDFs.
            # NOTE: We avoid `with ThreadPoolExecutor` because its __exit__
            # calls shutdown(wait=True), which blocks until the Docling
            # thread finishes — defeating the timeout entirely.
            pool = ThreadPoolExecutor(max_workers=1)
            future = pool.submit(converter.convert, pdf_path)
            try:
                conv_result = future.result(timeout=_DOCLING_TIMEOUT)
            except FuturesTimeoutError:
                logger.warning(
                    "Docling conversion timed out after %ds for %s",
                    _DOCLING_TIMEOUT, pdf_path,
                )
                future.cancel()
                pool.shutdown(wait=False)
                _docling_doc_cache[pdf_path] = None
                return None
            pool.shutdown(wait=False)
            doc = conv_result.document

            # Evict oldest entry when at capacity
            if len(_docling_doc_cache) >= _DOCLING_CACHE_MAXSIZE:
                evicted_key, _ = _docling_doc_cache.popitem(last=False)
                logger.debug("Docling cache evicted: %s", evicted_key)

            _docling_doc_cache[pdf_path] = doc
            return doc
        except Exception as exc:
            logger.warning("Docling conversion failed for %s: %s", pdf_path, exc)
            _docling_doc_cache[pdf_path] = None
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# Sheet Code Extraction
# ═══════════════════════════════════════════════════════════════════════════════

# Matches codes like E200, E2.01, E1-01, CS, …
_SHEET_CODE_RE = re.compile(
    r'\b(CS|[A-Z]{1,2}\d{1,4}(?:[.\-]\d{1,3})?[A-Z]?)\b',
    re.IGNORECASE,
)


def _extract_sheet_code(title_block_text: str) -> Optional[str]:
    """Extract the most likely sheet code from title-block text."""
    if not title_block_text:
        return None

    matches = _SHEET_CODE_RE.findall(title_block_text)
    if not matches:
        return None

    # Prefer codes starting with E (electrical) or CS
    for m in matches:
        upper = m.upper()
        if upper.startswith("E") or upper == "CS":
            return upper

    return matches[0].upper() if matches else None


# ═══════════════════════════════════════════════════════════════════════════════
# Sheet Index Building  (Priority 1)
# ═══════════════════════════════════════════════════════════════════════════════

_COVER_KEYWORDS = [
    "cover", "symbol", "abbreviation", "legend",
    "title sheet", "e000", "e0", "cs",
]

# Text-based line pattern:  SHEET_CODE    DESCRIPTION  (2+ spaces between)
_SHEET_INDEX_LINE_RE = re.compile(
    r'^\s*([A-Z]{1,3}[.\-]?\d{0,4}(?:[.\-]?\d{0,3})?[A-Z]?)\s{2,}(.+)$',
    re.IGNORECASE | re.MULTILINE,
)


def _find_sheet_index_candidate_pages(
    page_texts: Dict[int, str],
    total_pages: int,
) -> List[int]:
    """Return page numbers most likely to contain a sheet index."""
    candidates: List[int] = []

    # Page 1 is always checked first
    if 1 in page_texts:
        candidates.append(1)

    # Pages with cover/legend keywords
    for pn, text in sorted(page_texts.items()):
        if pn == 1:
            continue
        text_lower = text.lower()
        if any(kw in text_lower for kw in _COVER_KEYWORDS):
            candidates.append(pn)

    # Page 2 as fallback
    if 2 in page_texts and 2 not in candidates:
        candidates.append(2)

    return candidates


def _extract_sheet_index_from_tables(
    pdf_path: str, page_idx: int,
) -> Dict[str, str]:
    """
    Try to extract sheet index entries from tables detected by Docling.
    Returns {sheet_code_upper: description}.
    """
    try:
        doc = _get_docling_doc(pdf_path)
        if doc is None:
            return {}

        page_no = page_idx + 1  # Docling uses 1-based page numbers
        entries: Dict[str, str] = {}

        for table_item in doc.tables:
            tbl_page = table_item.prov[0].page_no if table_item.prov else 0
            if tbl_page != page_no:
                continue
            try:
                df = table_item.export_to_dataframe(doc)
                for _, row in df.iterrows():
                    vals = [str(v).strip() for v in row.values]
                    if len(vals) < 2:
                        continue
                    code = vals[0].upper()
                    desc = vals[1]
                    if code and desc and _SHEET_CODE_RE.match(code):
                        entries[code] = desc
            except Exception:
                continue

        return entries
    except Exception as exc:
        logger.debug("Docling table extraction failed for sheet index (page %d): %s",
                     page_idx, exc)
        return {}


def _extract_sheet_index_from_text(page_text: str) -> Dict[str, str]:
    """
    Fallback: pull sheet index entries from lines matching
    SHEET_CODE    DESCRIPTION  (at least two spaces).
    """
    entries: Dict[str, str] = {}
    for match in _SHEET_INDEX_LINE_RE.finditer(page_text):
        code = match.group(1).strip().upper()
        desc = match.group(2).strip()
        if code and desc:
            entries[code] = desc
    return entries


def _build_sheet_index(
    pdf_path: str,
    page_texts: Dict[int, str],
    total_pages: int,
) -> Dict[str, Tuple[str, Optional[PageType]]]:
    """
    Build a sheet index mapping:
        {sheet_code_upper: (description, inferred_type | None)}
    """
    candidate_pages = _find_sheet_index_candidate_pages(page_texts, total_pages)
    logger.debug("Sheet-index candidate pages: %s", candidate_pages)

    all_entries: Dict[str, str] = {}

    for pn in candidate_pages:
        page_idx = pn - 1

        # Try text-pattern extraction first (fast, no dependency)
        text_entries = _extract_sheet_index_from_text(page_texts.get(pn, ""))
        if text_entries:
            logger.info("Found %d sheet-index entries via text on page %d",
                        len(text_entries), pn)
            all_entries.update(text_entries)
            break

        # Fallback to Docling table extraction
        table_entries = _extract_sheet_index_from_tables(pdf_path, page_idx)
        if table_entries:
            logger.info("Found %d sheet-index entries via tables on page %d",
                        len(table_entries), pn)
            all_entries.update(table_entries)
            break

    # Infer types for every entry
    sheet_index: Dict[str, Tuple[str, Optional[PageType]]] = {}
    for code, desc in all_entries.items():
        inferred = _infer_type_from_description(desc)
        sheet_index[code] = (desc, inferred)
        logger.debug("  SheetIndex: %s → '%s' → %s", code, desc, inferred)

    return sheet_index


# ═══════════════════════════════════════════════════════════════════════════════
# Main Classification Logic  (four-priority chain)
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_page(
    page_num: int,
    page_text: str,
    title_block: str,
    sheet_code: str,
    sheet_index: Dict[str, Tuple[str, Optional[PageType]]],
    pdf_path: str,
) -> Tuple[PageType, str]:
    """Run the four-priority classification chain for a single page.

    Returns (PageType, confidence_source) where confidence_source is one of:
        sheet_index, title_block, sheet_code, full_text
    """

    # ── Priority 1: Sheet Index Lookup ────────────────────────────────────
    if sheet_code and sheet_code in sheet_index:
        desc, inferred_type = sheet_index[sheet_code]
        if inferred_type is not None:
            logger.debug("P%d → sheet-index: %s → %s",
                         page_num, sheet_code, inferred_type.value)
            return inferred_type, "sheet_index"

    # ── Priority 2: Title Block Analysis ──────────────────────────────────
    tb_type: Optional[PageType] = None

    if title_block:
        tb_type = _infer_type_from_title_block(title_block)

    # NOTE: Docling fallback is intentionally omitted here.
    # All title blocks (fitz + Docling) are pre-computed in classify_all_pages
    # Step 2b before this function is called, so title_block already contains
    # the best available text. Calling Docling per-page inside a thread pool
    # would trigger redundant 30s+ conversions.

    if tb_type is not None:
        logger.debug("P%d → title-block → %s", page_num, tb_type.value)
        return tb_type, "title_block"

    # ── Priority 3: Sheet Code Prefix ─────────────────────────────────────
    if sheet_code:
        # Use sheet index description (if any) for disambiguation
        idx_title = ""
        if sheet_code in sheet_index:
            idx_title = sheet_index[sheet_code][0]
        prefix_type = _classify_by_sheet_code(
            sheet_code, idx_title or title_block,
        )
        if prefix_type is not None:
            logger.debug("P%d → code-prefix %s → %s",
                         page_num, sheet_code, prefix_type.value)
            return prefix_type, "sheet_code"

    # ── Priority 4: Full-Page Content Scan ────────────────────────────────
    content_type = _classify_by_content(page_text)
    logger.debug("P%d → content-scan → %s", page_num, content_type.value)
    return content_type, "full_text"


# ═══════════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════════

def classify_all_pages(
    pdf_path: str,
    page_texts: Dict[int, str],
    total_pages: int,
) -> Dict[int, Dict[str, Any]]:
    """
    Classify every page of a PDF document.

    Args:
        pdf_path    : Absolute path to the PDF file on disk.
        page_texts  : {page_number (1-based): extracted_text}
        total_pages : Total number of pages in the PDF.

    Returns:
        {page_number: {"page_type": str, "is_relevant": bool,
                        "sheet_code": str, "confidence_source": str}}
    """
    logger.info("=== Page classification started (%d pages) ===", total_pages)

    # ── Filename-based hints ──────────────────────────────────────────────
    # Many construction PDFs use descriptive filenames like
    # "081---E2A UPPER LEVEL PLAN - LIGHTING AREA A.pdf"
    import os
    filename = os.path.splitext(os.path.basename(pdf_path))[0]
    filename_type = _infer_type_from_title_block(filename)
    filename_code = _extract_sheet_code(filename)
    if filename_type:
        logger.info("Filename hint: '%s' → %s (code=%s)",
                    filename, filename_type.value, filename_code or "-")

    # Step 1 — Build sheet index from cover / legend pages ─────────────────
    sheet_index = _build_sheet_index(pdf_path, page_texts, total_pages)
    logger.info("Sheet index: %d entries", len(sheet_index))

    # Step 2 — Extract title blocks + sheet codes (one fitz open) ──────────
    title_blocks: Dict[int, str] = {}
    sheet_codes: Dict[int, str] = {}

    try:
        pdf_doc = fitz.open(pdf_path)
        for pn in range(1, total_pages + 1):
            page = pdf_doc.load_page(pn - 1)
            tb_text = _extract_title_block_fitz(page)
            title_blocks[pn] = tb_text
            code = _extract_sheet_code(tb_text)
            if code:
                sheet_codes[pn] = code
        pdf_doc.close()
    except Exception as exc:
        logger.error("Title-block extraction failed: %s", exc)

    # Step 2b — Pre-warm Docling title blocks for pages fitz couldn't read.
    # Done ONCE here (single-threaded) so workers never call Docling themselves,
    # eliminating duplicate 30s+ conversions inside the thread pool.
    pages_needing_docling = [pn for pn in range(1, total_pages + 1)
                             if not title_blocks.get(pn)
                             or not _infer_type_from_title_block(title_blocks.get(pn, ""))]
    if pages_needing_docling:
        logger.info(
            "Pre-computing Docling title blocks for %d/%d pages...",
            len(pages_needing_docling), total_pages,
        )
        t0 = __import__('time').time()
        for pn in pages_needing_docling:
            docling_tb = _extract_title_block_docling(pdf_path, pn - 1)
            if docling_tb and not title_blocks.get(pn):
                title_blocks[pn] = docling_tb
                code = _extract_sheet_code(docling_tb)
                if code and pn not in sheet_codes:
                    sheet_codes[pn] = code
        logger.info(
            "Docling title-block pre-computation done in %.1fs",
            __import__('time').time() - t0,
        )

    # Step 3 — Classify each page through the priority chain ───────────────
    # Use a thread pool for parallel classification — each page goes through
    # the four-priority chain independently.
    # NOTE: Workers no longer call Docling (pre-warmed above), so the thread
    # pool is now purely CPU-bound text analysis — very fast.
    from config import CLASSIFICATION_WORKERS
    workers = min(CLASSIFICATION_WORKERS, total_pages)
    results: Dict[int, Dict[str, Any]] = {}

    def _classify_single_page_work(pn: int) -> Dict[str, Any]:
        """Classify one page — designed for parallel execution."""
        page_text = page_texts.get(pn, "")
        title_block = title_blocks.get(pn, "")
        sheet_code = sheet_codes.get(pn, "")

        # ── Filename-first for single-page PDFs (ZIP extracts) ────
        if total_pages == 1 and filename_type is not None:
            page_type = filename_type
            confidence_source = "filename"
            logger.info("P%d → filename (single-page PDF) → %s",
                        pn, page_type.value)
        else:
            page_type, confidence_source = _classify_page(
                page_num=pn,
                page_text=page_text,
                title_block=title_block,
                sheet_code=sheet_code,
                sheet_index=sheet_index,
                pdf_path=pdf_path,
            )

            # ── Filename fallback ────────────────────────────────────
            if page_type == PageType.OTHER and filename_type is not None:
                page_type = filename_type
                confidence_source = "filename"
                logger.info("P%d → filename fallback → %s", pn, page_type.value)

        # Inherit sheet code from filename when title block missed it
        if not sheet_code and filename_code:
            sheet_code = filename_code

        # Schedule verification — only luminaire schedules are relevant
        if page_type == PageType.SCHEDULE:
            is_relevant = _is_luminaire_schedule(page_text)
        elif page_type in _ALWAYS_RELEVANT:
            is_relevant = True
        else:
            is_relevant = False

        return {
            "pn": pn,
            "page_type": page_type.value,
            "is_relevant": is_relevant,
            "sheet_code": sheet_code or "",
            "confidence_source": confidence_source,
        }

    logger.info(
        "Classifying %d pages with %d parallel workers",
        total_pages, workers,
    )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_classify_single_page_work, pn): pn
            for pn in range(1, total_pages + 1)
        }
        for future in as_completed(futures):
            pn = futures[future]
            try:
                result = future.result()
                # Update shared sheet_codes if filename code was inherited
                if result["sheet_code"] and pn not in sheet_codes:
                    sheet_codes[pn] = result["sheet_code"]
                results[pn] = {
                    "page_type": result["page_type"],
                    "is_relevant": result["is_relevant"],
                    "sheet_code": result["sheet_code"],
                    "confidence_source": result["confidence_source"],
                }
                logger.info(
                    "  Page %d: type=%-18s relevant=%-5s code=%s",
                    pn, result["page_type"], str(result["is_relevant"]),
                    result["sheet_code"] or "-",
                )
            except Exception as exc:
                logger.error("Classification failed for page %d: %s", pn, exc)
                results[pn] = {
                    "page_type": PageType.OTHER.value,
                    "is_relevant": False,
                    "sheet_code": sheet_codes.get(pn, ""),
                    "confidence_source": "error",
                }

    logger.info("=== Page classification complete ===")

    # Free Docling Document objects from the cache now that classification
    # is finished — they are large and no longer needed.
    _clear_docling_cache()

    return results

