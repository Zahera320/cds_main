"""
Key Notes Extractor
====================
Extracts KEY NOTES / KEYED NOTES from engineering drawing page text.

Handles four common PDF-extraction formats:
  1. Inline:        "1. Note text continues..."
  2. Numbers-first: numbers listed then text blocks in order below
  3. Text-first:    text paragraphs then drawing callout numbers — auto-numbered
  4. Plain paragraphs: no numbers at all — auto-numbered sequentially

Only KEY NOTES / KEYED NOTES sections are extracted.

Public API:
    extract_keynotes(page_text: str) → list[dict]
        Returns list of {"number": str, "text": str, "section": str}
"""

import re
import logging
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Section header: KEY NOTES or KEYED NOTES (requires space before NOTE)
_KEY_NOTES_HDR = re.compile(r'(?i)^\s*KEY(?:ED)?\s+NOTES?\s*:?\s*$')

# Lines that clearly end the KEY NOTES section
_SECTION_END = re.compile(
    r'(?i)^\s*'
    r'(?:(?:GENERAL|ELECTRICAL|LIGHTING|MECHANICAL|PLUMBING|STRUCTURAL|'
    r'ARCHITECTURAL|ACCESS\s+CONTROL|FIRE)\s+)?'
    r'(?:GENERAL|NOTES?|ABBREVIATIONS|SYMBOLS?|LEGEND|SCHEDULE|TITLE\s+SHEET)'
    r'\s*:?\s*$'
)

# Inline numbered: "1. text" or "1) text"
_INLINE_NUM_RE = re.compile(r'^\s*(\d{1,2})[.\)]\s+(.+)')

# Standalone number on its own line
_STANDALONE_NUM_RE = re.compile(r'^\s*(\d{1,2})\s*$')

# Lettered items (signal GENERAL NOTES, not KEY NOTES)
_INLINE_LETTER_RE = re.compile(r'^\s*([A-Z])[.\)]\s+(.+)')
_STANDALONE_LETTER_RE = re.compile(r'^\s*([A-Z])\.\s*$')

# Drawing callout symbols — not note text (S5, E1, EW, PL.656, CV1/20, etc.)
_CALLOUT_LINE = re.compile(
    r'^\s*(?:'
    r'[A-Z]{1,3}\d{1,3}(?:[a-z])?(?:/\d+)?|'       # S5, E12, CV1/20, D1S
    r'PL\.?\d+[a-z]?|'                               # PL.656, PL656
    r'[A-Z]{1,3}\s*$|'                                # EW, LC, UP, VS, P, J, N
    r'\d{1,2}\s+[A-Z]\d{1,2}\s*$'                    # "4  E3", "11  B6" (fixture refs)
    r')\s*$'
)

# Room label lines (BREAK ROOM, LABORATORY, etc.) — not note text
_ROOM_LABEL = re.compile(
    r'(?i)^\s*(?:'
    r'(?:BREAK\s+)?ROOM|LABORATORY|OFFICE|TOILET|STORAGE|CIRCULATION|'
    r'CONFERENCE|WORKSTATION|RECEPTION|EXAM|KITCHEN|LOBBY|CORRIDOR|'
    r'VESTIBULE|STAIR|ELEVATOR|MECHANICAL|ELECTRICAL|JANITOR|CLOSET|'
    r'CUBICAL|MOTHERS|FUME\s+HOODS|EMBEDDING|CHECK.?IN|CHECK.?OUT|'
    r'MICROTOMES|H&E\s+STAINER|KIDNEY|TISSUE\s+PROC|SLIDE\s+SCANNER|'
    r'WORK\s+ROOM|SCOPE\s+ROOM|WATER\s+BOTTLING'
    r')\s*$'
)

# Lines that are clearly non-note junk (dates, filenames, coordinates)
_JUNK_LINE = re.compile(
    r'(?i)^\s*(?:\d{1,2}/\d{1,2}/\d{4}|'           # date MM/DD/YYYY
    r'\d{4}-\d{2}-\d{2}|'                             # ISO date YYYY-MM-DD
    r'\d{1,2}-\d{1,2}-\d{4}|'                         # date MM-DD-YYYY
    r'\d{4,6}\s*$|'                                    # registration/license numbers
    r'[A-Z]:\\|Autodesk|Copyright|COMMISSION NO|'     # file/copyright
    r'I hereby certify|Licensed Professional|'         # engineer stamp
    r'SCALE:|DRAWN BY:|CHECKED BY:|LICENSE|'
    r'SIGNATURE|PRINT NAME|PRINTED NAME|DATE ISSUED|REG\.?\s*NO|'
    r'ISSUE\s*/?\s*REVISION|ISSUE\s+FOR\s+BID|'
    r'ISSUE\s+FOR\s+PERMIT|ISSUE\s+FOR\s+CONSTRUCTION|'
    r'CONSULTANT\s*$|PROJECT\s+TITLE|SHEET\s+TITLE|'
    r'PROJECT\s+NO\.?|DATE\s*$)'
)

# Title-block / stamp patterns — terminates KEY NOTES collection
_TITLE_BLOCK = re.compile(
    r'(?i)^\s*(?:'
    r'I\s+HEREBY\s+CERTIFY|'
    r'LICENSED\s+PROFESSIONAL|'
    r'DATE\s+ISSUED|'
    r'REG\.?\s*NO\.?|'
    r'ISSUE\s*/?\s*REVISION|'
    r'ISSUE\s+FOR\s+BID|'
    r'ISSUE\s+FOR\s+PERMIT|'
    r'ISSUE\s+FOR\s+CONSTRUCTION|'
    r'PRINTED\s+NAME|'
    r'PRINT\s+NAME|'
    r'SIGNATURE\s*$|'
    r'SEAL\s*$|'
    r'CONSULTANT\s*$|'
    r'PROJECT\s+TITLE|'
    r'SHEET\s+TITLE|'
    r'PROJECT\s+NO\.?|'
    r'DRAWING\s+\d{4}\s+COPYRIGHT|'
    r'COPYRIGHT\s+\d{4}|'
    r'COPYRIGHT\s+MEYER|'
    r'ARCHITECTURE\s+AND\s+INTERIORS|'
    r'[A-Za-z]+\s+[A-Za-z]+\s+(?:ARCHITECTURE|ENGINEERING|ASSOCIATES)|'  # Firm names
    r'\d+\s+\w+\s+(?:Avenue|Street|Road|Drive|Blvd|Boulevard|Way|Lane|Circle|Court)|'  # addresses
    r'[A-Za-z]+,?\s*[A-Z]{2}\s+\d{5}|'                # City, ST ZIP
    r'sheadesign\.com|'
    r'MohagenHansen\.com'
    r')\s*'
)


def _is_skip_line(s: str) -> bool:
    """Return True if line is a callout, room label, junk, or title block."""
    return bool(
        _CALLOUT_LINE.match(s) or _ROOM_LABEL.match(s) or
        _JUNK_LINE.match(s) or _TITLE_BLOCK.match(s)
    )


def _is_real_note(text: str) -> bool:
    """Check if text looks like a real construction note vs. drawing artifacts."""
    if len(text) < 25:
        return False
    words = text.split()
    # Count words that are real English words (all alpha, > 3 chars)
    real_words = sum(1 for w in words if w.replace('.', '').replace(',', '').isalpha() and len(w) > 3)
    return real_words >= 2


def extract_keynotes(page_text: str) -> List[Dict[str, str]]:
    """
    Extract KEY NOTES / KEYED NOTES from a single page's extracted text.

    Returns:
        [{"number": "1", "text": "...", "section": "KEY NOTES"}, ...]
    """
    if not page_text:
        return []

    lines = page_text.split('\n')
    notes: List[Dict[str, str]] = []

    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if _KEY_NOTES_HDR.match(stripped):
            section = re.sub(r'\s+', ' ', stripped).upper().rstrip(':')
            i += 1
            i = _parse_section(lines, i, section, notes)
        else:
            i += 1

    # Post-filter: for auto-numbered notes (from paragraph parser), apply quality check
    filtered = [n for n in notes if n.get('_explicit_num') or _is_real_note(n['text'])]
    # Remove internal marker before returning
    for n in filtered:
        n.pop('_explicit_num', None)
    return filtered


def _parse_section(
    lines: List[str], start: int, section: str, notes: List[Dict[str, str]]
) -> int:
    """
    Parse one KEY NOTES section starting at `start`.
    Auto-detects the format and delegates. Returns next index after section.
    """
    # Collect a lookahead window (up to 60 lines) skipping empty and label lines
    window: List[Tuple[int, str]] = []  # (original_index, stripped_line)
    j = start
    skipped_labels = 0
    while j < len(lines) and len(window) < 60:
        s = lines[j].strip()
        j += 1
        if not s:
            continue
        # Skip one "GENERAL NOTES" style label that often immediately follows KEY NOTES
        if _SECTION_END.match(s) and skipped_labels == 0 and not window:
            skipped_labels += 1
            continue
        # Real section end or title block
        if _SECTION_END.match(s) or _TITLE_BLOCK.match(s):
            break
        if _KEY_NOTES_HDR.match(s):
            break
        # Skip callout symbols and room labels in lookahead
        if _CALLOUT_LINE.match(s) or _ROOM_LABEL.match(s):
            continue
        window.append((j - 1, s))

    if not window:
        return j

    # ── Detect format ──────────────────────────────────────────────────
    first_line = window[0][1]

    if _INLINE_NUM_RE.match(first_line) or _INLINE_LETTER_RE.match(first_line):
        # Format 1: inline numbered/lettered
        end = _parse_inline(lines, start, section, notes, skipped_labels)
        return end

    # Count leading standalone numbers
    leading_nums = []
    wi = 0
    while wi < len(window) and _STANDALONE_NUM_RE.match(window[wi][1]):
        leading_nums.append(window[wi][1])
        wi += 1

    if leading_nums and wi < len(window):
        next_after_nums = window[wi][1]
        # Validate: numbers should be unique (drawing content repeats numbers)
        unique_nums = set(leading_nums)
        if not _STANDALONE_NUM_RE.match(next_after_nums) and len(unique_nums) > len(leading_nums) * 0.4:
            # Format 2: numbers-first column
            end = _parse_column(lines, start, section, notes, skipped_labels)
            return end

    # Format 3 & 4: plain paragraphs (text-first or no numbers) — auto-number
    end = _parse_paragraphs(lines, start, section, notes, skipped_labels)
    return end


# ── Format 1: inline "1. text..." ─────────────────────────────────────
def _parse_inline(
    lines: List[str], start: int, section: str,
    notes: List[Dict[str, str]], skip_labels: int,
) -> int:
    i = start
    skipped = 0
    current_number: Optional[str] = None
    current_text: List[str] = []

    def _flush():
        nonlocal current_number, current_text
        if current_number and current_text:
            text = re.sub(r'\s+', ' ', ' '.join(current_text)).strip()
            if text and not _JUNK_LINE.match(text):
                notes.append({"number": current_number, "text": text, "section": section, "_explicit_num": True})
        current_number = None
        current_text = []

    while i < len(lines):
        s = lines[i].strip()
        i += 1
        if not s:
            continue
        # Skip one section-end label immediately after header
        if _SECTION_END.match(s) and skipped < skip_labels:
            skipped += 1
            continue
        if _SECTION_END.match(s) or _KEY_NOTES_HDR.match(s) or _TITLE_BLOCK.match(s):
            break

        m_num = _INLINE_NUM_RE.match(s)
        m_let = _INLINE_LETTER_RE.match(s)
        if m_num:
            _flush()
            current_number = m_num.group(1)
            current_text.append(m_num.group(2))
        elif m_let:
            # Lettered items are GENERAL NOTES — stop
            _flush()
            break
        elif current_number:
            if not _is_skip_line(s):
                current_text.append(s)

    _flush()
    return i - 1


# ── Format 2: numbers-first column ────────────────────────────────────
def _parse_column(
    lines: List[str], start: int, section: str,
    notes: List[Dict[str, str]], skip_labels: int,
) -> int:
    i = start
    skipped = 0
    numbers: List[str] = []
    text_blocks: List[str] = []
    current_text: List[str] = []
    collecting_numbers = True

    def _flush_text():
        if current_text:
            text = re.sub(r'\s+', ' ', ' '.join(current_text)).strip()
            if text and not _JUNK_LINE.match(text):
                text_blocks.append(text)
        current_text.clear()

    while i < len(lines):
        s = lines[i].strip()
        i += 1
        if not s:
            if not collecting_numbers:
                _flush_text()
            continue
        if _SECTION_END.match(s) and skipped < skip_labels:
            skipped += 1
            continue
        if _SECTION_END.match(s) or _KEY_NOTES_HDR.match(s) or _TITLE_BLOCK.match(s):
            _flush_text()
            break

        if collecting_numbers:
            if _STANDALONE_LETTER_RE.match(s) or _INLINE_LETTER_RE.match(s):
                break  # GENERAL NOTES start
            if _is_skip_line(s):
                continue
            # Single uppercase letter without dot = grid reference
            if re.match(r'^\s*[A-Z]\s*$', s):
                continue
            if _STANDALONE_NUM_RE.match(s):
                numbers.append(_STANDALONE_NUM_RE.match(s).group(1))
                continue
            # Skip all-caps labels before numbers start
            if s.isupper() and len(s) < 40 and not re.search(r'\d', s) and not numbers:
                continue
            collecting_numbers = False
            current_text.append(s)
            # First text line may also be sentence-end
            if numbers and s.endswith('.'):
                _flush_text()
        else:
            if _INLINE_LETTER_RE.match(s) or _STANDALONE_LETTER_RE.match(s):
                _flush_text()
                break
            if _is_skip_line(s):
                continue
            # Single uppercase letter without dot = grid reference
            if re.match(r'^\s*[A-Z]\s*$', s):
                _flush_text()
                break
            if _STANDALONE_NUM_RE.match(s):
                _flush_text()
                numbers.append(_STANDALONE_NUM_RE.match(s).group(1))
                collecting_numbers = True
                continue
            current_text.append(s)
            # Sentence-end → flush paragraph
            if numbers and s.endswith('.'):
                _flush_text()

    _flush_text()

    for idx, num in enumerate(numbers):
        if idx < len(text_blocks):
            notes.append({"number": num, "text": text_blocks[idx], "section": section})

    return i - 1


# ── Format 3/4: plain paragraphs — auto-number ────────────────────────
def _parse_paragraphs(
    lines: List[str], start: int, section: str,
    notes: List[Dict[str, str]], skip_labels: int,
) -> int:
    """
    Collect text paragraphs and auto-number them 1, 2, 3...
    A paragraph ends on a blank line OR when a sentence-ending line (ends '.') is
    followed by a line that starts a new sentence.
    Stop when numbers start appearing (drawing callout markers) or section ends.
    """
    i = start
    skipped = 0
    counter = 1
    current_text: List[str] = []

    def _flush():
        nonlocal counter, current_text
        if current_text:
            text = re.sub(r'\s+', ' ', ' '.join(current_text)).strip()
            if text and len(text) > 15 and not _JUNK_LINE.match(text):
                notes.append({"number": str(counter), "text": text, "section": section})
                counter += 1
        current_text = []

    while i < len(lines):
        s = lines[i].strip()
        i += 1
        if not s:
            _flush()
            continue
        if _SECTION_END.match(s) and skipped < skip_labels:
            skipped += 1
            continue
        if _SECTION_END.match(s) or _KEY_NOTES_HDR.match(s) or _TITLE_BLOCK.match(s):
            break
        # When standalone numbers or lettered notes start, stop
        if _STANDALONE_NUM_RE.match(s) or _STANDALONE_LETTER_RE.match(s):
            _flush()
            break
        if _INLINE_NUM_RE.match(s) or _INLINE_LETTER_RE.match(s):
            _flush()
            # Hand off to inline parser from this point
            # Rebuild context: put this line back
            i -= 1
            end = _parse_inline(lines, i, section, notes, 0)
            return end
        # Skip junk, callouts, and room labels
        if _JUNK_LINE.match(s) or _CALLOUT_LINE.match(s) or _ROOM_LABEL.match(s):
            continue
        current_text.append(s)
        # Sentence boundary → flush paragraph
        if s.endswith('.'):
            _flush()

    _flush()
    return i - 1


def extract_keynotes_from_pages(pages: list) -> Dict[int, List[Dict[str, str]]]:
    """
    Extract key notes from multiple DocumentPage objects.
    Returns {page_number: [note_dict, ...]} — only pages with notes included.
    """
    result = {}
    for page in pages:
        text = getattr(page, 'extracted_text', '') or ''
        notes = extract_keynotes(text)
        if notes:
            result[page.page_number] = notes
    return result
